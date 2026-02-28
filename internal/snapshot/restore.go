package snapshot

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/runs-on/snapshot/internal/utils"
)

func (s *AWSSnapshotter) subDirForPath(path string) string {
	return filepath.Join(s.baseMountPoint(), sanitizePath(path))
}

// setupBindMounts creates subdirectories on the EBS volume and bind-mounts them to each target path.
func (s *AWSSnapshotter) setupBindMounts(ctx context.Context) error {
	for _, path := range s.config.Paths {
		isDocker := strings.HasPrefix(path, "/var/lib/docker")
		subDir := s.subDirForPath(path)

		if isDocker {
			if _, err := s.runCommand(ctx, "sudo", "systemctl", "stop", "docker"); err != nil {
				s.logger.Warn().Msgf("failed to stop docker: %v", err)
			}
		}

		s.runCommand(ctx, "sudo", "umount", path)

		if _, err := s.runCommand(ctx, "sudo", "mkdir", "-p", subDir); err != nil {
			return fmt.Errorf("failed to create subdirectory %s: %w", subDir, err)
		}
		if _, err := s.runCommand(ctx, "sudo", "mkdir", "-p", path); err != nil {
			return fmt.Errorf("failed to create mount point %s: %w", path, err)
		}
		if _, err := s.runCommand(ctx, "sudo", "mount", "--bind", subDir, path); err != nil {
			return fmt.Errorf("failed to bind mount %s → %s: %w", subDir, path, err)
		}
		s.logger.Info().Msgf("Bind mounted %s → %s", subDir, path)

		if !isDocker {
			if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
				needsChown := true
				if u, err := user.Lookup(sudoUser); err == nil {
					if uid, err := strconv.Atoi(u.Uid); err == nil {
						if info, err := os.Stat(subDir); err == nil {
							if stat, ok := info.Sys().(*syscall.Stat_t); ok && int(stat.Uid) == uid {
								needsChown = false
							}
						}
					}
				}
				if needsChown {
					s.runCommand(ctx, "sudo", "chown", sudoUser+":"+sudoUser, subDir)
				} else {
					s.logger.Info().Msgf("Ownership of %s already correct, skipping chown", subDir)
				}
			}
		}

		if isDocker {
			if _, err := s.runCommand(ctx, "sudo", "systemctl", "start", "docker"); err != nil {
				return fmt.Errorf("failed to start docker: %w", err)
			}
		}
	}
	return nil
}

// tryWarmRestore attempts to reuse a volume already attached from a previous job on the same instance.
func (s *AWSSnapshotter) tryWarmRestore(ctx context.Context) (*RestoreSnapshotOutput, error) {
	volumeInfo, err := s.loadVolumeInfo()
	if err != nil {
		return nil, fmt.Errorf("no volume info: %w", err)
	}

	s.logger.Info().Msgf("RestoreSnapshot: Found existing volume info (volume %s), checking if still attached...", volumeInfo.VolumeID)

	descOutput, err := s.ec2Client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []string{volumeInfo.VolumeID},
		Filters: []types.Filter{
			{Name: aws.String("attachment.instance-id"), Values: []string{s.config.InstanceID}},
			{Name: aws.String("attachment.status"), Values: []string{"attached"}},
		},
	})
	if err != nil || len(descOutput.Volumes) == 0 {
		s.logger.Info().Msgf("RestoreSnapshot: Volume %s is no longer attached", volumeInfo.VolumeID)
		return nil, fmt.Errorf("volume not attached")
	}

	s.logger.Info().Msgf("RestoreSnapshot: Warm volume %s detected, reusing (skipping create/attach)...", volumeInfo.VolumeID)

	bmp := s.baseMountPoint()
	if _, err := s.runCommand(ctx, "mountpoint", "-q", bmp); err != nil {
		if _, err := s.runCommand(ctx, "sudo", "mkdir", "-p", bmp); err != nil {
			return nil, fmt.Errorf("failed to create base mount point: %w", err)
		}
		if _, err := s.runCommand(ctx, "sudo", "mount", volumeInfo.DeviceName, bmp); err != nil {
			s.logger.Warn().Msgf("Warm base mount failed (%v), detaching stale volume %s", err, volumeInfo.VolumeID)
			s.ec2Client.DetachVolume(ctx, &ec2.DetachVolumeInput{
				VolumeId:   aws.String(volumeInfo.VolumeID),
				InstanceId: aws.String(s.config.InstanceID),
			})
			return nil, fmt.Errorf("warm mount failed: %w", err)
		}
	}

	if err := s.setupBindMounts(ctx); err != nil {
		return nil, fmt.Errorf("warm bind mount setup failed: %w", err)
	}

	s.ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{volumeInfo.VolumeID},
		Tags: []types.Tag{
			{Key: aws.String(ttlTagKey), Value: aws.String(fmt.Sprintf("%d", time.Now().Add(time.Duration(defaultVolumeLifeDurationMinutes)*time.Minute).Unix()))},
		},
	})

	volumeInfo.NewVolume = false
	s.saveVolumeInfo(volumeInfo)

	return &RestoreSnapshotOutput{
		VolumeID:   volumeInfo.VolumeID,
		DeviceName: volumeInfo.DeviceName,
		NewVolume:  false,
	}, nil
}

// RestoreSnapshot creates (or reuses) a single EBS volume, mounts it, and sets up bind mounts for all configured paths.
func (s *AWSSnapshotter) RestoreSnapshot(ctx context.Context) (*RestoreSnapshotOutput, error) {
	if output, err := s.tryWarmRestore(ctx); err == nil {
		return output, nil
	}

	gitBranch := s.config.GithubRef
	s.logger.Info().Msgf("RestoreSnapshot: Cold restore, using git ref: %s", gitBranch)

	var err error
	var newVolume *types.Volume
	var volumeIsNewAndUnformatted bool

	latestSnapshot := s.findSnapshot(ctx, gitBranch)

	// Cross-suffix fallback: any snapshot from the same repo is better than a blank volume
	if latestSnapshot == nil && s.config.Suffix != "" {
		s.logger.Info().Msgf("RestoreSnapshot: No snapshot found for suffix %q, trying any suffix as fallback", s.config.Suffix)
		latestSnapshot = s.findSnapshotAnySuffix(ctx, gitBranch)
	}

	if latestSnapshot == nil {
		s.logger.Info().Msgf("RestoreSnapshot: No existing snapshot found. A new volume will be created.")
	}

	commonVolumeTags := append(s.defaultTags(), []types.Tag{
		{Key: aws.String(nameTagKey), Value: aws.String(s.config.VolumeName)},
		{Key: aws.String(ttlTagKey), Value: aws.String(fmt.Sprintf("%d", time.Now().Add(time.Duration(defaultVolumeLifeDurationMinutes)*time.Minute).Unix()))},
	}...)

	if latestSnapshot != nil && latestSnapshot.VolumeSize != nil && *latestSnapshot.VolumeSize >= s.config.VolumeSize {
		s.logger.Info().Msgf("RestoreSnapshot: Creating volume from snapshot %s", *latestSnapshot.SnapshotId)
		createVolumeInput := &ec2.CreateVolumeInput{
			SnapshotId:       latestSnapshot.SnapshotId,
			AvailabilityZone: aws.String(s.config.Az),
			VolumeType:       s.config.VolumeType,
			Iops:             aws.Int32(s.config.VolumeIops),
			TagSpecifications: []types.TagSpecification{
				{ResourceType: types.ResourceTypeVolume, Tags: commonVolumeTags},
			},
		}
		if s.config.VolumeType == types.VolumeTypeGp3 {
			createVolumeInput.Throughput = aws.Int32(s.config.VolumeThroughput)
		}
		if s.config.VolumeInitializationRate > 0 {
			createVolumeInput.VolumeInitializationRate = aws.Int32(s.config.VolumeInitializationRate)
		}
		createVolumeOutput, err := s.ec2Client.CreateVolume(ctx, createVolumeInput)
		if err != nil {
			return nil, fmt.Errorf("failed to create volume from snapshot %s: %w", *latestSnapshot.SnapshotId, err)
		}
		newVolume = &types.Volume{VolumeId: createVolumeOutput.VolumeId}
		volumeIsNewAndUnformatted = false
		s.logger.Info().Msgf("RestoreSnapshot: Created volume %s from snapshot %s", *newVolume.VolumeId, *latestSnapshot.SnapshotId)
	} else {
		s.logger.Info().Msgf("RestoreSnapshot: Creating a new blank volume")
		createVolumeInput := &ec2.CreateVolumeInput{
			AvailabilityZone: aws.String(s.config.Az),
			VolumeType:       s.config.VolumeType,
			Size:             aws.Int32(s.config.VolumeSize),
			Iops:             aws.Int32(s.config.VolumeIops),
			TagSpecifications: []types.TagSpecification{
				{ResourceType: types.ResourceTypeVolume, Tags: commonVolumeTags},
			},
		}
		if s.config.VolumeType == types.VolumeTypeGp3 {
			createVolumeInput.Throughput = aws.Int32(s.config.VolumeThroughput)
		}
		createVolumeOutput, err := s.ec2Client.CreateVolume(ctx, createVolumeInput)
		if err != nil {
			return nil, fmt.Errorf("failed to create new volume: %w", err)
		}
		newVolume = &types.Volume{VolumeId: createVolumeOutput.VolumeId}
		volumeIsNewAndUnformatted = true
		s.logger.Info().Msgf("RestoreSnapshot: Created new blank volume %s", *newVolume.VolumeId)
	}

	defer func() {
		if err != nil && newVolume != nil {
			s.logger.Info().Msgf("RestoreSnapshot: Cleaning up volume %s due to error", *newVolume.VolumeId)
			s.ec2Client.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: newVolume.VolumeId})
		}
	}()

	s.logger.Info().Msgf("RestoreSnapshot: Waiting for volume %s to become available...", *newVolume.VolumeId)
	volumeAvailableWaiter := ec2.NewVolumeAvailableWaiter(s.ec2Client, defaultVolumeAvailableWaiterOptions)
	err = volumeAvailableWaiter.Wait(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{*newVolume.VolumeId}}, defaultVolumeAvailableMaxWaitTime)
	if err != nil {
		return nil, fmt.Errorf("volume %s did not become available in time: %w", *newVolume.VolumeId, err)
	}

	s.logger.Info().Msgf("RestoreSnapshot: Attaching volume %s to instance %s as %s", *newVolume.VolumeId, s.config.InstanceID, suggestedDeviceName)
	_, err = s.ec2Client.AttachVolume(ctx, &ec2.AttachVolumeInput{
		Device:     aws.String(suggestedDeviceName),
		InstanceId: aws.String(s.config.InstanceID),
		VolumeId:   newVolume.VolumeId,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to attach volume %s: %w", *newVolume.VolumeId, err)
	}

	volumeInUseWaiter := ec2.NewVolumeInUseWaiter(s.ec2Client, defaultVolumeInUseWaiterOptions)
	err = volumeInUseWaiter.Wait(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []string{*newVolume.VolumeId},
		Filters: []types.Filter{
			{Name: aws.String("attachment.status"), Values: []string{"attached"}},
		},
	}, defaultVolumeInUseMaxWaitTime)
	if err != nil {
		return nil, fmt.Errorf("volume %s did not attach in time: %w", *newVolume.VolumeId, err)
	}

	// Match NVMe device by volume serial number
	expectedSerial := strings.Replace(*newVolume.VolumeId, "-", "", 1)
	s.logger.Info().Msgf("RestoreSnapshot: Looking for NVMe device with serial %s...", expectedSerial)
	var actualDeviceName string
	var found bool
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < 10; attempt++ {
		lsblkOutput, lsblkErr := s.runCommand(ctx, "lsblk", "-d", "-n", "-o", "PATH,SERIAL")
		if lsblkErr != nil {
			time.Sleep(backoff)
			backoff = min(backoff*2, 2*time.Second)
			continue
		}
		for _, line := range strings.Split(strings.TrimSpace(string(lsblkOutput)), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[1] == expectedSerial {
				actualDeviceName = fields[0]
				found = true
				break
			}
		}
		if found {
			break
		}
		s.logger.Info().Msgf("Device not yet visible (attempt %d/10), retrying in %s...", attempt+1, backoff)
		time.Sleep(backoff)
		backoff = min(backoff*2, 2*time.Second)
	}
	if !found {
		err = fmt.Errorf("could not find NVMe device for volume %s (serial %s)", *newVolume.VolumeId, expectedSerial)
		return nil, err
	}
	s.logger.Info().Msgf("RestoreSnapshot: Matched volume %s to device %s", *newVolume.VolumeId, actualDeviceName)

	if volumeIsNewAndUnformatted {
		s.logger.Info().Msgf("RestoreSnapshot: Formatting new volume with ext4...")
		if _, fmtErr := s.runCommand(ctx, "sudo", "mkfs.ext4", "-F", "-E", "lazy_itable_init=1,lazy_journal_init=1", actualDeviceName); fmtErr != nil {
			err = fmt.Errorf("failed to format device %s: %w", actualDeviceName, fmtErr)
			return nil, err
		}
	}

	bmp := s.baseMountPoint()
	if _, err = s.runCommand(ctx, "sudo", "mkdir", "-p", bmp); err != nil {
		return nil, fmt.Errorf("failed to create base mount point: %w", err)
	}
	if _, err = s.runCommand(ctx, "sudo", "mount", actualDeviceName, bmp); err != nil {
		return nil, fmt.Errorf("failed to mount %s at %s: %w", actualDeviceName, bmp, err)
	}
	s.logger.Info().Msgf("RestoreSnapshot: Volume mounted at %s", bmp)

	volumeInfo := &VolumeInfo{
		VolumeID:   *newVolume.VolumeId,
		DeviceName: actualDeviceName,
		MountPoint: bmp,
		NewVolume:  volumeIsNewAndUnformatted,
	}
	if saveErr := s.saveVolumeInfo(volumeInfo); saveErr != nil {
		s.logger.Warn().Msgf("Failed to save volume info: %v", saveErr)
	}

	// Bind mount errors don't trigger volume cleanup — the volume itself is fine
	if bindErr := s.setupBindMounts(ctx); bindErr != nil {
		return nil, fmt.Errorf("bind mount setup failed: %w", bindErr)
	}

	return &RestoreSnapshotOutput{
		VolumeID:   *newVolume.VolumeId,
		DeviceName: actualDeviceName,
		NewVolume:  volumeIsNewAndUnformatted,
	}, nil
}

// buildFilters creates the default AWS snapshot search filters.
// If a suffix is set, the path filter includes both the suffix-aware and legacy _all_ values.
func (s *AWSSnapshotter) buildFilters() []types.Filter {
	filters := []types.Filter{
		{Name: aws.String("status"), Values: []string{string(types.SnapshotStateCompleted)}},
	}
	for _, tag := range s.defaultTags() {
		filters = append(filters, types.Filter{Name: aws.String(fmt.Sprintf("tag:%s", *tag.Key)), Values: []string{*tag.Value}})
	}
	if s.config.Suffix != "" {
		pathFilterName := fmt.Sprintf("tag:%s", snapshotTagKeyPath)
		for i, f := range filters {
			if *f.Name == pathFilterName {
				filters[i].Values = append(filters[i].Values, "_all_")
				break
			}
		}
	}
	return filters
}

// findLatestSnapshot returns the newest snapshot from a DescribeSnapshots response, or nil.
func findLatestSnapshot(snapshots []types.Snapshot) *types.Snapshot {
	if len(snapshots) == 0 {
		return nil
	}
	latest := &snapshots[0]
	for i := range snapshots {
		if snapshots[i].StartTime.After(*latest.StartTime) {
			latest = &snapshots[i]
		}
	}
	return latest
}

// searchSnapshots runs DescribeSnapshots with the given filters and returns the latest match.
func (s *AWSSnapshotter) searchSnapshots(ctx context.Context, filters []types.Filter) *types.Snapshot {
	output, err := s.ec2Client.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{
		Filters:  filters,
		OwnerIds: []string{"self"},
	})
	if err != nil {
		s.logger.Warn().Msgf("DescribeSnapshots failed: %v", err)
		return nil
	}
	return findLatestSnapshot(output.Snapshots)
}

// findSnapshot searches for a snapshot matching the exact suffix, trying the given branch then the default branch.
func (s *AWSSnapshotter) findSnapshot(ctx context.Context, branch string) *types.Snapshot {
	filters := s.buildFilters()

	s.logger.Info().Msgf("RestoreSnapshot: Searching for snapshot (branch=%s, suffix=%s)", branch, s.config.Suffix)
	if snap := s.searchSnapshots(ctx, filters); snap != nil {
		s.logger.Info().Msgf("RestoreSnapshot: Found snapshot %s for branch %s", *snap.SnapshotId, branch)
		return snap
	}

	if s.config.RunnerConfig.DefaultBranch != "" {
		s.logger.Info().Msgf("RestoreSnapshot: No snapshot found for branch %s, trying default branch %s", branch, s.config.RunnerConfig.DefaultBranch)
		if err := replaceFilterValues(filters, "tag:"+snapshotTagKeyBranch, []string{s.getSnapshotTagValueDefaultBranch()}); err != nil {
			s.logger.Warn().Msgf("Failed to update branch filter: %v", err)
			return nil
		}
		if snap := s.searchSnapshots(ctx, filters); snap != nil {
			s.logger.Info().Msgf("RestoreSnapshot: Found snapshot %s from default branch %s", *snap.SnapshotId, s.config.RunnerConfig.DefaultBranch)
			return snap
		}
	}

	return nil
}

// findSnapshotAnySuffix searches for any snapshot from the same repo, ignoring suffix/path tags.
// Used as a last-resort fallback when no suffix-specific snapshot exists.
func (s *AWSSnapshotter) findSnapshotAnySuffix(ctx context.Context, branch string) *types.Snapshot {
	filters := []types.Filter{
		{Name: aws.String("status"), Values: []string{string(types.SnapshotStateCompleted)}},
	}
	for _, tag := range s.defaultTags() {
		key := *tag.Key
		if key == snapshotTagKeyPath || key == snapshotTagKeySuffix {
			continue
		}
		filters = append(filters, types.Filter{Name: aws.String(fmt.Sprintf("tag:%s", key)), Values: []string{*tag.Value}})
	}

	s.logger.Info().Msgf("RestoreSnapshot: Cross-suffix fallback search (branch=%s)", branch)
	if snap := s.searchSnapshots(ctx, filters); snap != nil {
		s.logger.Info().Msgf("RestoreSnapshot: Found cross-suffix snapshot %s for branch %s", *snap.SnapshotId, branch)
		return snap
	}

	if s.config.RunnerConfig.DefaultBranch != "" {
		s.logger.Info().Msgf("RestoreSnapshot: Cross-suffix fallback, trying default branch %s", s.config.RunnerConfig.DefaultBranch)
		if err := replaceFilterValues(filters, "tag:"+snapshotTagKeyBranch, []string{s.getSnapshotTagValueDefaultBranch()}); err != nil {
			s.logger.Warn().Msgf("Failed to update branch filter: %v", err)
			return nil
		}
		if snap := s.searchSnapshots(ctx, filters); snap != nil {
			s.logger.Info().Msgf("RestoreSnapshot: Found cross-suffix snapshot %s from default branch %s", *snap.SnapshotId, s.config.RunnerConfig.DefaultBranch)
			return snap
		}
	}

	return nil
}

func replaceFilterValues(filters []types.Filter, name string, values []string) error {
	for i, filter := range filters {
		if *filter.Name == name {
			filters[i].Values = values
			return nil
		}
	}
	return fmt.Errorf("filter %s not found in filters: %v", name, utils.PrettyPrint(filters))
}
