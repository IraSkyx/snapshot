package snapshot

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/runs-on/snapshot/internal/utils"
)

// tryWarmRestore attempts to reuse a volume already attached from a previous job on the same instance.
// Returns a successful result if the volume is still attached and mountable, or an error to fall through to cold restore.
func (s *AWSSnapshotter) tryWarmRestore(ctx context.Context, mountPoint string) (*RestoreSnapshotOutput, error) {
	volumeInfo, err := s.loadVolumeInfo(mountPoint)
	if err != nil {
		return nil, fmt.Errorf("no volume info: %w", err)
	}

	s.logger.Info().Msgf("RestoreSnapshot: Found existing volume info for %s (volume %s), checking if still attached...", mountPoint, volumeInfo.VolumeID)

	descOutput, err := s.ec2Client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []string{volumeInfo.VolumeID},
		Filters: []types.Filter{
			{Name: aws.String("attachment.instance-id"), Values: []string{s.config.InstanceID}},
			{Name: aws.String("attachment.status"), Values: []string{"attached"}},
		},
	})
	if err != nil || len(descOutput.Volumes) == 0 {
		s.logger.Info().Msgf("RestoreSnapshot: Volume %s is no longer attached to this instance", volumeInfo.VolumeID)
		return nil, fmt.Errorf("volume not attached")
	}

	s.logger.Info().Msgf("RestoreSnapshot: Warm volume %s detected, reusing (skipping create/attach)...", volumeInfo.VolumeID)

	if strings.HasPrefix(mountPoint, "/var/lib/docker") {
		if _, err := s.runCommand(ctx, "sudo", "systemctl", "stop", "docker"); err != nil {
			s.logger.Warn().Msgf("RestoreSnapshot: failed to stop docker: %v", err)
		}
	}

	if _, err := s.runCommand(ctx, "mountpoint", "-q", mountPoint); err != nil {
		if _, err := s.runCommand(ctx, "sudo", "mkdir", "-p", mountPoint); err != nil {
			return nil, fmt.Errorf("failed to create mount point: %w", err)
		}
		if _, err := s.runCommand(ctx, "sudo", "mount", volumeInfo.DeviceName, mountPoint); err != nil {
			s.logger.Warn().Msgf("RestoreSnapshot: Warm mount failed (%v), detaching stale volume %s and falling through to cold restore", err, volumeInfo.VolumeID)
			s.ec2Client.DetachVolume(ctx, &ec2.DetachVolumeInput{
				VolumeId:   aws.String(volumeInfo.VolumeID),
				InstanceId: aws.String(s.config.InstanceID),
			})
			return nil, fmt.Errorf("warm mount failed: %w", err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Warm volume mounted at %s", mountPoint)
	} else {
		s.logger.Info().Msgf("RestoreSnapshot: %s already mounted (warm hit)", mountPoint)
	}

	if !strings.HasPrefix(mountPoint, "/var/lib/docker") {
		if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
			s.runCommand(ctx, "sudo", "chown", sudoUser+":"+sudoUser, mountPoint)
		}
	}

	if strings.HasPrefix(mountPoint, "/var/lib/docker") {
		if _, err := s.runCommand(ctx, "sudo", "systemctl", "start", "docker"); err != nil {
			s.logger.Warn().Msgf("RestoreSnapshot: failed to start docker after warm mount: %v", err)
		}
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

// RestoreSnapshot finds the latest snapshot for the current git branch,
// creates a volume from it (or a new volume if no snapshot exists),
// attaches it to the instance, and mounts it to the specified mountPoint.
func (s *AWSSnapshotter) RestoreSnapshot(ctx context.Context, mountPoint string) (*RestoreSnapshotOutput, error) {
	if output, err := s.tryWarmRestore(ctx, mountPoint); err == nil {
		return output, nil
	}

	gitBranch := s.config.GithubRef
	s.logger.Info().Msgf("RestoreSnapshot: Cold restore for %s, using git ref: %s", mountPoint, gitBranch)

	var err error

	var newVolume *types.Volume
	var volumeIsNewAndUnformatted bool
	// 1. Find latest snapshot for branch
	filters := []types.Filter{
		{Name: aws.String("status"), Values: []string{string(types.SnapshotStateCompleted)}},
	}
	for _, tag := range s.defaultTags() {
		filters = append(filters, types.Filter{Name: aws.String(fmt.Sprintf("tag:%s", *tag.Key)), Values: []string{*tag.Value}})
	}
	s.logger.Info().Msgf("RestoreSnapshot: Searching for the latest snapshot for branch: %s and filters: %s", gitBranch, utils.PrettyPrint(filters))
	snapshotsOutput, err := s.ec2Client.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{
		Filters:  filters,
		OwnerIds: []string{"self"}, // Or specific account ID if needed
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe snapshots for branch %s: %w", gitBranch, err)
	}

	var latestSnapshot *types.Snapshot
	if len(snapshotsOutput.Snapshots) > 0 {
		// Find most recent snapshot by comparing timestamps
		latestSnapshot = &snapshotsOutput.Snapshots[0]
		for _, snap := range snapshotsOutput.Snapshots {
			if snapTime := snap.StartTime; snapTime.After(*latestSnapshot.StartTime) {
				latestSnapshot = &snap
			}
		}
		s.logger.Info().Msgf("RestoreSnapshot: Found latest snapshot %s for branch %s", *latestSnapshot.SnapshotId, gitBranch)
	} else if s.config.RunnerConfig.DefaultBranch != "" {
		// Try finding snapshot from default branch
		if err := replaceFilterValues(filters, "tag:"+snapshotTagKeyBranch, []string{s.getSnapshotTagValueDefaultBranch()}); err != nil {
			return nil, fmt.Errorf("failed to find default branch filter: %w", err)
		}

		s.logger.Info().Msgf("RestoreSnapshot: No snapshot found for branch %s, trying default branch %s with filters: %s", gitBranch, s.config.RunnerConfig.DefaultBranch, utils.PrettyPrint(filters))

		defaultBranchSnapshotsOutput, err := s.ec2Client.DescribeSnapshots(ctx, &ec2.DescribeSnapshotsInput{
			Filters:  filters,
			OwnerIds: []string{"self"},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to describe snapshots for default branch %s: %w", s.config.RunnerConfig.DefaultBranch, err)
		}

		if len(defaultBranchSnapshotsOutput.Snapshots) > 0 {
			latestSnapshot = &defaultBranchSnapshotsOutput.Snapshots[0]
			for _, snap := range defaultBranchSnapshotsOutput.Snapshots {
				if snapTime := snap.StartTime; snapTime.After(*latestSnapshot.StartTime) {
					latestSnapshot = &snap
				}
			}
			s.logger.Info().Msgf("RestoreSnapshot: Found latest snapshot %s from default branch %s", *latestSnapshot.SnapshotId, s.config.RunnerConfig.DefaultBranch)
		} else {
			s.logger.Info().Msgf("RestoreSnapshot: No existing snapshot found for branch %s or default branch %s. A new volume will be created.", gitBranch, s.config.RunnerConfig.DefaultBranch)
		}
	}

	commonVolumeTags := append(s.defaultTags(), []types.Tag{
		{Key: aws.String(nameTagKey), Value: aws.String(s.config.VolumeName)},
		{Key: aws.String(ttlTagKey), Value: aws.String(fmt.Sprintf("%d", time.Now().Add(time.Duration(defaultVolumeLifeDurationMinutes)*time.Minute).Unix()))},
	}...)

	s.logger.Info().Msgf("RestoreSnapshot: common volume tags: %s", utils.PrettyPrint(commonVolumeTags))

	// Use snapshot only if its size is at least the default volume size, otherwise create a new volume
	// TODO: maybe just expand the volume size to snapshot size + 10GB, and resize disk
	if latestSnapshot != nil && latestSnapshot.VolumeSize != nil && *latestSnapshot.VolumeSize >= s.config.VolumeSize {
		// 2. Create Volume from Snapshot
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
		// Throughput is only supported for gp3 volumes
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
		volumeIsNewAndUnformatted = false // Volume from snapshot is already formatted
		s.logger.Info().Msgf("RestoreSnapshot: Created volume %s from snapshot %s", *newVolume.VolumeId, *latestSnapshot.SnapshotId)
	} else {
		// 3. No snapshot found, create a new volume
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
		// Throughput is only supported for gp3 volumes
		if s.config.VolumeType == types.VolumeTypeGp3 {
			createVolumeInput.Throughput = aws.Int32(s.config.VolumeThroughput)
		}
		createVolumeOutput, err := s.ec2Client.CreateVolume(ctx, createVolumeInput)
		if err != nil {
			return nil, fmt.Errorf("failed to create new volume: %w", err)
		}
		newVolume = &types.Volume{VolumeId: createVolumeOutput.VolumeId}
		volumeIsNewAndUnformatted = true // New volume needs formatting
		s.logger.Info().Msgf("RestoreSnapshot: Created new blank volume %s", *newVolume.VolumeId)
	}

	defer func() {
		s.logger.Info().Msgf("RestoreSnapshot: Deferring cleanup of volume %s", *newVolume.VolumeId)
		if err != nil {
			s.logger.Error().Msgf("RestoreSnapshot: Error: %v", err)
			if newVolume != nil {
				s.logger.Info().Msgf("RestoreSnapshot: Deleting volume %s", *newVolume.VolumeId)
				_, err := s.ec2Client.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: newVolume.VolumeId})
				if err != nil {
					s.logger.Error().Msgf("RestoreSnapshot: Error deleting volume %s: %v", *newVolume.VolumeId, err)
				}
			}
		}
	}()

	// 4. Wait for volume to be 'available'
	s.logger.Info().Msgf("RestoreSnapshot: Waiting for volume %s to become available...", *newVolume.VolumeId)
	volumeAvailableWaiter := ec2.NewVolumeAvailableWaiter(s.ec2Client, defaultVolumeAvailableWaiterOptions)
	if err := volumeAvailableWaiter.Wait(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{*newVolume.VolumeId}}, defaultVolumeAvailableMaxWaitTime); err != nil {
		return nil, fmt.Errorf("volume %s did not become available in time: %w", *newVolume.VolumeId, err)
	}
	s.logger.Info().Msgf("RestoreSnapshot: Volume %s is available.", *newVolume.VolumeId)

	// 5. Attach Volume
	suggestedDevice := suggestedDeviceNameForIndex(s.config.DeviceIndex)
	s.logger.Info().Msgf("RestoreSnapshot: Attaching volume %s to instance %s as %s", *newVolume.VolumeId, s.config.InstanceID, suggestedDevice)
	attachOutput, err := s.ec2Client.AttachVolume(ctx, &ec2.AttachVolumeInput{
		Device:     aws.String(suggestedDevice),
		InstanceId: aws.String(s.config.InstanceID),
		VolumeId:   newVolume.VolumeId,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to attach volume %s to instance %s: %w", *newVolume.VolumeId, s.config.InstanceID, err)
	}
	actualDeviceName := *attachOutput.Device
	s.logger.Info().Msgf("RestoreSnapshot: Volume %s attach initiated, device hint: %s. Waiting for attachment...", *newVolume.VolumeId, actualDeviceName)

	volumeInUseWaiter := ec2.NewVolumeInUseWaiter(s.ec2Client, defaultVolumeInUseWaiterOptions)
	err = volumeInUseWaiter.Wait(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []string{*newVolume.VolumeId},
		Filters: []types.Filter{
			{
				Name:   aws.String("attachment.status"),
				Values: []string{"attached"},
			},
		},
	}, defaultVolumeInUseMaxWaitTime)
	if err != nil {
		return nil, fmt.Errorf("volume %s did not attach successfully and current state unknown: %w", *newVolume.VolumeId, err)
	}
	// Fetch volume details again to confirm device name, as the attachOutput.Device might be a suggestion
	// and the waiter confirms attachment, not necessarily the final device name if it changed.
	descVolOutput, descErr := s.ec2Client.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{*newVolume.VolumeId}})
	s.logger.Info().Msgf("RestoreSnapshot: Volume %s attachments: %v", *newVolume.VolumeId, descVolOutput.Volumes[0].Attachments)
	if descErr == nil && len(descVolOutput.Volumes) > 0 && len(descVolOutput.Volumes[0].Attachments) > 0 {
		actualDeviceName = *descVolOutput.Volumes[0].Attachments[0].Device
	} else {
		return nil, fmt.Errorf("volume %s did not attach successfully and current state unknown: %w", *newVolume.VolumeId, err)
	}
	s.logger.Info().Msgf("RestoreSnapshot: Volume %s attached as %s.", *newVolume.VolumeId, actualDeviceName)

	if strings.HasPrefix(mountPoint, "/var/lib/docker") {
		s.logger.Info().Msgf("RestoreSnapshot: Stopping docker service...")
		if _, err := s.runCommand(ctx, "sudo", "systemctl", "stop", "docker"); err != nil {
			s.logger.Warn().Msgf("RestoreSnapshot: failed to stop docker (may not be running or installed): %v", err)
		}
	}

	s.logger.Info().Msgf("RestoreSnapshot: Attempting to unmount %s (defensive)", mountPoint)
	if _, err := s.runCommand(ctx, "sudo", "umount", mountPoint); err != nil {
		s.logger.Warn().Msgf("RestoreSnapshot: Defensive unmount of %s failed (likely not mounted): %v", mountPoint, err)
	}

	// Match the NVMe device to our volume using the serial number.
	// AWS NVMe serials are the volume ID without the dash: "vol-0abc" → "vol0abc"
	expectedSerial := strings.Replace(*newVolume.VolumeId, "-", "", 1)
	s.logger.Info().Msgf("RestoreSnapshot: Looking for NVMe device with serial %s (volume %s)...", expectedSerial, *newVolume.VolumeId)
	var found bool
	for attempt := 0; attempt < 10; attempt++ {
		lsblkOutput, lsblkErr := s.runCommand(ctx, "lsblk", "-d", "-n", "-o", "PATH,SERIAL")
		if lsblkErr != nil {
			s.logger.Warn().Msgf("RestoreSnapshot: lsblk failed (attempt %d): %v", attempt+1, lsblkErr)
			time.Sleep(1 * time.Second)
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
		s.logger.Info().Msgf("RestoreSnapshot: Device not yet visible (attempt %d/10), retrying...", attempt+1)
		time.Sleep(1 * time.Second)
	}
	if !found {
		return nil, fmt.Errorf("could not find NVMe device for volume %s (expected serial %s)", *newVolume.VolumeId, expectedSerial)
	}
	s.logger.Info().Msgf("RestoreSnapshot: Matched volume %s to device %s", *newVolume.VolumeId, actualDeviceName)

	// Save volume info to JSON file
	volumeInfo := &VolumeInfo{
		VolumeID:   *newVolume.VolumeId,
		DeviceName: actualDeviceName,
		MountPoint: mountPoint,
		NewVolume:  volumeIsNewAndUnformatted,
	}
	if err := s.saveVolumeInfo(volumeInfo); err != nil {
		s.logger.Warn().Msgf("RestoreSnapshot: Failed to save volume info: %v", err)
	}

	if volumeIsNewAndUnformatted {
		s.logger.Info().Msgf("RestoreSnapshot: Formatting new volume %s (%s) with ext4...", *newVolume.VolumeId, actualDeviceName)
		if _, err := s.runCommand(ctx, "sudo", "mkfs.ext4", "-F", actualDeviceName); err != nil { // -F to force if already formatted by mistake or small
			return nil, fmt.Errorf("failed to format device %s: %w", actualDeviceName, err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Device %s formatted.", actualDeviceName)
	}

	s.logger.Info().Msgf("RestoreSnapshot: Creating mount point %s if it doesn't exist...", mountPoint)
	if _, err := s.runCommand(ctx, "sudo", "mkdir", "-p", mountPoint); err != nil {
		return nil, fmt.Errorf("failed to create mount point %s: %w", mountPoint, err)
	}

	s.logger.Info().Msgf("RestoreSnapshot: Mounting %s to %s...", actualDeviceName, mountPoint)
	if _, err := s.runCommand(ctx, "sudo", "mount", actualDeviceName, mountPoint); err != nil {
		return nil, fmt.Errorf("failed to mount %s to %s: %w", actualDeviceName, mountPoint, err)
	}
	s.logger.Info().Msgf("RestoreSnapshot: Device %s mounted to %s.", actualDeviceName, mountPoint)

	if !strings.HasPrefix(mountPoint, "/var/lib/docker") {
		if sudoUser := os.Getenv("SUDO_USER"); sudoUser != "" {
			s.logger.Info().Msgf("RestoreSnapshot: Fixing ownership of %s for user %s...", mountPoint, sudoUser)
			if _, err := s.runCommand(ctx, "sudo", "chown", "-R", sudoUser+":"+sudoUser, mountPoint); err != nil {
				s.logger.Warn().Msgf("RestoreSnapshot: failed to chown %s to %s: %v", mountPoint, sudoUser, err)
			}
		}
	}

	if strings.HasPrefix(mountPoint, "/var/lib/docker") {
		s.logger.Info().Msgf("RestoreSnapshot: Starting docker service...")
		if _, err := s.runCommand(ctx, "sudo", "systemctl", "start", "docker"); err != nil {
			return nil, fmt.Errorf("failed to start docker after mounting: %w", err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Docker service started.")

		s.logger.Info().Msgf("RestoreSnapshot: Displaying docker disk usage...")
		if _, err := s.runCommand(ctx, "sudo", "docker", "system", "info"); err != nil {
			s.logger.Warn().Msgf("RestoreSnapshot: failed to display docker info: %v. Docker snapshot may not be working so unmounting docker folder.", err)
			// Try to unmount docker folder on error
			if _, err := s.runCommand(ctx, "sudo", "umount", mountPoint); err != nil {
				s.logger.Warn().Msgf("RestoreSnapshot: failed to unmount docker folder: %v", err)
			}
			return nil, fmt.Errorf("failed to display docker disk usage: %w", err)
		}
		s.logger.Info().Msgf("RestoreSnapshot: Docker disk usage displayed.")
	}

	return &RestoreSnapshotOutput{VolumeID: *newVolume.VolumeId, DeviceName: actualDeviceName, NewVolume: volumeIsNewAndUnformatted}, nil
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
