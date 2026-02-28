package snapshot

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

const (
	defaultVolumeLifeDurationMinutes int32 = 20
	warmVolumeTTLMinutes             int32 = 120 // generous TTL for warm pool reuse
)

func (s *AWSSnapshotter) CreateSnapshot(ctx context.Context, mountPoint string) (*CreateSnapshotOutput, error) {
	gitBranch := s.config.GithubRef
	s.logger.Info().Msgf("CreateSnapshot: Using git ref: %s, Instance ID: %s, MountPoint: %s", gitBranch, s.config.InstanceID, mountPoint)

	// Load volume info from JSON file
	volumeInfo, err := s.loadVolumeInfo(mountPoint)
	if err != nil {
		return nil, fmt.Errorf("failed to load volume info: %w", err)
	}

	// 2. Operations on jobVolumeID
	if strings.HasPrefix(mountPoint, "/var/lib/docker") {
		s.logger.Info().Msgf("CreateSnapshot: Cleaning up useless files...")
		if _, err := s.runCommand(ctx, "sudo", "docker", "builder", "prune", "-f"); err != nil {
			s.logger.Warn().Msgf("Warning: failed to prune docker builder: %v", err)
		}

		s.logger.Info().Msgf("CreateSnapshot: Stopping docker service...")
		if _, err := s.runCommand(ctx, "sudo", "systemctl", "stop", "docker"); err != nil {
			s.logger.Warn().Msgf("Warning: failed to stop docker (may not be running or installed): %v", err)
		}
	}

	s.logger.Info().Msgf("CreateSnapshot: Unmounting %s (from device %s, volume %s)...", mountPoint, volumeInfo.DeviceName, volumeInfo.VolumeID)
	if _, err := s.runCommand(ctx, "sudo", "umount", mountPoint); err != nil {
		dfOutput, checkErr := s.runCommand(ctx, "df", mountPoint)
		if checkErr == nil && strings.Contains(string(dfOutput), mountPoint) {
			return nil, fmt.Errorf("failed to unmount %s: %w. Output: %s", mountPoint, err, string(dfOutput))
		}
		s.logger.Warn().Msgf("CreateSnapshot: Unmount of %s failed but it seems not mounted anymore: %v", mountPoint, err)
	} else {
		s.logger.Info().Msgf("CreateSnapshot: Successfully unmounted %s.", mountPoint)
	}

	// Extend TTL generously for warm pool reuse — volume stays attached for the next job
	_, err = s.ec2Client.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{volumeInfo.VolumeID},
		Tags: []types.Tag{
			{Key: aws.String(ttlTagKey), Value: aws.String(fmt.Sprintf("%d", time.Now().Add(time.Duration(warmVolumeTTLMinutes)*time.Minute).Unix()))},
		},
	})
	if err != nil {
		s.logger.Warn().Msgf("Failed to update TTL tag on volume %s: %v", volumeInfo.VolumeID, err)
	}

	// Snapshot while volume is still attached — AWS supports this as long as the FS is unmounted
	currentTime := time.Now()
	s.logger.Info().Msgf("CreateSnapshot: Creating snapshot '%s' from volume %s (still attached)...", s.config.SnapshotName, volumeInfo.VolumeID)
	snapshotTags := append(s.defaultTags(), types.Tag{
		Key: aws.String(nameTagKey), Value: aws.String(s.config.SnapshotName),
	})
	createSnapshotOutput, err := s.ec2Client.CreateSnapshot(ctx, &ec2.CreateSnapshotInput{
		VolumeId: aws.String(volumeInfo.VolumeID),
		TagSpecifications: []types.TagSpecification{
			{
				ResourceType: types.ResourceTypeSnapshot,
				Tags:         snapshotTags,
			},
		},
		Description: aws.String(fmt.Sprintf("Snapshot for branch %s taken at %s", s.config.GithubRef, currentTime.Format(time.RFC3339))),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot from volume %s: %w", volumeInfo.VolumeID, err)
	}
	newSnapshotID := *createSnapshotOutput.SnapshotId
	s.logger.Info().Msgf("CreateSnapshot: Snapshot %s creation initiated.", newSnapshotID)

	if volumeInfo.NewVolume {
		s.logger.Info().Msgf("CreateSnapshot: Initial snapshot from new volume, waiting for completion...")
	} else if s.config.WaitForCompletion {
		s.logger.Info().Msgf("CreateSnapshot: Waiting for snapshot completion (wait_for_completion=true)...")
	} else {
		s.logger.Info().Msgf("CreateSnapshot: Snapshot initiated, volume %s left attached for warm reuse.", volumeInfo.VolumeID)
		return &CreateSnapshotOutput{SnapshotID: newSnapshotID}, nil
	}

	s.logger.Info().Msgf("CreateSnapshot: Waiting for snapshot %s completion...", newSnapshotID)
	snapshotCompletedWaiter := ec2.NewSnapshotCompletedWaiter(s.ec2Client, defaultSnapshotCompletedWaiterOptions)
	if err := snapshotCompletedWaiter.Wait(ctx, &ec2.DescribeSnapshotsInput{SnapshotIds: []string{newSnapshotID}}, defaultSnapshotCompletedMaxWaitTime); err != nil {
		return nil, fmt.Errorf("snapshot %s did not complete in time: %w", newSnapshotID, err)
	}
	s.logger.Info().Msgf("CreateSnapshot: Snapshot %s completed. Volume %s left attached for warm reuse.", newSnapshotID, volumeInfo.VolumeID)

	return &CreateSnapshotOutput{SnapshotID: newSnapshotID}, nil
}
