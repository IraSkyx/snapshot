package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/rs/zerolog"
	runsOnConfig "github.com/runs-on/snapshot/internal/config"
	"github.com/runs-on/snapshot/internal/utils"
)

const (
	snapshotTagKeyArch       = "runs-on-snapshot-arch"
	snapshotTagKeyPlatform   = "runs-on-snapshot-platform"
	snapshotTagKeyBranch     = "runs-on-snapshot-branch"
	snapshotTagKeyRepository = "runs-on-snapshot-repository"
	snapshotTagKeyVersion    = "runs-on-snapshot-version"
	snapshotTagKeyPath       = "runs-on-snapshot-path"
	nameTagKey               = "Name"
	timestampTagKey          = "runs-on-timestamp"
	ttlTagKey                = "runs-on-delete-after"

	baseMountPoint    = "/mnt/runs-on-snapshot"
	suggestedDeviceName = "/dev/sdf"

	defaultVolumeInUseMaxWaitTime       = 5 * time.Minute
	defaultVolumeAvailableMaxWaitTime   = 5 * time.Minute
	defaultSnapshotCompletedMaxWaitTime = 10 * time.Minute
)

var defaultSnapshotCompletedWaiterOptions = func(o *ec2.SnapshotCompletedWaiterOptions) {
	o.MinDelay = 2 * time.Second
	o.MaxDelay = 5 * time.Second
}

var defaultVolumeInUseWaiterOptions = func(o *ec2.VolumeInUseWaiterOptions) {
	o.MinDelay = 1 * time.Second
	o.MaxDelay = 3 * time.Second
}

var defaultVolumeAvailableWaiterOptions = func(o *ec2.VolumeAvailableWaiterOptions) {
	o.MinDelay = 1 * time.Second
	o.MaxDelay = 3 * time.Second
}

func sanitizePath(path string) string {
	return strings.Trim(strings.ReplaceAll(path, "/", "-"), "-")
}

type Snapshotter interface {
	CreateSnapshot(ctx context.Context, snapshot *Snapshot) error
	GetSnapshot(ctx context.Context, id string) (*Snapshot, error)
	DeleteSnapshot(ctx context.Context, id string) error
}

type AWSSnapshotter struct {
	logger    *zerolog.Logger
	config    *runsOnConfig.Config
	ec2Client *ec2.Client
}

type Snapshot struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type RestoreSnapshotOutput struct {
	VolumeID   string
	DeviceName string
	NewVolume  bool
}

type CreateSnapshotOutput struct {
	SnapshotID string
}

type VolumeInfo struct {
	VolumeID     string `json:"volume_id"`
	DeviceName   string `json:"device_name"`
	MountPoint   string `json:"mount_point"`
	AttachmentID string `json:"attachment_id,omitempty"`
	NewVolume    bool   `json:"new_volume,omitempty"`
}

func NewAWSSnapshotter(ctx context.Context, logger *zerolog.Logger, cfg *runsOnConfig.Config) (*AWSSnapshotter, error) {
	awsConfig, err := utils.GetAWSClientFromEC2IMDS(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS SDK config: %w", err)
	}

	if cfg.InstanceID == "" {
		return nil, fmt.Errorf("instanceID is required")
	}

	if cfg.Az == "" {
		return nil, fmt.Errorf("az is required")
	}

	if cfg.GithubRepository == "" {
		return nil, fmt.Errorf("githubRepository is required")
	}

	if cfg.GithubRef == "" {
		return nil, fmt.Errorf("githubRef is required")
	}

	if cfg.CustomTags == nil {
		cfg.CustomTags = []runsOnConfig.Tag{}
	}

	sanitizedGithubRef := strings.TrimPrefix(cfg.GithubRef, "refs/")
	sanitizedGithubRef = strings.ReplaceAll(sanitizedGithubRef, "/", "-")
	if len(sanitizedGithubRef) > 40 {
		sanitizedGithubRef = sanitizedGithubRef[:40]
	}

	currentTime := time.Now()
	if cfg.SnapshotName == "" {
		cfg.SnapshotName = fmt.Sprintf("runs-on-snapshot-%s-%s", sanitizedGithubRef, currentTime.Format("20060102-150405"))
	}

	if cfg.VolumeName == "" {
		cfg.VolumeName = fmt.Sprintf("runs-on-volume-%s-%s", sanitizedGithubRef, currentTime.Format("20060102-150405"))
	}

	return &AWSSnapshotter{
		logger:    logger,
		config:    cfg,
		ec2Client: ec2.NewFromConfig(*awsConfig),
	}, nil
}

func (s *AWSSnapshotter) arch() string {
	return runtime.GOARCH
}

func (s *AWSSnapshotter) platform() string {
	return runtime.GOOS
}

func (s *AWSSnapshotter) defaultTags() []types.Tag {
	tags := []types.Tag{
		{Key: aws.String(snapshotTagKeyVersion), Value: aws.String(s.config.Version)},
		{Key: aws.String(snapshotTagKeyRepository), Value: aws.String(s.config.GithubRepository)},
		{Key: aws.String(snapshotTagKeyBranch), Value: aws.String(s.getSnapshotTagValue())},
		{Key: aws.String(snapshotTagKeyArch), Value: aws.String(s.arch())},
		{Key: aws.String(snapshotTagKeyPlatform), Value: aws.String(s.platform())},
		{Key: aws.String(snapshotTagKeyPath), Value: aws.String("_all_")},
	}
	for _, tag := range s.config.CustomTags {
		tags = append(tags, types.Tag{Key: aws.String(tag.Key), Value: aws.String(tag.Value)})
	}
	return tags
}

func (s *AWSSnapshotter) saveVolumeInfo(volumeInfo *VolumeInfo) error {
	infoPath := getVolumeInfoPath()

	if err := os.MkdirAll(filepath.Dir(infoPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory for volume info: %w", err)
	}

	data, err := json.MarshalIndent(volumeInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal volume info: %w", err)
	}

	if err := os.WriteFile(infoPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write volume info file: %w", err)
	}

	return nil
}

func (s *AWSSnapshotter) loadVolumeInfo() (*VolumeInfo, error) {
	infoPath := getVolumeInfoPath()
	data, err := os.ReadFile(infoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read volume info file: %w", err)
	}

	var volumeInfo VolumeInfo
	if err := json.Unmarshal(data, &volumeInfo); err != nil {
		return nil, fmt.Errorf("failed to unmarshal volume info: %w", err)
	}

	return &volumeInfo, nil
}

func (s *AWSSnapshotter) getSnapshotTagValue() string {
	return s.config.GithubRef
}

func (s *AWSSnapshotter) getSnapshotTagValueDefaultBranch() string {
	return s.config.RunnerConfig.DefaultBranch
}

func (s *AWSSnapshotter) runCommand(ctx context.Context, name string, arg ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, arg...)
	s.logger.Info().Msgf("Executing command: %s %s", name, strings.Join(arg, " "))
	output, err := cmd.CombinedOutput()
	if err != nil {
		s.logger.Warn().Msgf("Command failed: %s %s\nOutput:\n%s\nError: %v", name, strings.Join(arg, " "), string(output), err)
		return output, fmt.Errorf("command '%s %s' failed: %s: %w", name, strings.Join(arg, " "), string(output), err)
	}
	logOutput := string(output)
	if len(logOutput) > 400 {
		logOutput = logOutput[:200] + "... (output truncated)"
	}
	s.logger.Info().Msgf("Command successful. Output (first 200 chars or less):\n%s", logOutput)
	return output, nil
}

func getVolumeInfoPath() string {
	return filepath.Join("/runs-on", "snapshot.json")
}
