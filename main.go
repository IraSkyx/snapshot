package main

import (
	"context"
	"flag"
	"os"

	"github.com/rs/zerolog"
	"github.com/runs-on/snapshot/internal/config"
	"github.com/runs-on/snapshot/internal/snapshot"
	"github.com/sethvargo/go-githubactions"
)

// handleMainExecution contains the original main logic.
func handleMainExecution(action *githubactions.Action, ctx context.Context, logger *zerolog.Logger) {
	cfg := config.NewConfigFromInputs(action)

	for i, path := range cfg.Paths {
		pathCfg := cfg.ForPath(i)
		action.Infof("Restoring volume for %s (%d/%d)...", path, i+1, len(cfg.Paths))
		snapshotter, err := snapshot.NewAWSSnapshotter(ctx, logger, pathCfg)
		if err != nil {
			action.Errorf("Failed to create snapshotter for %s: %v", path, err)
			continue
		}
		snapshotOutput, err := snapshotter.RestoreSnapshot(ctx, path)
		if err != nil {
			action.Errorf("Failed to restore snapshot for %s: %v", path, err)
			continue
		}
		action.Infof("Snapshot restored into volume %s for %s", snapshotOutput.VolumeID, path)
	}

	action.Infof("Action finished.")
}

// handlePostExecution contains the logic for the post-execution phase.
func handlePostExecution(action *githubactions.Action, ctx context.Context, logger *zerolog.Logger) {
	action.Infof("Running post-execution phase...")
	cfg := config.NewConfigFromInputs(action)

	if !cfg.Save {
		action.Infof("Skipping snapshot creation as 'save' is set to false.")
		action.Infof("Post-execution phase finished.")
		return
	}

	for i, path := range cfg.Paths {
		pathCfg := cfg.ForPath(i)
		action.Infof("Snapshotting volume for %s (%d/%d)...", path, i+1, len(cfg.Paths))
		snapshotter, err := snapshot.NewAWSSnapshotter(ctx, logger, pathCfg)
		if err != nil {
			action.Errorf("Failed to create snapshotter for %s: %v", path, err)
			continue
		}
		snap, err := snapshotter.CreateSnapshot(ctx, path)
		if err != nil {
			action.Errorf("Failed to snapshot volume for %s: %v", path, err)
			continue
		}
		action.Infof("Snapshot created for %s: %s. Note that it might take a few minutes to be available for use.", path, snap.SnapshotID)
	}
	action.Infof("Post-execution phase finished.")
}

func main() {
	ctx := context.Background()
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	postFlag := flag.Bool("post", false, "Indicates the post-execution phase")
	flag.Parse()

	action := githubactions.New()

	if *postFlag {
		handlePostExecution(action, ctx, &logger)
	} else {
		handleMainExecution(action, ctx, &logger)
	}
}
