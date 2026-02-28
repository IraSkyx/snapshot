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

func handleMainExecution(action *githubactions.Action, ctx context.Context, logger *zerolog.Logger) {
	cfg := config.NewConfigFromInputs(action)

	snapshotter, err := snapshot.NewAWSSnapshotter(ctx, logger, cfg)
	if err != nil {
		action.Fatalf("Failed to create snapshotter: %v", err)
	}

	output, err := snapshotter.RestoreSnapshot(ctx)
	if err != nil {
		action.Fatalf("Failed to restore snapshot: %v", err)
	}

	action.Infof("Snapshot restored into volume %s", output.VolumeID)
	action.Infof("Action finished.")
}

func handlePostExecution(action *githubactions.Action, ctx context.Context, logger *zerolog.Logger) {
	action.Infof("Running post-execution phase...")
	cfg := config.NewConfigFromInputs(action)

	if !cfg.Save {
		action.Infof("Skipping snapshot creation as 'save' is set to false.")
		return
	}

	snapshotter, err := snapshot.NewAWSSnapshotter(ctx, logger, cfg)
	if err != nil {
		action.Fatalf("Failed to create snapshotter: %v", err)
	}

	snap, err := snapshotter.CreateSnapshot(ctx)
	if err != nil {
		action.Fatalf("Failed to create snapshot: %v", err)
	}

	action.Infof("Snapshot created: %s", snap.SnapshotID)
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
