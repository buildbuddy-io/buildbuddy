package snaploader

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
)

type LoadSnapshotOptions struct {
	// The following fields are all required.
	MemSnapshotPath  string
	DiskSnapshotPath string
	KernelImagePath  string
	InitrdImagePath  string
	ContainerFSPath  string

	// This field is optional -- a snapshot may have a filesystem
	// stored with it or it may have one attached at runtime.
	WorkspaceFSPath string
}

func OptionsFromCache(ctx context.Context, env *environment.Env, snapshotID string) (*LoadSnapshotOptions, error) {
	return nil, nil
}

func OptionsToCache(ctx context.Context, env *environment.Env, snapOpts *LoadSnapshotOptions) (string, error) {
	return "", nil
}

/*
// in vmstart.go:
//   env := basicEnv{}
//   env.SetCache(diskCache{})
//   if *snapID {
//     opts, err := snaploader.OptionsFromCache(ctx, env, *snapID)
//     container := firecracker.NewContainer()
//     container.LoadFromSnapshot(opts)
//     container.Exec()
//   }
//
//   if *saveSnapshot {
//     opts, err := container.DumpToSnapshot(ctx)
//     snapID, err := snapopts.OptionsToCache(opts)
//   }
//
// in firecracker.Create():
//
*/
