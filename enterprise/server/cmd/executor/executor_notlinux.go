//go:build !linux

package main

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

func setupRlimits() error {
	if *nofileLimit != 0 {
		return fmt.Errorf("executor.nofile_limit is only supported on Linux")
	}
	return nil
}

func setupCgroups() (*Cgroups, error) {
	return &Cgroups{}, nil
}

func setupNetworking(rootContext context.Context) {
}

func cleanupFUSEMounts() {
}

func cleanBuildRoot(ctx context.Context, buildRoot string) error {
	return disk.ForceRemove(ctx, buildRoot)
}

func migrateExt4ImagesToFileCache(fc interfaces.FileCache, cacheRoot string) error {
	return nil
}
