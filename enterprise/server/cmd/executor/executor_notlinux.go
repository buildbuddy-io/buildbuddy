//go:build !linux

package main

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

func setupCgroups() (string, error) {
	return "", nil
}

func execUnderInitProcess() error {
	return nil
}

func setupNetworking(rootContext context.Context) {
}

func cleanupFUSEMounts() {
}

func cleanBuildRoot(ctx context.Context, buildRoot string) error {
	return disk.ForceRemove(ctx, buildRoot)
}
