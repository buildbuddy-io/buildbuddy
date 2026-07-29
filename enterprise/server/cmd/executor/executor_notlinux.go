//go:build !linux

package main

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/llmproxy"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func setupCgroups() (*Cgroups, error) {
	return &Cgroups{}, nil
}

func setupNetworking(rootContext context.Context) {
}

func startLLMProxy(ctx context.Context) (*llmproxy.Service, error) {
	if *llmProxyEnabled {
		return nil, status.UnimplementedError("the executor LLM proxy requires Linux")
	}
	return nil, nil
}

func cleanupFUSEMounts() {
}

func cleanBuildRoot(ctx context.Context, buildRoot string) error {
	return disk.ForceRemove(ctx, buildRoot)
}

func migrateExt4ImagesToFileCache(fc interfaces.FileCache, cacheRoot string) error {
	return nil
}
