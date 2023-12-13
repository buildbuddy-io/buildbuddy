//go:build windows

package runner

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func (p *pool) registerContainerProviders(providers map[platform.ContainerType]container.Provider, executor *platform.ExecutorProperties) error {
	if executor.SupportsIsolation(platform.BareContainerType) {
		providers[platform.BareContainerType] = &bare.Provider{}
	}

	return nil
}

func (r *commandRunner) startVFS() error {
	return nil
}

func (r *commandRunner) prepareVFS(ctx context.Context, layout *container.FileSystemLayout) error {
	return nil
}

func (r *commandRunner) removeVFS() error {
	return nil
}

func (r *commandRunner) hasMaxResourceUtilization(ctx context.Context, usageStats *repb.UsageStats) bool {
	return false
}
