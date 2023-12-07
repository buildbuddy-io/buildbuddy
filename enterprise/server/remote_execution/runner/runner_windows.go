//go:build windows

package runner

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
)

func (p *pool) registerContainerProviders(providers map[platform.ContainerType]container.Provider, executor *platform.ExecutorProperties) error {
	if executor.SupportsIsolation(platform.BareContainerType) {
		providers[platform.BareContainerType] = &bare.Provider{}
	}

	return nil
}

func (r *CommandRunner) startVFS() error {
	return nil
}

func (r *CommandRunner) prepareVFS(ctx context.Context, layout *container.FileSystemLayout) error {
	return nil
}

func (r *CommandRunner) removeVFS() error {
	return nil
}
