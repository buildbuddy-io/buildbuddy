//go:build windows

package runner

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
)

func (p *pool) initContainerProviders() error {
	providers := make(map[platform.ContainerType]container.Provider)
	providers[platform.BareContainerType] = &bare.Provider{}
	p.containerProviders = providers

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
