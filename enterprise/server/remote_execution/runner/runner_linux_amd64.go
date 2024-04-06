//go:build linux && amd64 && !android

package runner

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func (p *pool) registerFirecrackerProvider(providers map[platform.ContainerType]container.Provider, executor *platform.ExecutorProperties) error {
	if executor.SupportsIsolation(platform.FirecrackerContainerType) {
		p, err := firecracker.NewProvider(p.env, *rootDirectory)
		if err != nil {
			return status.FailedPreconditionErrorf("Failed to initialize firecracker container provider: %s", err)
		}
		providers[platform.FirecrackerContainerType] = p
	}
	return nil
}
