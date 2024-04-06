//go:build linux && !amd64 && !android

package runner

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
)

func (p *pool) registerFirecrackerProvider(providers map[platform.ContainerType]container.Provider, executor *platform.ExecutorProperties) error {
	return nil
}
