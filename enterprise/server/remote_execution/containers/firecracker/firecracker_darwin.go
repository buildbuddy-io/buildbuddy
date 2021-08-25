// +build darwin
// +build !ios

package firecracker

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type firecrackerContainer struct{}

func NewContainer(env environment.Env, opts ContainerOpts) (*firecrackerContainer, error) {
	c := &firecrackerContainer{}
	return c, nil
}

func (c *firecrackerContainer) Run(ctx context.Context, command *repb.Command, actionWorkingDir string, creds *container.PullCredentials) *interfaces.CommandResult {
	return &interfaces.CommandResult{}
}

func (c *firecrackerContainer) Create(ctx context.Context, actionWorkingDir string) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *firecrackerContainer) Exec(ctx context.Context, cmd *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	return &interfaces.CommandResult{}
}

func (c *firecrackerContainer) PullImageIfNecessary(ctx context.Context, creds *container.PullCredentials) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *firecrackerContainer) Remove(ctx context.Context) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *firecrackerContainer) Pause(ctx context.Context) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *firecrackerContainer) Unpause(ctx context.Context) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *firecrackerContainer) Wait(ctx context.Context) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *firecrackerContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return nil, status.UnimplementedError("Not yet implemented.")
}
