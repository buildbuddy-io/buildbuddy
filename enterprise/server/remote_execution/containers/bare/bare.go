package bare

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// bareCommandContainer executes commands directly, without any isolation
// between containers.
type bareCommandContainer struct {
	WorkDir string
}

func NewBareCommandContainer() container.CommandContainer {
	return &bareCommandContainer{}
}

func (c *bareCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string, creds container.PullCredentials) *interfaces.CommandResult {
	return commandutil.Run(ctx, command, workDir, &container.Stdio{})
}

func (c *bareCommandContainer) Create(ctx context.Context, workDir string) error {
	c.WorkDir = workDir
	return nil
}

func (c *bareCommandContainer) Exec(ctx context.Context, cmd *repb.Command, stdio *container.Stdio) *interfaces.CommandResult {
	return commandutil.Run(ctx, cmd, c.WorkDir, stdio)
}

func (c *bareCommandContainer) IsImageCached(ctx context.Context) (bool, error) { return false, nil }
func (c *bareCommandContainer) PullImage(ctx context.Context, creds container.PullCredentials) error {
	return nil
}
func (c *bareCommandContainer) Start(ctx context.Context) error   { return nil }
func (c *bareCommandContainer) Remove(ctx context.Context) error  { return nil }
func (c *bareCommandContainer) Pause(ctx context.Context) error   { return nil }
func (c *bareCommandContainer) Unpause(ctx context.Context) error { return nil }

func (c *bareCommandContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return &container.Stats{}, nil
}
