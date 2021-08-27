package bare

import (
	"context"
	"io"

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

func (c *bareCommandContainer) Run(ctx context.Context, task *repb.ExecutionTask, workDir string, fsLayout *container.FileSystemLayout) *interfaces.CommandResult {
	return commandutil.Run(ctx, task.GetCommand(), workDir)
}

func (c *bareCommandContainer) Create(ctx context.Context, workDir string) error {
	c.WorkDir = workDir
	return nil
}

func (c *bareCommandContainer) Exec(ctx context.Context, task *repb.ExecutionTask, fsLayout *container.FileSystemLayout, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	// TODO(siggisim): Wire up stdin/stdout to support persistent workers on bare commands.
	return c.Run(ctx, task, c.WorkDir, fsLayout)
}

func (c *bareCommandContainer) PullImageIfNecessary(ctx context.Context) error { return nil }
func (c *bareCommandContainer) Start(ctx context.Context) error                { return nil }
func (c *bareCommandContainer) Remove(ctx context.Context) error               { return nil }
func (c *bareCommandContainer) Pause(ctx context.Context) error                { return nil }
func (c *bareCommandContainer) Unpause(ctx context.Context) error              { return nil }

func (c *bareCommandContainer) Stats(ctx context.Context) (*container.Stats, error) {
	return &container.Stats{}, nil
}
