package bare

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type Opts struct {
	// EnableStats specifies whether to collect stats while the command is
	// in progress.
	EnableStats bool
}

// bareCommandContainer executes commands directly, without any isolation
// between containers.
type bareCommandContainer struct {
	opts    *Opts
	WorkDir string
}

func NewBareCommandContainer(opts *Opts) container.CommandContainer {
	return &bareCommandContainer{opts: opts}
}

func (c *bareCommandContainer) Run(ctx context.Context, command *repb.Command, workDir string, creds container.PullCredentials) *interfaces.CommandResult {
	opts := &commandutil.RunOpts{EnableStats: c.opts.EnableStats}
	return commandutil.Run(ctx, command, workDir, opts)
}

func (c *bareCommandContainer) Create(ctx context.Context, workDir string) error {
	c.WorkDir = workDir
	return nil
}

func (c *bareCommandContainer) Exec(ctx context.Context, cmd *repb.Command, opts *container.ExecOpts) *interfaces.CommandResult {
	runOpts := &commandutil.RunOpts{
		ExecOpts:    opts,
		EnableStats: c.opts.EnableStats,
	}
	return commandutil.Run(ctx, cmd, c.WorkDir, runOpts)
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
