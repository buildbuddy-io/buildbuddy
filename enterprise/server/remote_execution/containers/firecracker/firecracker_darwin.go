// +build darwin

package firecracker

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type ContainerOpts struct {
	// The OCI container image. ex "alpine:latest"
	ContainerImage string

	// The action directory with inputs / outputs
	ActionWorkingDirectory string

	// The number of CPUs to allocate to this VM.
	NumCPUs int64

	// The amount of RAM, in MB, to allocate to this VM.
	MemSizeMB int64

	// Whether or not to enable networking.
	EnableNetworking bool

	// Optional flags -- these will default to sane values.
	// They are here primarily for debugging and running
	// VMs outside of the normal action-execution framework.

	// DebugMode runs init in debugmode and enables stdin/stdout so
	// that machines can be logged into via the console.
	DebugMode bool

	// ForceVMIdx forces a machine to use a particular vm index,
	// allowing for multiple locally-started VMs to avoid using
	// conflicting network interfaces.
	ForceVMIdx int
}

type firecrackerContainer struct{}

func NewContainer(ctx context.Context, opts interface{}) (*firecrackerContainer, error) {
	c := &firecrackerContainer{}
	return c, nil
}

func (c *firecrackerContainer) Run(ctx context.Context, command *repb.Command, actionWorkingDir string) *interfaces.CommandResult {
	return &interfaces.CommandResult{}
}

func (c *firecrackerContainer) Create(ctx context.Context, actionWorkingDir string) error {
	return status.UnimplementedError("Not yet implemented.")
}

func (c *firecrackerContainer) Exec(ctx context.Context, cmd *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	return &interfaces.CommandResult{}
}

func (c *firecrackerContainer) PullImageIfNecessary(ctx context.Context) error {
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
