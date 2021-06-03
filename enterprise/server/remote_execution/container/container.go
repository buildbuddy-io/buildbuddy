package container

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Stats holds represents a container's held resources.
type Stats struct {
	MemoryUsageBytes int64

	// TODO: add CPU usage once we have a reliable way to measure it.
	// CPU only applies to bare execution, since Docker and Containerd containers
	// are frozen when not in use, reducing their CPU usage to 0.
}

// CommandContainer provides an execution environment for commands.
type CommandContainer interface {
	// Run the given command within the container and remove the container after
	// it is done executing.
	//
	// It is approximately the same as calling PullImageIfNecessary, Create,
	// Exec, then Remove.
	Run(ctx context.Context, command *repb.Command, workingDir string) *interfaces.CommandResult

	// PullImageIfNecessary pulls the container image if it is not already
	// available locally.
	PullImageIfNecessary(ctx context.Context) error

	// Create creates a new container and starts a top-level process inside it
	// (`sleep infinity`) so that it stays alive and running until explicitly
	// removed. Note, this works slightly differently than commands like
	// `docker create` or `ctr containers create` -- in addition to creating the
	// container, it also puts it in a "ready to execute" state by starting the
	// top level process.
	Create(ctx context.Context, workingDir string) error
	// Exec runs a command inside a container, with the same working dir set when
	// creating the container.
	// If stdin is non-nil, the contents of stdin reader will be piped to the stdin of
	// the executed process.
	// If stdout is non-nil, the stdout of the executed process will be written to the
	// stdout writer.
	Exec(ctx context.Context, command *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult
	// Unpause un-freezes a container so that it can be used to execute commands.
	Unpause(ctx context.Context) error
	// Pause freezes a container so that it no longer consumes CPU resources.
	Pause(ctx context.Context) error
	// Remove kills any processes currently running inside the container and
	// removes any resources associated with the container itself.
	Remove(ctx context.Context) error

	// Stats returns the current resource usage of this container.
	Stats(ctx context.Context) (*Stats, error)
}
