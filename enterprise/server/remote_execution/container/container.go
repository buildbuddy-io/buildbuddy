package container

import (
	"context"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/docker/distribution/reference"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

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
	Run(ctx context.Context, command *repb.Command, workingDir string, creds *PullCredentials) *interfaces.CommandResult

	// PullImageIfNecessary pulls the container image if it is not already
	// available locally.
	PullImageIfNecessary(ctx context.Context, creds *PullCredentials) error

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

type PullCredentials struct {
	Username string
	Password string
}

func (p *PullCredentials) IsEmpty() bool {
	return p == nil || *p == PullCredentials{}
}

func (p *PullCredentials) String() string {
	if p.IsEmpty() {
		return ""
	}
	return p.Username + ":" + p.Password
}

// GetPullCredentials returns the image pull credentials for the given image
// ref. If credentials are not returned, container implementations may still
// invoke locally available credential helpers (for example, via `docker pull`
// or `skopeo copy`)
func GetPullCredentials(env environment.Env, props *platform.Properties) *PullCredentials {
	imageRef := props.ContainerImage

	// TODO(bduffany): Accept credentials from platform props as well.

	regCfgs := env.GetConfigurator().GetExecutorConfig().ContainerRegistries
	if len(regCfgs) == 0 {
		return nil
	}

	ref, err := reference.ParseNormalizedNamed(imageRef)
	if err != nil {
		log.Debugf("Failed to parse image ref %q: %s", imageRef, err)
		return nil
	}
	refHostname := reference.Domain(ref)
	for _, cfg := range regCfgs {
		for _, cfgHostname := range cfg.Hostnames {
			if refHostname == cfgHostname {
				return &PullCredentials{
					Username: cfg.Username,
					Password: cfg.Password,
				}
			}
		}
	}

	return nil
}

// TracedCommandContainer is a wrapper that creates tracing spans for all CommandContainer methods.
type TracedCommandContainer struct {
	delegate CommandContainer
	implAttr attribute.KeyValue
}

func (t *TracedCommandContainer) Run(ctx context.Context, command *repb.Command, workingDir string, creds *PullCredentials) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.Run(ctx, command, workingDir, creds)
}

func (t *TracedCommandContainer) PullImageIfNecessary(ctx context.Context, creds *PullCredentials) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.PullImageIfNecessary(ctx, creds)
}

func (t *TracedCommandContainer) Create(ctx context.Context, workingDir string) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.Create(ctx, workingDir)
}

func (t *TracedCommandContainer) Exec(ctx context.Context, command *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.Exec(ctx, command, stdin, stdout)
}

func (t *TracedCommandContainer) Unpause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.Unpause(ctx)
}

func (t *TracedCommandContainer) Pause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.Pause(ctx)
}

func (t *TracedCommandContainer) Remove(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.Remove(ctx)
}

func (t *TracedCommandContainer) Stats(ctx context.Context) (*Stats, error) {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.Stats(ctx)
}

func NewTracedCommandContainer(delegate CommandContainer) *TracedCommandContainer {
	return &TracedCommandContainer{
		delegate: delegate,
		implAttr: attribute.String("container.impl", fmt.Sprintf("%T", delegate)),
	}
}
