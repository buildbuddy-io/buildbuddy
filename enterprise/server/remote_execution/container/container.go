package container

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Default TTL for tokens granting access to locally cached images, before
	// re-authentication with the remote registry is required.
	defaultImageCacheTokenTTL = 15 * time.Minute
)

var (
	emptyCredentials = PullCredentials{}
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

type PullCredentials struct {
	Username string
	Password string
}

// ImageCacheToken is a claim to be able to access a locally cached image.
type ImageCacheToken string

// NewImageCacheToken returns the token representing the authenticated group ID,
// pull credentials, and image ref. For the same sets of those values, the
// same token is always returned.
func NewImageCacheToken(ctx context.Context, env environment.Env, creds *PullCredentials, imageRef string) (ImageCacheToken, error) {
	u, err := perms.AuthenticatedUser(ctx, env)
	if err != nil && !status.IsUnauthenticatedError(err) && !status.IsPermissionDeniedError(err) {
		return "", err
	}
	groupID := ""
	if u != nil {
		groupID = u.GetGroupID()
	}
	if creds == nil {
		creds = &emptyCredentials
	}
	return ImageCacheToken(
		hashString(
			hashString(groupID) + hashString(creds.Username) + hashString(creds.Password) + hashString(imageRef),
		),
	), nil
}

func hashString(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

// ImageCacheAuthenticator grants access to short-lived tokens for accessing
// locally cached images without needing to re-authenticate with the remote
// registry (which can be slow).
type ImageCacheAuthenticator struct {
	opts ImageCacheAuthenticatorOpts

	mu               sync.Mutex // protects(tokenExpireTimes)
	tokenExpireTimes map[ImageCacheToken]time.Time
}

type ImageCacheAuthenticatorOpts struct {
	// TokenTTL controls how long tokens can be used to access locally cached
	// images until re-authentication with the remote registry is required.
	TokenTTL time.Duration
}

func NewImageCacheAuthenticator(opts ImageCacheAuthenticatorOpts) *ImageCacheAuthenticator {
	if opts.TokenTTL == 0 {
		opts.TokenTTL = defaultImageCacheTokenTTL
	}
	return &ImageCacheAuthenticator{
		opts:             opts,
		tokenExpireTimes: map[ImageCacheToken]time.Time{},
	}
}

func (a *ImageCacheAuthenticator) IsAuthorized(token ImageCacheToken) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.purgeExpiredTokens()
	_, ok := a.tokenExpireTimes[token]
	return ok
}

func (a *ImageCacheAuthenticator) Refresh(token ImageCacheToken) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokenExpireTimes[token] = time.Now().Add(a.opts.TokenTTL)
}

func (a *ImageCacheAuthenticator) purgeExpiredTokens() {
	for token, expireTime := range a.tokenExpireTimes {
		if time.Now().After(expireTime) {
			delete(a.tokenExpireTimes, token)
		}
	}
}

// TracedCommandContainer is a wrapper that creates tracing spans for all CommandContainer methods.
type TracedCommandContainer struct {
	delegate CommandContainer
	implAttr attribute.KeyValue
}

func (t *TracedCommandContainer) Run(ctx context.Context, command *repb.Command, workingDir string) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.Run(ctx, command, workingDir)
}

func (t *TracedCommandContainer) PullImageIfNecessary(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.delegate.PullImageIfNecessary(ctx)
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
