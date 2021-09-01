package container

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/docker/distribution/reference"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	// Default TTL for tokens granting access to locally cached images, before
	// re-authentication with the remote registry is required.
	defaultImageCacheTokenTTL = 15 * time.Minute
)

// Stats holds represents a container's held resources.
type Stats struct {
	MemoryUsageBytes int64

	// TODO: add CPU usage once we have a reliable way to measure it.
	// CPU only applies to bare execution, since Docker and Containerd containers
	// are frozen when not in use, reducing their CPU usage to 0.
}

type FileSystemLayout struct {
	RemoteInstanceName string
	Inputs             *repb.Tree
	OutputDirs         []string
	OutputFiles        []string
}

// CommandContainer provides an execution environment for commands.
type CommandContainer interface {
	// Run the given command within the container and remove the container after
	// it is done executing.
	//
	// It is approximately the same as calling PullImageIfNecessary, Create,
	// Exec, then Remove.
	Run(ctx context.Context, command *repb.Command, workingDir string, creds PullCredentials) *interfaces.CommandResult

	// IsImageCached returns whether the configured image is cached locally.
	IsImageCached(ctx context.Context) (bool, error)

	// PullImage pulls the container image from the remote. It always
	// re-authenticates the request, but may serve the image from a local cache
	// if needed.
	PullImage(ctx context.Context, creds PullCredentials) error

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

// PullImageIfNecessary pulls the image configured for the container if it
// is not cached locally.
func PullImageIfNecessary(ctx context.Context, env environment.Env, cacheAuth *ImageCacheAuthenticator, ctr CommandContainer, creds PullCredentials, imageRef string) error {
	isCached, err := ctr.IsImageCached(ctx)
	if err != nil {
		return err
	}
	cacheToken, err := NewImageCacheToken(ctx, env, creds, imageRef)
	if err != nil {
		return status.WrapError(err, "create image cache token")
	}
	// If the image is cached and these credentials have been used recently
	// by this group to pull the image, no need to re-auth.
	if isCached && cacheAuth.IsAuthorized(cacheToken) {
		return nil
	}
	if err := ctr.PullImage(ctx, creds); err != nil {
		return err
	}
	// Pull was successful, which means auth was successful. Refresh the token so
	// we don't have to keep re-authenticating on every action until the token
	// expires.
	cacheAuth.Refresh(cacheToken)
	return nil
}

type PullCredentials struct {
	Username string
	Password string
}

func (p PullCredentials) IsEmpty() bool {
	return p == PullCredentials{}
}

func (p PullCredentials) String() string {
	if p.IsEmpty() {
		return ""
	}
	return p.Username + ":" + p.Password
}

// ImageCacheToken is a claim to be able to access a locally cached image.
type ImageCacheToken struct {
	GroupID      string
	ImageRef     string
	UserHash     string
	PasswordHash string
}

// NewImageCacheToken returns the token representing the authenticated group ID,
// pull credentials, and image ref. For the same sets of those values, the
// same token is always returned.
func NewImageCacheToken(ctx context.Context, env environment.Env, creds PullCredentials, imageRef string) (ImageCacheToken, error) {
	groupID := ""
	u, err := perms.AuthenticatedUser(ctx, env)
	if err != nil {
		// PermissionDenied, Unauthenticated, Unimplemented all imply that this is an
		// anonymous execution, so ignore those.
		if !status.IsUnauthenticatedError(err) && !status.IsPermissionDeniedError(err) && !status.IsUnimplementedError(err) {
			return ImageCacheToken{}, err
		}
	} else {
		groupID = u.GetGroupID()
	}
	return ImageCacheToken{
		GroupID:      groupID,
		ImageRef:     imageRef,
		UserHash:     hashString(creds.Username),
		PasswordHash: hashString(creds.Password),
	}, nil
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

// GetPullCredentials returns the image pull credentials for the given image
// ref. If credentials are not returned, container implementations may still
// invoke locally available credential helpers (for example, via `docker pull`
// or `skopeo copy`)
func GetPullCredentials(env environment.Env, props *platform.Properties) PullCredentials {
	imageRef := props.ContainerImage
	if imageRef == "" {
		return PullCredentials{}
	}

	if props.ContainerRegistryUsername != "" || props.ContainerRegistryPassword != "" {
		return PullCredentials{
			Username: props.ContainerRegistryUsername,
			Password: props.ContainerRegistryPassword,
		}
	}

	regCfgs := env.GetConfigurator().GetExecutorConfig().ContainerRegistries
	if len(regCfgs) == 0 {
		return PullCredentials{}
	}

	ref, err := reference.ParseNormalizedNamed(imageRef)
	if err != nil {
		log.Debugf("Failed to parse image ref %q: %s", imageRef, err)
		return PullCredentials{}
	}
	refHostname := reference.Domain(ref)
	for _, cfg := range regCfgs {
		for _, cfgHostname := range cfg.Hostnames {
			if refHostname == cfgHostname {
				return PullCredentials{
					Username: cfg.Username,
					Password: cfg.Password,
				}
			}
		}
	}

	return PullCredentials{}
}

// TracedCommandContainer is a wrapper that creates tracing spans for all CommandContainer methods.
type TracedCommandContainer struct {
	Delegate CommandContainer
	implAttr attribute.KeyValue
}

func (t *TracedCommandContainer) Run(ctx context.Context, command *repb.Command, workingDir string, creds PullCredentials) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.Run(ctx, command, workingDir, creds)
}

func (t *TracedCommandContainer) IsImageCached(ctx context.Context) (bool, error) {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.IsImageCached(ctx)
}

func (t *TracedCommandContainer) PullImage(ctx context.Context, creds PullCredentials) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.PullImage(ctx, creds)
}

func (t *TracedCommandContainer) Create(ctx context.Context, workingDir string) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.Create(ctx, workingDir)
}

func (t *TracedCommandContainer) Exec(ctx context.Context, command *repb.Command, stdin io.Reader, stdout io.Writer) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.Exec(ctx, command, stdin, stdout)
}

func (t *TracedCommandContainer) Unpause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.Unpause(ctx)
}

func (t *TracedCommandContainer) Pause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.Pause(ctx)
}

func (t *TracedCommandContainer) Remove(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.Remove(ctx)
}

func (t *TracedCommandContainer) Stats(ctx context.Context) (*Stats, error) {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.Stats(ctx)
}

func NewTracedCommandContainer(delegate CommandContainer) *TracedCommandContainer {
	return &TracedCommandContainer{
		Delegate: delegate,
		implAttr: attribute.String("container.impl", fmt.Sprintf("%T", delegate)),
	}
}
