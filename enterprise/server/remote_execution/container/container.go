package container

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

const (
	// Default TTL for tokens granting access to locally cached images, before
	// re-authentication with the remote registry is required.
	defaultImageCacheTokenTTL = 15 * time.Minute

	// Time window over which to measure CPU usage when exporting the milliCPU
	// used metric.
	cpuUsageUpdateInterval = 1 * time.Second

	// Exit code used when returning an error instead of an actual exit code.
	// TODO: fix circular dependency with commandutil and reference that const
	// instead.
	noExitCode = -2
)

var (
	// Metrics is a shared metrics object to handle proper prometheus metrics
	// accounting across container instances.
	Metrics = NewContainerMetrics()

	// ErrRemoved is returned by TracedCommandContainer operations when an
	// operation fails due to the container already being removed.
	ErrRemoved = status.UnavailableError("container has been removed")

	debugUseLocalImagesOnly = flag.Bool("debug_use_local_images_only", false, "Do not pull OCI images and only used locally cached images. This can be set to test local image builds during development without needing to push to a container registry. Not intended for production use.")

	DebugEnableAnonymousRecycling = flag.Bool("debug_enable_anonymous_runner_recycling", false, "Whether to enable runner recycling for unauthenticated requests. For debugging purposes only - do not use in production.")

	slowPullWarnOnce sync.Once
)

type DockerDeviceMapping struct {
	PathOnHost        string `yaml:"path_on_host" usage:"path to device that should be mapped from the host."`
	PathInContainer   string `yaml:"path_in_container" usage:"path under which the device will be present in container."`
	CgroupPermissions string `yaml:"cgroup_permissions" usage:"cgroup permissions that should be assigned to device."`
}

// Provider constructs new CommandContainer instances
// for a specific isolation type.
//
// This approach is used instead of package-level
// `New` funcs because it allows the executor to avoid
// depending directly on platform-specific container
// implementations.
type Provider interface {
	New(context.Context, *platform.Properties, *repb.ScheduledTask, *rnpb.RunnerState, string) (CommandContainer, error)
}

// ContainerMetrics handles Prometheus metrics accounting for CommandContainer
// instances.
type ContainerMetrics struct {
	mu sync.Mutex
	// Latest stats observed, per-container.
	latest map[CommandContainer]*repb.UsageStats
	// CPU usage for the current usage interval, per-container. This is cleared
	// every time we update the CPU gauge.
	intervalCPUNanos int64
}

func NewContainerMetrics() *ContainerMetrics {
	return &ContainerMetrics{
		latest: make(map[CommandContainer]*repb.UsageStats),
	}
}

// Start kicks off a goroutine that periodically updates the CPU gauge.
func (m *ContainerMetrics) Start(ctx context.Context) {
	go func() {
		t := time.NewTicker(cpuUsageUpdateInterval)
		defer t.Stop()
		lastTick := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				tick := time.Now()
				m.updateCPUMetric(tick.Sub(lastTick))
				lastTick = tick
			}
		}
	}()
}

func (m *ContainerMetrics) updateCPUMetric(dt time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	milliCPU := (float64(m.intervalCPUNanos) / 1e6) / dt.Seconds()
	metrics.RemoteExecutionCPUUtilization.Set(milliCPU)
	m.intervalCPUNanos = 0
}

// Observe records the latest stats for the current container execution.
func (m *ContainerMetrics) Observe(c CommandContainer, s *repb.UsageStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s == nil {
		delete(m.latest, c)
	} else {
		// Before recording CPU usage, sum the previous CPU usage so we know how
		// much new usage has been incurred.
		var prevCPUNanos int64
		for _, stats := range m.latest {
			prevCPUNanos += stats.CpuNanos
		}
		m.latest[c] = s
		var cpuNanos int64
		for _, stats := range m.latest {
			cpuNanos += stats.CpuNanos
		}
		diffCPUNanos := cpuNanos - prevCPUNanos
		// Note: This > 0 check is here to avoid panicking in case there are
		// issues with process stats returning non-monotonically-increasing
		// values for CPU usage.
		if diffCPUNanos > 0 {
			metrics.RemoteExecutionUsedMilliCPU.Add(float64(diffCPUNanos) / 1e6)
			m.intervalCPUNanos += diffCPUNanos
		}
	}
	var totalMemBytes, totalPeakMemBytes int64
	for _, stats := range m.latest {
		totalMemBytes += stats.MemoryBytes
		totalPeakMemBytes += stats.PeakMemoryBytes
	}
	metrics.RemoteExecutionMemoryUsageBytes.Set(float64(totalMemBytes))
	metrics.RemoteExecutionPeakMemoryUsageBytes.Set(float64(totalPeakMemBytes))
}

// Unregister records that the given container has completed execution. It must
// be called for each container whose stats are observed via ObserveStats,
// otherwise a memory leak will occur.
func (m *ContainerMetrics) Unregister(c CommandContainer) {
	m.Observe(c, nil)
}

type FileSystemLayout struct {
	RemoteInstanceName string
	DigestFunction     repb.DigestFunction_Value
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
	Run(ctx context.Context, command *repb.Command, workingDir string, creds oci.Credentials) *interfaces.CommandResult

	// IsImageCached returns whether the configured image is cached locally.
	IsImageCached(ctx context.Context) (bool, error)

	// PullImage pulls the container image from the remote. It always
	// re-authenticates the request, but may serve the image from a local cache
	// if needed.
	PullImage(ctx context.Context, creds oci.Credentials) error

	// Create creates a new container and starts a top-level process inside it
	// (`sleep infinity`) so that it stays alive and running until explicitly
	// removed. Note, this works slightly differently than commands like
	// `docker create` or `ctr containers create` -- in addition to creating the
	// container, it also puts it in a "ready to execute" state by starting the
	// top level process.
	Create(ctx context.Context, workingDir string) error
	// Exec runs a command inside a container, with the same working dir set when
	// creating the container.
	//
	// If stdin is non-nil, the contents of stdin reader will be piped to the
	// stdin of the executed process. If stdout is non-nil, the stdout of the
	// executed process will be written to the stdout writer rather than being
	// written to the command result's stdout field (same for stderr).
	Exec(ctx context.Context, command *repb.Command, stdio *commandutil.Stdio) *interfaces.CommandResult
	// Unpause un-freezes a container so that it can be used to execute commands.
	Unpause(ctx context.Context) error
	// Pause freezes a container so that it no longer consumes CPU resources.
	Pause(ctx context.Context) error
	// Remove kills any processes currently running inside the container and
	// removes any resources associated with the container itself. It is safe to
	// call remove if Create has not been called. If Create has been called but
	// failed, then Remove should remove any created resources if applicable.
	Remove(ctx context.Context) error

	// Stats returns the current resource usage of this container.
	//
	// A `nil` value may be returned if the resource usage is unknown.
	//
	// Implementations may assume that this will only be called when the
	// container is paused, for the purposes of computing resources used for
	// pooled runners.
	Stats(ctx context.Context) (*repb.UsageStats, error)

	// State returns the ContainerState to be persisted. This should be a
	// relatively fast operation since it is called on executor shutdown.
	//
	// If a container implementation does not support saving and restoring state
	// across restarts, it can return UNIMPLEMENTED.
	State(ctx context.Context) (*rnpb.ContainerState, error)
}

// PullImageIfNecessary pulls the image configured for the container if it
// is not cached locally.
func PullImageIfNecessary(ctx context.Context, env environment.Env, ctr CommandContainer, creds oci.Credentials, imageRef string) error {
	if *debugUseLocalImagesOnly {
		return nil
	}
	cacheAuth := env.GetImageCacheAuthenticator()
	if cacheAuth == nil || env.GetAuthenticator() == nil {
		// If we don't have an authenticator available, fall back to
		// authenticating the creds with the image registry on every request.
		slowPullWarnOnce.Do(func() {
			log.CtxWarningf(ctx, "Authentication is not properly configured; this will result in slower image pulls.")
		})
		return ctr.PullImage(ctx, creds)
	}
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

// NewImageCacheToken returns the token representing the authenticated group ID,
// pull credentials, and image ref. For the same sets of those values, the
// same token is always returned.
func NewImageCacheToken(ctx context.Context, env environment.Env, creds oci.Credentials, imageRef string) (interfaces.ImageCacheToken, error) {
	groupID := ""
	u, err := perms.AuthenticatedUser(ctx, env)
	if err != nil {
		if !authutil.IsAnonymousUserError(err) {
			return interfaces.ImageCacheToken{}, err
		}
	} else {
		groupID = u.GetGroupID()
	}
	return interfaces.ImageCacheToken{
		GroupID:  groupID,
		ImageRef: imageRef,
	}, nil
}

// imageCacheAuthenticator grants access to short-lived tokens for accessing
// locally cached images without needing to re-authenticate with the remote
// registry (which can be slow).
type imageCacheAuthenticator struct {
	opts ImageCacheAuthenticatorOpts

	mu               sync.Mutex // protects(tokenExpireTimes)
	tokenExpireTimes map[interfaces.ImageCacheToken]time.Time
}

type ImageCacheAuthenticatorOpts struct {
	// TokenTTL controls how long tokens can be used to access locally cached
	// images until re-authentication with the remote registry is required.
	TokenTTL time.Duration
}

func NewImageCacheAuthenticator(opts ImageCacheAuthenticatorOpts) *imageCacheAuthenticator {
	if opts.TokenTTL == 0 {
		opts.TokenTTL = defaultImageCacheTokenTTL
	}
	return &imageCacheAuthenticator{
		opts:             opts,
		tokenExpireTimes: map[interfaces.ImageCacheToken]time.Time{},
	}
}

func (a *imageCacheAuthenticator) IsAuthorized(token interfaces.ImageCacheToken) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.purgeExpiredTokens()
	_, ok := a.tokenExpireTimes[token]
	return ok
}

func (a *imageCacheAuthenticator) Refresh(token interfaces.ImageCacheToken) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokenExpireTimes[token] = time.Now().Add(a.opts.TokenTTL)
}

func (a *imageCacheAuthenticator) purgeExpiredTokens() {
	for token, expireTime := range a.tokenExpireTimes {
		if time.Now().After(expireTime) {
			delete(a.tokenExpireTimes, token)
		}
	}
}

// TracedCommandContainer is a wrapper that creates tracing spans for all
// CommandContainer methods. It also provides some basic protection against race
// conditions, such as preventing Remove() from being called twice or while
// other operations are in progress.
type TracedCommandContainer struct {
	implAttr attribute.KeyValue

	// This mutex is used only to prevent Remove() from being called
	// concurrently with other operations. Exec() may be called concurrently
	// with Pause() and Unpause() because persistent workers are implemented
	// using a long-running Exec() operation.
	mu       sync.RWMutex
	removed  bool
	Delegate CommandContainer
}

func (t *TracedCommandContainer) Run(ctx context.Context, command *repb.Command, workingDir string, creds oci.Credentials) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return &interfaces.CommandResult{ExitCode: noExitCode, Error: ErrRemoved}
	}

	return t.Delegate.Run(ctx, command, workingDir, creds)
}

func (t *TracedCommandContainer) IsImageCached(ctx context.Context) (bool, error) {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return false, ErrRemoved
	}

	return t.Delegate.IsImageCached(ctx)
}

func (t *TracedCommandContainer) PullImage(ctx context.Context, creds oci.Credentials) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return ErrRemoved
	}

	return t.Delegate.PullImage(ctx, creds)
}

func (t *TracedCommandContainer) Create(ctx context.Context, workingDir string) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return ErrRemoved
	}

	return t.Delegate.Create(ctx, workingDir)
}

func (t *TracedCommandContainer) Exec(ctx context.Context, command *repb.Command, opts *commandutil.Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return &interfaces.CommandResult{ExitCode: noExitCode, Error: ErrRemoved}
	}

	return t.Delegate.Exec(ctx, command, opts)
}

func (t *TracedCommandContainer) Unpause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return ErrRemoved
	}

	return t.Delegate.Unpause(ctx)
}

func (t *TracedCommandContainer) Pause(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return ErrRemoved
	}

	return t.Delegate.Pause(ctx)
}

func (t *TracedCommandContainer) Remove(ctx context.Context) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	// Get an *exclusive* lock here to ensure any other concurrent operations
	// are completed before we remove.
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.removed {
		return ErrRemoved
	}
	t.removed = true

	return t.Delegate.Remove(ctx)
}

func (t *TracedCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return nil, ErrRemoved
	}

	return t.Delegate.Stats(ctx)
}

func (t *TracedCommandContainer) State(ctx context.Context) (*rnpb.ContainerState, error) {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return nil, ErrRemoved
	}

	return t.Delegate.State(ctx)
}

func NewTracedCommandContainer(delegate CommandContainer) *TracedCommandContainer {
	return &TracedCommandContainer{
		Delegate: delegate,
		implAttr: attribute.String("container.impl", fmt.Sprintf("%T", delegate)),
	}
}
