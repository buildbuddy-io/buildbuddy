package container

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
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

	// Time window over which to measure CPU usage when exporting the milliCPU
	// used metric.
	cpuUsageUpdateInterval = 1 * time.Second
)

var (
	// Metrics is a shared metrics object to handle proper prometheus metrics
	// accounting across container instances.
	Metrics = NewContainerMetrics()

	containerRegistries     = flagutil.New("executor.container_registries", []ContainerRegistry{}, "")
	debugUseLocalImagesOnly = flag.Bool("debug_use_local_images_only", false, "Do not pull OCI images and only used locally cached images. This can be set to test local image builds during development without needing to push to a container registry. Not intended for production use.")
)

type ContainerRegistry struct {
	Hostnames []string `yaml:"hostnames" json:"hostnames"`
	Username  string   `yaml:"username" json:"username"`
	Password  string   `yaml:"password" json:"password" config:"secret"`
}

type DockerDeviceMapping struct {
	PathOnHost        string `yaml:"path_on_host" usage:"path to device that should be mapped from the host."`
	PathInContainer   string `yaml:"path_in_container" usage:"path under which the device will be present in container."`
	CgroupPermissions string `yaml:"cgroup_permissions" usage:"cgroup permissions that should be assigned to device."`
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
	//
	// If stdin is non-nil, the contents of stdin reader will be piped to the
	// stdin of the executed process. If stdout is non-nil, the stdout of the
	// executed process will be written to the stdout writer rather than being
	// written to the command result's stdout field (same for stderr).
	Exec(ctx context.Context, command *repb.Command, stdio *Stdio) *interfaces.CommandResult
	// Unpause un-freezes a container so that it can be used to execute commands.
	Unpause(ctx context.Context) error
	// Pause freezes a container so that it no longer consumes CPU resources.
	Pause(ctx context.Context) error
	// Remove kills any processes currently running inside the container and
	// removes any resources associated with the container itself.
	Remove(ctx context.Context) error

	// Stats returns the current resource usage of this container.
	//
	// A `nil` value may be returned if the resource usage is unknown.
	//
	// Implementations may assume that this will only be called when the
	// container is paused, for the purposes of computing resources used for
	// pooled runners.
	Stats(ctx context.Context) (*repb.UsageStats, error)
}

// Stdio specifies standard input / output readers for a command.
type Stdio struct {
	// Stdin is an optional stdin source for the executed process.
	Stdin io.Reader
	// Stdout is an optional stdout sink for the executed process.
	Stdout io.Writer
	// Stderr is an optional stderr sink for the executed process.
	Stderr io.Writer
}

// PullImageIfNecessary pulls the image configured for the container if it
// is not cached locally.
func PullImageIfNecessary(ctx context.Context, env environment.Env, cacheAuth *ImageCacheAuthenticator, ctr CommandContainer, creds PullCredentials, imageRef string) error {
	if *debugUseLocalImagesOnly {
		return nil
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
	GroupID  string
	ImageRef string
}

// NewImageCacheToken returns the token representing the authenticated group ID,
// pull credentials, and image ref. For the same sets of those values, the
// same token is always returned.
func NewImageCacheToken(ctx context.Context, env environment.Env, creds PullCredentials, imageRef string) (ImageCacheToken, error) {
	groupID := ""
	u, err := perms.AuthenticatedUser(ctx, env)
	if err != nil {
		if !authutil.IsAnonymousUserError(err) {
			return ImageCacheToken{}, err
		}
	} else {
		groupID = u.GetGroupID()
	}
	return ImageCacheToken{
		GroupID:  groupID,
		ImageRef: imageRef,
	}, nil
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
func GetPullCredentials(env environment.Env, props *platform.Properties) (PullCredentials, error) {
	imageRef := props.ContainerImage
	if imageRef == "" {
		return PullCredentials{}, nil
	}

	username := props.ContainerRegistryUsername
	password := props.ContainerRegistryPassword
	if username != "" && password == "" {
		return PullCredentials{}, status.InvalidArgumentError(
			"Received container-registry-username with no container-registry-password")
	} else if username == "" && password != "" {
		return PullCredentials{}, status.InvalidArgumentError(
			"Received container-registry-password with no container-registry-username")
	} else if username != "" || password != "" {
		return PullCredentials{
			Username: username,
			Password: password,
		}, nil
	}

	if len(*containerRegistries) == 0 {
		return PullCredentials{}, nil
	}

	ref, err := reference.ParseNormalizedNamed(imageRef)
	if err != nil {
		log.Debugf("Failed to parse image ref %q: %s", imageRef, err)
		return PullCredentials{}, nil
	}
	refHostname := reference.Domain(ref)
	for _, cfg := range *containerRegistries {
		for _, cfgHostname := range cfg.Hostnames {
			if refHostname == cfgHostname {
				return PullCredentials{
					Username: cfg.Username,
					Password: cfg.Password,
				}, nil
			}
		}
	}

	return PullCredentials{}, nil
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

func (t *TracedCommandContainer) Exec(ctx context.Context, command *repb.Command, opts *Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()
	return t.Delegate.Exec(ctx, command, opts)
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

func (t *TracedCommandContainer) Stats(ctx context.Context) (*repb.UsageStats, error) {
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
