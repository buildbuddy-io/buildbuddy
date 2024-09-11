package container

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/unixcred"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

	// How often to poll container stats.
	statsPollInterval = 50 * time.Millisecond
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

	// A map from isolation type + image name to a mutex that serializes
	// existence checks and image pulls.
	pullOperations sync.Map
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
	New(context.Context, *Init) (CommandContainer, error)
}

// Init contains initialization parameters used well calling Provider.New.
//
// It's not strictly necessary to look at all of these params in order to create
// a working container implementation, but they might be useful.
type Init struct {
	// WorkDir is the working directory for the initially assigned task.
	WorkDir string

	// Task is the execution initially assigned to the container.
	Task *repb.ScheduledTask

	// Props contains parsed platform properties for the task, with
	// executor-level overrides and remote header overrides applied.
	Props *platform.Properties

	// Publisher can be used to send fine-grained execution progress updates.
	Publisher *operation.Publisher
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

// UsageStats holds usage stats for a container.
// It is useful for keeping track of usage relative to when the container
// last executed a task.
//
// TODO: see whether its feasible to execute each task in its own cgroup
// so that we can avoid this bookkeeping and get stats without polling.
type UsageStats struct {
	// last is the last stats update we observed.
	last *repb.UsageStats
	// taskStats is the usage stats relative to when Reset() was last called
	// (i.e. for the current task).
	taskStats *repb.UsageStats
	// peakMemoryUsageBytes is the max memory usage from the last task
	// execution. This is reset between tasks so that we can determine a task's
	// peak memory usage when using a recycled runner.
	peakMemoryUsageBytes int64
	// baselineCPUNanos is the CPU usage from when a task last finished
	// executing. This is needed so that we can determine a task's CPU usage
	// when using a recycled runner.
	baselineCPUNanos int64
	// Baseline PSI metrics from when a task last finished executing.
	// This is needed so that we can determine PSI stall totals when using
	// a recycled runner.
	baselineCPUPressure, baselineMemoryPressure, baselineIOPressure *repb.PSI
}

// Reset resets resource usage counters in preparation for a new task, so that
// the new task's resource usage can be accounted for. It should be called
// at the beginning of Exec() in the container lifecycle.
func (s *UsageStats) Reset() {
	if s.last == nil {
		// No observations yet; nothing to do.
		return
	}
	s.last.MemoryBytes = 0
	s.baselineCPUNanos = s.last.GetCpuNanos()
	s.baselineCPUPressure = s.last.GetCpuPressure()
	s.baselineMemoryPressure = s.last.GetMemoryPressure()
	s.baselineIOPressure = s.last.GetIoPressure()
	s.peakMemoryUsageBytes = 0
}

// TODO: remove after debugging stats issue
func (s *UsageStats) Clone() *UsageStats {
	clone := *s
	if s.last != nil {
		clone.last = s.last.CloneVT()
	}
	// Baseline PSI protos are readonly; no need to clone.
	return &clone
}

// TaskStats returns the usage stats for an executed task.
func (s *UsageStats) TaskStats() *repb.UsageStats {
	if s.last == nil {
		return &repb.UsageStats{}
	}

	taskStats := s.last.CloneVT()

	taskStats.CpuNanos -= s.baselineCPUNanos
	taskStats.PeakMemoryBytes = s.peakMemoryUsageBytes

	if taskStats.GetCpuPressure().GetSome().GetTotal() > 0 {
		taskStats.CpuPressure.Some.Total -= s.baselineCPUPressure.GetSome().GetTotal()
	}
	if taskStats.GetCpuPressure().GetFull().GetTotal() > 0 {
		taskStats.CpuPressure.Full.Total -= s.baselineCPUPressure.GetFull().GetTotal()
	}
	if taskStats.GetMemoryPressure().GetSome().GetTotal() > 0 {
		taskStats.MemoryPressure.Some.Total -= s.baselineMemoryPressure.GetSome().GetTotal()
	}
	if taskStats.GetMemoryPressure().GetFull().GetTotal() > 0 {
		taskStats.MemoryPressure.Full.Total -= s.baselineMemoryPressure.GetFull().GetTotal()
	}
	if taskStats.GetIoPressure().GetSome().GetTotal() > 0 {
		taskStats.IoPressure.Some.Total -= s.baselineIOPressure.GetSome().GetTotal()
	}
	if taskStats.GetIoPressure().GetFull().GetTotal() > 0 {
		taskStats.IoPressure.Full.Total -= s.baselineIOPressure.GetFull().GetTotal()
	}

	return taskStats
}

// Update updates the usage for the current task, given a reading from the
// lifetime stats (e.g. cgroup created when the task container was initially
// created).
func (s *UsageStats) Update(lifetimeStats *repb.UsageStats) {
	s.last = lifetimeStats.CloneVT()
	if lifetimeStats.GetMemoryBytes() > s.peakMemoryUsageBytes {
		s.peakMemoryUsageBytes = lifetimeStats.GetMemoryBytes()
	}
}

// TrackStats starts a goroutine to monitor the container's resource usage. It
// polls c.Stats() to get the cumulative usage since the start of the current
// task.
//
// The returned func stops tracking resource usage. It must be called, or else a
// goroutine leak may occur. Monitoring can safely be stopped more than once.
//
// The returned channel should be received from at most once, *after* calling
// the returned stop function. The received value can be nil if stats were not
// successfully sampled at least once.
func TrackStats(ctx context.Context, c CommandContainer) (stop func(), res <-chan *repb.UsageStats) {
	ctx, cancel := context.WithCancel(ctx)
	result := make(chan *repb.UsageStats, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer Metrics.Unregister(c)
		var last *repb.UsageStats
		var lastErr error

		start := time.Now()
		defer func() {
			// Only log an error if the task ran long enough that we could
			// reasonably expect to sample stats at least once while it was
			// executing. Note that we can't sample stats until podman creates
			// the container, which can take a few hundred ms or possibly longer
			// if the executor is heavily loaded.
			dur := time.Since(start)
			if last == nil && dur > 1*time.Second && lastErr != nil {
				log.CtxWarningf(ctx, "Failed to read container stats: %s", lastErr)
			}
		}()

		t := time.NewTicker(statsPollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				result <- last
				return
			case <-t.C:
				stats, err := c.Stats(ctx)
				if err != nil {
					lastErr = err
					continue
				}
				Metrics.Observe(c, stats)
				last = stats
			}
		}
	}()
	stop = func() {
		cancel()
		<-done
	}
	return stop, result
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
	// Returns the isolation type of this container.
	IsolationType() string

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
	Exec(ctx context.Context, command *repb.Command, stdio *interfaces.Stdio) *interfaces.CommandResult

	// Signal sends the given signal to all containerized processes.
	//
	// For now, only processes spawned via Run() are required to be signaled,
	// since it is more difficult to handle the Create()/Exec() case.
	//
	// If no processes are currently running, an error may be returned.
	Signal(ctx context.Context, sig syscall.Signal) error

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
}

// VM is an interface implemented by containers backed by VMs (i.e. just
// Firecracker). This just exists to avoid depending on the Firecracker package
// on non-linux/amd64 platforms.
type VM interface {
	// SetTaskFileSystemLayout sets the VFS layout for use inside the guest.
	SetTaskFileSystemLayout(layout *FileSystemLayout)

	// SnapshotDebugString returns a string representing the cache key used for
	// VM snapshots, if applicable.
	SnapshotDebugString(ctx context.Context) string

	// VMConfig returns the VM's initialization config.
	VMConfig() *fcpb.VMConfiguration
}

// PullImageIfNecessary pulls the image configured for the container if it
// is not cached locally.
func PullImageIfNecessary(ctx context.Context, env environment.Env, ctr CommandContainer, creds oci.Credentials, imageRef string) error {
	if *debugUseLocalImagesOnly || imageRef == "" {
		return nil
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.End()
	cacheAuth := env.GetImageCacheAuthenticator()
	if cacheAuth == nil || env.GetAuthenticator() == nil {
		// If we don't have an authenticator available, fall back to
		// authenticating the creds with the image registry on every request.
		slowPullWarnOnce.Do(func() {
			log.CtxWarningf(ctx, "Authentication is not properly configured; this will result in slower image pulls.")
		})
		return ctr.PullImage(ctx, creds)
	}

	// TODO(iain): the auth/existence/pull synchronization is getting unruly.
	// TODO: this (and the similar map in podman.go) can theoretically leak
	// memory, though in practice it shouldn't be a problem.
	uncastmu, _ := pullOperations.LoadOrStore(hash.Strings(ctr.IsolationType(), imageRef), &sync.Mutex{})
	mu, ok := uncastmu.(*sync.Mutex)
	if !ok {
		alert.UnexpectedEvent("loaded mutex from sync.map that isn't a mutex!")
		return status.InternalError("PullImageIfNecessary failed: cannot obtain mutex")
	}
	mu.Lock()
	defer mu.Unlock()
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
	u, err := env.GetAuthenticator().AuthenticatedUser(ctx)
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

// Parses a "USER[:GROUP]" string, which has the same semantics as docker/
// podman's '--user' flag.
func ParseUserGroup(input string) (user *unixcred.NameOrID, group *unixcred.NameOrID, err error) {
	if input == "" {
		return nil, nil, fmt.Errorf("input string is empty")
	}
	parts := strings.Split(input, ":")
	if len(parts) > 2 {
		return nil, nil, fmt.Errorf("invalid format, too many colons in input %q", input)
	}
	u := parts[0]
	var g string
	if len(parts) == 2 {
		g = parts[1]
	}
	if u == "" {
		return nil, nil, fmt.Errorf("user part is required in input %q", input)
	}
	if uid, err := strconv.ParseUint(u, 10, 32); err == nil {
		user = &unixcred.NameOrID{ID: uint32(uid)}
	} else {
		user = &unixcred.NameOrID{Name: u}
	}
	if g != "" {
		if gid, err := strconv.ParseUint(g, 10, 32); err == nil {
			group = &unixcred.NameOrID{ID: uint32(gid)}
		} else {
			group = &unixcred.NameOrID{Name: g}
		}
	}
	return user, group, nil
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

func (t *TracedCommandContainer) IsolationType() string {
	return t.Delegate.IsolationType()
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

func (t *TracedCommandContainer) Exec(ctx context.Context, command *repb.Command, opts *interfaces.Stdio) *interfaces.CommandResult {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return &interfaces.CommandResult{ExitCode: noExitCode, Error: ErrRemoved}
	}

	return t.Delegate.Exec(ctx, command, opts)
}

func (t *TracedCommandContainer) Signal(ctx context.Context, sig syscall.Signal) error {
	ctx, span := tracing.StartSpan(ctx, trace.WithAttributes(t.implAttr))
	defer span.End()

	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.removed {
		return ErrRemoved
	}

	return t.Delegate.Signal(ctx, sig)
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

func NewTracedCommandContainer(delegate CommandContainer) *TracedCommandContainer {
	return &TracedCommandContainer{
		Delegate: delegate,
		implAttr: attribute.String("container.impl", fmt.Sprintf("%T", delegate)),
	}
}
