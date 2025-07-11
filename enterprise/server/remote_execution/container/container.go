package container

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/block_io"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor_auth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/buildbuddy-io/buildbuddy/server/util/unixcred"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
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

	// How long to extend the context deadline to allow the final container
	// stats to be collected once execution has completed.
	statsFinalMeasurementDeadlineExtension = 1 * time.Second

	// Max uncompressed size in bytes to retain for timeseries data. After this
	// limit is reached, samples are dropped.
	timeseriesSizeLimitBytes = 2_000_000
)

var (
	// ErrRemoved is returned by TracedCommandContainer operations when an
	// operation fails due to the container already being removed.
	ErrRemoved = status.UnavailableError("container has been removed")

	recordUsageTimelines          = flag.Bool("executor.record_usage_timelines", false, "Capture resource usage timeseries data in UsageStats for each task.")
	imagePullTimeout              = flag.Duration("executor.image_pull_timeout", 5*time.Minute, "How long to wait for the container image to be pulled before returning an Unavailable (retryable) error for an action execution attempt. Applies to all isolation types (docker, firecracker, etc.)")
	cgroupStatsPollInterval       = flag.Duration("executor.cgroup_stats_poll_interval", 500*time.Millisecond, "How often to poll container stats.")
	debugUseLocalImagesOnly       = flag.Bool("debug_use_local_images_only", false, "Do not pull OCI images and only used locally cached images. This can be set to test local image builds during development without needing to push to a container registry. Not intended for production use.")
	debugEnableAnonymousRecycling = flag.Bool("debug_enable_anonymous_runner_recycling", false, "Whether to enable runner recycling for unauthenticated requests. For debugging purposes only - do not use in production.")

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

	// BlockDevice is the block device where the build root dir is located.
	BlockDevice *block_io.Device

	// CgroupParent is the path relative to the cgroupfs root under which the
	// container's cgroup should be created. For example, if the cgroupfs is
	// rooted at "/sys/fs/cgroup" and all container cgroups are placed under
	// "/sys/fs/cgroup/buildbuddy.executor.containers" then this will be
	// "buildbuddy.executor.containers". The container implementation is
	// responsible for creating the cgroup, applying the CgroupSettings
	// from `Task.SchedulingMetadata.CgroupSettings`.
	CgroupParent string

	// Publisher can be used to send fine-grained execution progress updates.
	Publisher *operation.Publisher
}

// UsageStats holds usage stats for a container.
// It is useful for keeping track of usage relative to when the container
// last executed a task.
type UsageStats struct {
	Clock clockwork.Clock

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
	// Baseline IO stats from when a task last finished executing.
	// This is needed so that we can determine total IO stats when using
	// a recycled runner.
	baselineIOStats *repb.CgroupIOStats

	timeline      *repb.UsageTimeline
	timelineState timelineState
}

// Delta-encoding state for timelines. We store the delta-encoding directly so
// that TaskStats() can just return a view of the data rather than requiring a
// full re-encoding.
type timelineState struct {
	lastTimestampUnixMillis int64
	lastCPUMillis           int64
	lastMemoryKB            int64
	lastDiskRbytes          int64
	lastWbytes              int64
	lastDiskRios            int64
	lastDiskWios            int64
	// When adding new fields here, also update:
	// - The size calculation in updateTimeline()
	// - The test
	// - The trace format adapter logic in execution_service.go
	// - TIME_SERIES_EVENT_NAMES_AND_ARG_KEYS in trace_events.ts
}

func (s *UsageStats) clock() clockwork.Clock {
	if s.Clock == nil {
		s.Clock = clockwork.NewRealClock()
	}
	return s.Clock
}

// Reset resets resource usage counters in preparation for a new task, so that
// the new task's resource usage can be accounted for.
// TODO: make this private - it should only be used by TrackExecution.
func (s *UsageStats) Reset() {
	if s.last != nil {
		s.last.MemoryBytes = 0
	}
	s.baselineCPUNanos = s.last.GetCpuNanos()
	s.baselineCPUPressure = s.last.GetCpuPressure()
	s.baselineMemoryPressure = s.last.GetMemoryPressure()
	s.baselineIOPressure = s.last.GetIoPressure()
	s.baselineIOStats = s.last.GetCgroupIoStats()
	s.peakMemoryUsageBytes = 0

	now := s.clock().Now()
	if *recordUsageTimelines {
		s.timeline = &repb.UsageTimeline{StartTime: tspb.New(now)}
		s.timelineState = timelineState{}
		s.updateTimeline(now)
	}
}

// TaskStats returns the usage stats for an executed task.
func (s *UsageStats) TaskStats() *repb.UsageStats {
	if s.last == nil {
		return &repb.UsageStats{}
	}

	taskStats := s.last.CloneVT()

	taskStats.CpuNanos -= s.baselineCPUNanos
	taskStats.PeakMemoryBytes = s.peakMemoryUsageBytes

	// Update all IO stats to be relative to the baseline
	ioStats := taskStats.CgroupIoStats
	if ioStats != nil {
		ioStats.Rbytes -= s.baselineIOStats.GetRbytes()
		ioStats.Wbytes -= s.baselineIOStats.GetWbytes()
		ioStats.Rios -= s.baselineIOStats.GetRios()
		ioStats.Wios -= s.baselineIOStats.GetWios()
		ioStats.Dbytes -= s.baselineIOStats.GetDbytes()
		ioStats.Dios -= s.baselineIOStats.GetDios()
	}

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

	// Note: we don't clone the timeline because it's expensive.
	taskStats.Timeline = s.timeline

	return taskStats
}

func (s *UsageStats) updateTimeline(now time.Time) {
	st := s.timeline
	totalLength := len(st.GetTimestamps()) +
		len(st.GetCpuSamples()) +
		len(st.GetMemoryKbSamples()) +
		len(st.GetWbytesTotalSamples()) +
		len(st.GetRbytesTotalSamples()) +
		len(st.GetWiosTotalSamples()) +
		len(st.GetRiosTotalSamples())
	if 8*totalLength > timeseriesSizeLimitBytes {
		return
	}

	// Update timestamps
	ts := now.UnixMilli()
	tsDelta := ts - s.timelineState.lastTimestampUnixMillis
	s.timeline.Timestamps = append(s.timeline.Timestamps, tsDelta)
	s.timelineState.lastTimestampUnixMillis = ts

	// Update CPU samples with cumulative CPU milliseconds used.
	cpu := (s.last.GetCpuNanos() - s.baselineCPUNanos) / 1e6
	cpuDelta := cpu - s.timelineState.lastCPUMillis
	s.timeline.CpuSamples = append(s.timeline.CpuSamples, cpuDelta)
	s.timelineState.lastCPUMillis = cpu

	// Update memory samples with current memory in KB (1000 bytes).
	mem := s.last.GetMemoryBytes() / 1e3
	memDelta := mem - s.timelineState.lastMemoryKB
	s.timeline.MemoryKbSamples = append(s.timeline.MemoryKbSamples, memDelta)
	s.timelineState.lastMemoryKB = mem

	// Update disk rbytes samples with cumulative bytes read.
	diskRbytes := s.last.GetCgroupIoStats().GetRbytes()
	diskRbytesDelta := diskRbytes - s.timelineState.lastDiskRbytes
	s.timeline.RbytesTotalSamples = append(s.timeline.RbytesTotalSamples, diskRbytesDelta)
	s.timelineState.lastDiskRbytes = diskRbytes

	// Update disk wbytes samples with cumulative bytes written.
	diskWbytes := s.last.GetCgroupIoStats().GetWbytes()
	diskWbytesDelta := diskWbytes - s.timelineState.lastWbytes
	s.timeline.WbytesTotalSamples = append(s.timeline.WbytesTotalSamples, diskWbytesDelta)
	s.timelineState.lastWbytes = diskWbytes

	// Update disk rios samples with cumulative read operations.
	diskRios := s.last.GetCgroupIoStats().GetRios()
	diskRiosDelta := diskRios - s.timelineState.lastDiskRios
	s.timeline.RiosTotalSamples = append(s.timeline.RiosTotalSamples, diskRiosDelta)
	s.timelineState.lastDiskRios = diskRios

	// Update disk wios samples with cumulative write operations.
	diskWios := s.last.GetCgroupIoStats().GetWios()
	diskWiosDelta := diskWios - s.timelineState.lastDiskWios
	s.timeline.WiosTotalSamples = append(s.timeline.WiosTotalSamples, diskWiosDelta)
	s.timelineState.lastDiskWios = diskWios
}

// Update updates the usage for the current task, given a reading from the
// lifetime stats (e.g. cgroup created when the task container was initially
// created).
// TODO: make this private - it should only be used by TrackExecution.
func (s *UsageStats) Update(lifetimeStats *repb.UsageStats) {
	s.last = lifetimeStats.CloneVT()
	if lifetimeStats.GetMemoryBytes() > s.peakMemoryUsageBytes {
		s.peakMemoryUsageBytes = lifetimeStats.GetMemoryBytes()
	}
	if *recordUsageTimelines {
		s.updateTimeline(s.clock().Now())
	}
}

// TrackExecution starts a goroutine to monitor a container's resource usage
// during an execution, periodically calling Update. It polls the given stats
// function to get the cumulative usage for the lifetime of the container (not
// just the current task).
//
// The returned func stops tracking resource usage. It must be called, or else a
// goroutine leak may occur. Monitoring can safely be stopped more than once.
func (s *UsageStats) TrackExecution(ctx context.Context, lifetimeStatsFn func(ctx context.Context) (*repb.UsageStats, error)) (stop func()) {
	// Since we're starting a new execution, set the stats baseline to the last
	// observed value.
	s.Reset()

	originalCtx := ctx

	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var lastErr error

		start := time.Now()
		defer func() {
			// Only log an error if the task ran long enough that we could
			// reasonably expect to sample stats at least once while it was
			// executing.
			dur := time.Since(start)
			if dur > 1*time.Second && lastErr != nil && s.TaskStats() == nil {
				log.CtxWarningf(ctx, "Failed to read container stats: %s", lastErr)
			}
		}()

		t := time.NewTicker(*cgroupStatsPollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				// Do one more stats collection right at the end of the
				// execution. This serves two purposes: first, it makes sure we
				// count any CPU usage that we might've missed at the very end
				// of the execution. Second, it ensures we update memory usage
				// to reflect what the task looks like at the very end, after
				// the main task process has exited, which is important since we
				// don't recycle runners if their current memory usage exceeds a
				// certain threshold.
				ctx, cancel := background.ExtendContextForFinalization(originalCtx, statsFinalMeasurementDeadlineExtension)
				defer cancel()
				stats, err := lifetimeStatsFn(ctx)
				if err == nil {
					// TODO: an error will be returned for podman here because
					// we run containers with --rm, which deletes the container
					// cgroup. We should do the removal in Remove() instead, so
					// that we can reliably perform the final stats collection.
					s.Update(stats)
				}
				return
			case <-t.C:
				stats, err := lifetimeStatsFn(ctx)
				if err != nil {
					lastErr = err
					continue
				}
				s.Update(stats)
			}
		}
	}()
	stop = func() {
		cancel()
		<-done
	}
	return stop
}

type FileSystemLayout struct {
	RemoteInstanceName string
	DigestFunction     repb.DigestFunction_Value
	Inputs             *repb.Tree
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

	if *imagePullTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *imagePullTimeout)
		defer cancel()
	}

	if err := pullImageIfNecessary(ctx, env, ctr, creds, imageRef); err != nil {
		// make sure we always return Unavailable if the context deadline
		// was exceeded
		if err == context.DeadlineExceeded || ctx.Err() != nil {
			return status.UnavailableErrorf("%s", status.Message(err))
		}
		return err
	}
	return nil
}

func pullImageIfNecessary(ctx context.Context, env environment.Env, ctr CommandContainer, creds oci.Credentials, imageRef string) error {
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

func AnonymousRecyclingEnabled() bool {
	// If the executor is registered to the app without auth, then anonymous
	// recycling should be enabled. Otherwise, it's only enabled if the debug
	// flag is set.
	return executor_auth.APIKey() == "" || *debugEnableAnonymousRecycling
}
