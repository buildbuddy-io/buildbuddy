package tasksize

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize_model"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	useMeasuredSizes          = flag.Bool("remote_execution.use_measured_task_sizes", false, "Whether to use measured usage stats to determine task sizes.")
	modelEnabled              = flag.Bool("remote_execution.task_size_model.enabled", false, "Whether to enable model-based task size prediction.")
	psiCorrectionFactor       = flag.Float64("remote_execution.task_size_psi_correction", 1.0, "What percentage of full-stall time should be subtracted from the execution duration.")
	cpuQuotaLimit             = flag.Duration("remote_execution.cpu_quota_limit", 30*100*time.Millisecond /*30 cores*/, "Maximum CPU time allowed for each quota period.")
	cpuQuotaPeriod            = flag.Duration("remote_execution.cpu_quota_period", 100*time.Millisecond, "How often the CPU quota is refreshed.")
	memoryLimitBytes          = flag.Int64("remote_execution.memory_limit_bytes", 0, "Task cgroup memory limit in bytes.")
	memoryOOMGroup            = flag.Bool("remote_execution.memory_oom_group", true, "If there is an OOM within any process in a cgroup, fail the entire execution with an OOM error.")
	pidLimit                  = flag.Int64("remote_execution.pids_limit", 4096, "Maximum number of processes allowed per task at any time.")
	additionalPIDsLimitPerCPU = flag.Int64("remote_execution.additional_pids_limit_per_cpu", 4096, "Additional number of processes allowed per estimated CPU.")
	// TODO: enforce a lower CPU hard limit for tasks in general, instead of
	// just limiting the task size that gets stored in redis.
	milliCPULimit = flag.Int64("remote_execution.stored_task_size_millicpu_limit", 7500, "Limit placed on milliCPU calculated from task execution statistics.")
)

const (
	testSizeEnvVar = "TEST_SIZE"

	// Definitions for BCU ("BuildBuddy Compute Unit")

	ComputeUnitsToMilliCPU = 1000      // 1 BCU = 1000 milli-CPU
	ComputeUnitsToRAMBytes = 2.5 * 1e9 // 1 BCU = 2.5GB of memory

	// Default resource estimates

	DefaultMemEstimate      = int64(400 * 1e6) // 400 MB
	WorkflowMemEstimate     = int64(8 * 1e9)   // 8 GB
	DefaultCPUEstimate      = int64(600)
	DefaultFreeDiskEstimate = int64(100 * 1e6) // 100 MB

	// Minimum size values.
	// These are an extra safeguard against overscheduling, and help account
	// for task overhead that is not measured explicitly (e.g. task setup).

	MinimumMilliCPU    = int64(250)
	MinimumMemoryBytes = int64(6_000_000) // 6 MB

	// Additional resources needed depending on task characteristics

	// FirecrackerAdditionalMemEstimateBytes represents the overhead incurred by
	// the firecracker runtime. It was computed as the minimum memory needed to
	// execute a trivial task (i.e. pwd) in Firecracker, multiplied by ~1.5x so
	// that we have some wiggle room.
	FirecrackerAdditionalMemEstimateBytes = int64(150 * 1e6) // 150 MB

	// DockerInFirecrackerAdditionalMemEstimateBytes is an additional memory
	// estimate added for docker-in-firecracker actions. It was computed as the
	// minimum additional memory needed to run a mysql:8.0 container inside
	// a firecracker VM, multiplied by ~2X.
	DockerInFirecrackerAdditionalMemEstimateBytes = int64(800 * 1e6) // 800 MB

	// DockerInFirecrackerAdditionalDiskEstimateBytes is an additional memory
	// estimate added for docker-in-firecracker actions. It was computed as the
	// minimum additional disk needed to run a mysql:8.0 container inside
	// a firecracker VM, multiplied by ~3X.
	DockerInFirecrackerAdditionalDiskEstimateBytes = int64(12 * 1e9) // 12 GB

	// The maximum amount of disk a workflow may request.
	MaxEstimatedFreeDisk = int64(100 * 1e9) // 100GB

	// The maximum amount of disk a non-recyclable workflow may request.
	MaxEstimatedFreeDiskRecycleFalse = int64(200 * 1e9) // 200GB

	// The fraction of an executor's allocatable resources to make available for task sizing.
	MaxResourceCapacityRatio = 1

	// The expiration for task usage measurements stored in Redis.
	sizeMeasurementExpiration = 5 * 24 * time.Hour

	// Redis key prefix used for holding current task size estimates.
	redisKeyPrefix = "taskSize"
)

// The OCI spec only supports "shares" for specifying CPU weights, which are
// based on cgroup v1 CPU shares. These are transformed to cgroup2 weights
// internally by crun using a simple linear mapping. These are the min/max
// values for "shares". See
// https://github.com/containers/crun/blob/main/crun.1.md#cpu-controller
const (
	cpuSharesMin = 2
	cpuSharesMax = 262_144
)

// Min/max values for cgroup2 CPU weight.
const (
	cpuWeightMin = 1
	cpuWeightMax = 10_000
)

// Register registers the task sizer with the env.
func Register(env *real_environment.RealEnv) error {
	sizer, err := NewSizer(env)
	if err != nil {
		return err
	}
	env.SetTaskSizer(sizer)
	return nil
}

type taskSizer struct {
	env   environment.Env
	rdb   redis.UniversalClient
	model *tasksize_model.Model
}

func NewSizer(env environment.Env) (*taskSizer, error) {
	ts := &taskSizer{env: env}
	if *useMeasuredSizes {
		if env.GetRemoteExecutionRedisClient() == nil {
			return nil, status.FailedPreconditionError("missing Redis client configuration")
		}
		ts.rdb = env.GetRemoteExecutionRedisClient()
	}
	if *modelEnabled {
		m, err := tasksize_model.New(env)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("Failed to initialize task size model: %s", err)
		}
		ts.model = m
	}
	return ts, nil
}

func (s *taskSizer) Get(ctx context.Context, cmd *repb.Command, props *platform.Properties) *scpb.TaskSize {
	if !*useMeasuredSizes {
		return nil
	}
	// If a task size is explicitly requested, measured task size is not used.
	if props.EstimatedComputeUnits != 0 {
		return nil
	}
	// TODO(bduffany): Remove or hide behind a dev-only flag once measured task sizing
	// is battle-tested.
	if props.DisableMeasuredTaskSize {
		return nil
	}
	// Don't use measured task sizes for Firecracker tasks for now, since task
	// sizes are used as hard limits on allowed resources.
	if props.WorkloadIsolationType == string(platform.FirecrackerContainerType) {
		return nil
	}
	statusLabel := "hit"
	defer func() {
		groupID, _ := s.groupKey(ctx)
		metrics.RemoteExecutionTaskSizeReadRequests.With(prometheus.Labels{
			metrics.TaskSizeReadStatusLabel: statusLabel,
			metrics.IsolationTypeLabel:      props.WorkloadIsolationType,
			metrics.OS:                      props.OS,
			metrics.Arch:                    props.Arch,
			metrics.GroupID:                 groupID,
		}).Inc()
	}()
	recordedSize, err := s.lastRecordedSize(ctx, cmd)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to read task size from Redis; falling back to default size estimate: %s", err)
		statusLabel = "error"
		return nil
	}
	if recordedSize == nil {
		statusLabel = "miss"
		// TODO: return a value indicating "unsized" here, and instead let the
		// executor run this task once to estimate the size.
		return nil
	}
	return ApplyLimits(ctx, s.env.GetExperimentFlagProvider(), cmd, props, &scpb.TaskSize{
		EstimatedMemoryBytes: recordedSize.EstimatedMemoryBytes,
		EstimatedMilliCpu:    recordedSize.EstimatedMilliCpu,
	})
}

func (s *taskSizer) Predict(ctx context.Context, action *repb.Action, cmd *repb.Command, props *platform.Properties) *scpb.TaskSize {
	if s.model == nil {
		return nil
	}
	return ApplyLimits(ctx, s.env.GetExperimentFlagProvider(), cmd, props, s.model.Predict(ctx, action, cmd, props))
}

func (s *taskSizer) Update(ctx context.Context, cmd *repb.Command, props *platform.Properties, md *repb.ExecutedActionMetadata) error {
	if !*useMeasuredSizes {
		return nil
	}
	statusLabel := "ok"
	defer func() {
		groupID, _ := s.groupKey(ctx)
		metrics.RemoteExecutionTaskSizeWriteRequests.With(prometheus.Labels{
			metrics.TaskSizeWriteStatusLabel: statusLabel,
			metrics.IsolationTypeLabel:       props.WorkloadIsolationType,
			metrics.OS:                       props.OS,
			metrics.Arch:                     props.Arch,
			metrics.GroupID:                  groupID,
		}).Inc()
	}()
	// If we are missing CPU/memory stats, do nothing. This is expected in some
	// cases, for example if a task completed too quickly to get a sample of its
	// CPU/mem usage.
	stats := md.GetUsageStats()
	if stats.GetCpuNanos() == 0 || stats.GetPeakMemoryBytes() == 0 {
		statusLabel = "missing_stats"
		return nil
	}
	averageMilliCPU := computeAverageMilliCPU(ctx, md)
	if averageMilliCPU <= 0 {
		statusLabel = "missing_stats"
		return status.InvalidArgumentErrorf("execution duration is missing or invalid")
	}
	key, err := s.taskSizeKey(ctx, cmd)
	if err != nil {
		statusLabel = "error"
		return err
	}
	size := &scpb.TaskSize{
		EstimatedMilliCpu:    averageMilliCPU,
		EstimatedMemoryBytes: stats.GetPeakMemoryBytes(),
	}

	if useP90, _ := EvaluateP90CPUTrial(ctx, s.env.GetExperimentFlagProvider(), cmd); useP90 {
		p90MilliCPU := computeP90MilliCPU(stats)
		// Use p90 or avg; whichever is larger. This makes sure we fall back to
		// the avg if timelines aren't available, and safeguards against
		// possible corner cases where p90 might somehow be computed as even
		// lower than the avg.
		size.EstimatedMilliCpu = max(size.EstimatedMilliCpu, p90MilliCPU)
	}

	// Apply the configured max CPU limit for computed task sizes. Sizes larger
	// than this amount must be manually requested.
	size.EstimatedMilliCpu = min(size.EstimatedMilliCpu, *milliCPULimit)

	b, err := proto.Marshal(size)
	if err != nil {
		statusLabel = "error"
		return err
	}
	s.rdb.Set(ctx, key, string(b), sizeMeasurementExpiration)
	return err
}

// EvaluateP90CPUTrial returns whether the command is included in the treatment
// arm for the p90 CPU experiment. The second return value is the experiment arm
// identifier (experiment name + arm name) which will be non-empty if the
// command is included in either the control or treatment arm.
func EvaluateP90CPUTrial(ctx context.Context, efp interfaces.ExperimentFlagProvider, cmd *repb.Command) (enabled bool, exp string) {
	if efp == nil {
		return false, ""
	}

	cmdKey, err := commandKey(cmd)
	if err != nil {
		return false, ""
	}
	const flagName = "remote_execution.task_size_use_p90_cpu"
	enabled, details := efp.BooleanDetails(
		ctx, flagName, false,
		// Include the command key in the evaluation context since we'll use it
		// for the %-based diversion.
		experiments.WithContext("command_key", cmdKey),
	)
	if details.Variant() == "" || details.Variant() == "default" {
		// Not in any experiment arm (control or treatment).
		return enabled, ""
	}
	return enabled, flagName + ":" + details.Variant()
}

func computeAverageMilliCPU(ctx context.Context, md *repb.ExecutedActionMetadata) int64 {
	execDuration := md.GetExecutionCompletedTimestamp().AsTime().Sub(md.GetExecutionStartTimestamp().AsTime())
	// Subtract full-stall durations from execution duration, to avoid inflating
	// the denominator. Note that these durations shouldn't overlap in terms of
	// wall-time, since e.g. if a task is stalled on I/O, it shouldn't require
	// any CPU resources, and therefore can't also be stalled on CPU.
	var cpuFullStallDuration, memoryFullStallDuration, ioFullStallDuration time.Duration
	if cpuStalledUsec := md.GetUsageStats().GetCpuPressure().GetFull().GetTotal(); cpuStalledUsec > 0 {
		cpuFullStallDuration = time.Duration(cpuStalledUsec) * time.Microsecond
	}
	if memoryStalledUsec := md.GetUsageStats().GetMemoryPressure().GetFull().GetTotal(); memoryStalledUsec > 0 {
		memoryFullStallDuration = time.Duration(memoryStalledUsec) * time.Microsecond
	}
	if ioStalledUsec := md.GetUsageStats().GetIoPressure().GetFull().GetTotal(); ioStalledUsec > 0 {
		ioFullStallDuration = time.Duration(ioStalledUsec) * time.Microsecond
	}
	totalFullStallDuration := cpuFullStallDuration + memoryFullStallDuration + ioFullStallDuration
	// Apply a correction factor, since using 100% of the stall duration can
	// lead to exploding task sizes due to measurement inaccuracies in the case
	// where the full stall duration is very close to the exec duration.
	adjustedFullStallDuration := time.Duration(*psiCorrectionFactor * float64(totalFullStallDuration))

	activeDuration := execDuration - adjustedFullStallDuration
	if activeDuration <= 0 {
		return 0
	}
	// Compute milliCPU as CPU-milliseconds used per second of non-stalled
	// execution time.
	cpuMillisUsed := float64(md.GetUsageStats().GetCpuNanos()) / 1e6
	milliCPUFloat := cpuMillisUsed / activeDuration.Seconds()
	// Run through Ceil() to prevent storing 0 values in case the CPU usage
	// was greater than 0 but less than 1 CPU-millisecond.
	milliCPU := int64(math.Ceil(milliCPUFloat))

	// Log all task sizing information so that we have a way to assess potential
	// changes to the formula.
	log.CtxInfof(
		ctx,
		"Computed CPU usage: %d average milli-CPU from %.1f CPU-millis over %s exec duration minus full-stall durations cpu=%s, mem=%s, io=%s",
		milliCPU, cpuMillisUsed, execDuration, cpuFullStallDuration, memoryFullStallDuration, ioFullStallDuration,
	)

	return milliCPU
}

func computeP90MilliCPU(stats *repb.UsageStats) int64 {
	// Some clarification on how we compute p90 from the usage data in the
	// proto:
	//
	// - The 'timestamps' field is an array of delta-encoded timestamps
	//   (milliseconds since Unix epoch).
	// - The 'cpu_samples' field is an array of delta-encoded cumulative
	//   CPU-millis (since the runner was created).
	// - Both arrays represent *cumulative* metrics, so their delta-encoding
	//   (ignoring the first sample) contains the *incremental* metrics that
	//   we want to use in order to compute CPU utilization.
	//
	// Example:
	//
	// Original timestamps: [9000000, 9001000, 9002001] (units: ms since epoch)
	// Cumulative CPU:      [  77000,   79000,   79500] (units: cpu-millis since
	// container created)
	//
	// The delta-encodings (stored in the Timeline proto) will look like this:
	//         timestamps = [9000000, (+1000), (+1001)]
	//        cpu_samples = [  77000, (+2000),  (+500)]
	//
	// In this example, we compute the CPU utilization samples by looking
	// directly at the delta-encodings, ignoring the first sample:
	//
	// CPU utilization samples: [(+2000)/(+1000), (+500)/(+1001)]
	//                        = [              2,         0.4995]
	//
	// Then we return the p90 value from this final array (the value is in
	// cpu-ms/ms, so we multiply by 1000 to get cpu-ms/s i.e. milliCPU)

	timestampsDeltaEncoding := stats.GetTimeline().GetTimestamps()
	cumulativeCPUDeltaEncoding := stats.GetTimeline().GetCpuSamples()
	// These should be the same, but sanity check.
	if len(timestampsDeltaEncoding) != len(cumulativeCPUDeltaEncoding) {
		return 0
	}
	// Need multiple samples in order to compute p90.
	if len(timestampsDeltaEncoding) <= 1 {
		return 0
	}
	// Samples are delta-encoded, so each sample after the first index
	// tells us the incremental duration/CPU usage respectively.
	durations := timestampsDeltaEncoding[1:]
	cpuMillisIncrements := cumulativeCPUDeltaEncoding[1:]
	// Make a list with the CPU utilization samples over time, in units of
	// cpu-millis/millis (CPU cores). For simplicity, don't bother with
	// smoothing / averaging - just get p90 from the delta samples that we have,
	// normalized by duration. The data may be a bit noisy and may occasionally
	// have some extremes, but the p90 metric should mostly be robust to these
	// outliers.
	utilizationSamples := make([]float64, len(durations))
	for i := range durations {
		if durations[i] <= 0 {
			// Should never happen, but safeguard against dividing by zero.
			return 0
		}
		utilizationSamples[i] = float64(cpuMillisIncrements[i]) / float64(durations[i])
	}
	// Get p90 delta.
	sort.Float64s(utilizationSamples)
	p90 := utilizationSamples[int(float64(len(utilizationSamples))*0.9)]
	// The units are cpu-ms/ms (CPU cores), but we want cpu-ms/s (milli-CPU
	// cores). Multiply by 1000 ms/s to get the correct units.
	return int64(p90 * 1000)
}

func (s *taskSizer) lastRecordedSize(ctx context.Context, cmd *repb.Command) (*scpb.TaskSize, error) {
	key, err := s.taskSizeKey(ctx, cmd)
	if err != nil {
		return nil, err
	}
	serializedSize, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	size := &scpb.TaskSize{}
	if err := proto.Unmarshal([]byte(serializedSize), size); err != nil {
		return nil, err
	}
	if size.EstimatedMemoryBytes == 0 || size.EstimatedMilliCpu == 0 {
		return nil, status.InternalError("found invalid task size stored in Redis")
	}
	return size, nil
}

func (s *taskSizer) taskSizeKey(ctx context.Context, cmd *repb.Command) (string, error) {
	// Get group ID (task sizing is segmented by group)
	groupKey, err := s.groupKey(ctx)
	if err != nil {
		return "", err
	}
	// For now, associate stats with the exact command, including the full
	// command line, env vars, and platform.
	// Note: This doesn't account for platform overrides for now
	// (--remote_exec_header=x-buildbuddy-platform.NAME=VALUE).
	cmdKey, err := commandKey(cmd)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/%s", redisKeyPrefix, groupKey, cmdKey), nil
}

func (s *taskSizer) groupKey(ctx context.Context) (string, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		if authutil.IsAnonymousUserError(err) && s.env.GetAuthenticator().AnonymousUsageEnabled(ctx) {
			return "ANON", nil
		}
		return "", err
	}
	return u.GetGroupID(), nil
}

func commandKey(cmd *repb.Command) (string, error) {
	// Include the command executable name in the key for easier debugging.
	// Truncate so that the keys cannot get too big.
	arg0 := "?"
	if len(cmd.Arguments) > 0 {
		arg0 = cmd.Arguments[0]
		if len(arg0) > 64 {
			arg0 = arg0[:64] + "..."
		}
	}
	b, err := proto.Marshal(cmd)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%x", arg0, sha256.Sum256(b)), nil
}

func estimateFromTestSize(testSize string) (bytes int64, milliCPU int64) {
	mb := 0
	cpu := 0

	switch testSize {
	case "small":
		mb = 20
		cpu = 1000
	case "medium":
		mb = 100
		cpu = 1000
	case "large":
		mb = 300
		cpu = 1000
	case "enormous":
		mb = 800
		cpu = 1000
	default:
		log.Warningf("Unknown testsize: %q", testSize)
		mb = 800
		cpu = 1000
	}
	return int64(mb * 1e6), int64(cpu)
}

func testSize(cmd *repb.Command) (s string, ok bool) {
	for _, envVar := range cmd.GetEnvironmentVariables() {
		if envVar.GetName() == testSizeEnvVar {
			return envVar.GetValue(), true
		}
	}
	return "", false
}

// Requested returns the explictily requested task size as described in
// https://www.buildbuddy.io/docs/rbe-platforms#runner-resource-allocation.
// There is no validation or clamping of the values so we can save exactly what
// the user requested. EstimatedMemory and EstimatedCPU override
// EstimatedComputeUnits.
func Requested(task *repb.ExecutionTask) *scpb.TaskSize {
	props, err := platform.ParseProperties(task)
	if err != nil {
		log.Infof("Failed to parse task properties, using empty requested size: %s", err)
		return new(scpb.TaskSize)
	}
	cpu := int64(props.EstimatedComputeUnits * ComputeUnitsToMilliCPU)
	mem := int64(props.EstimatedComputeUnits * ComputeUnitsToRAMBytes)
	if props.EstimatedMilliCPU > 0 {
		cpu = props.EstimatedMilliCPU
	}
	if props.EstimatedMemoryBytes > 0 {
		mem = props.EstimatedMemoryBytes
	}
	return &scpb.TaskSize{
		EstimatedMemoryBytes:   mem,
		EstimatedMilliCpu:      cpu,
		EstimatedFreeDiskBytes: props.EstimatedFreeDiskBytes,
		CustomResources:        props.CustomResources,
	}
}

// Default returns the default task size estimate for a task. This depends on
// properties like test size and isolation type, but it doesn't reflect the
// explicitly requested size. Values are NOT clamped within allowed ranges,
// so ApplyLimits should be called before using this size.
func Default(task *repb.ExecutionTask) *scpb.TaskSize {
	size := &scpb.TaskSize{
		EstimatedMemoryBytes:   DefaultMemEstimate,
		EstimatedMilliCpu:      DefaultCPUEstimate,
		EstimatedFreeDiskBytes: DefaultFreeDiskEstimate,
	}
	props, err := platform.ParseProperties(task)
	if err != nil {
		log.Infof("Failed to parse task properties, using default estimation: %s", err)
		return size
	}

	// Set default mem estimate based on whether this is a workflow.
	if props.WorkflowID != "" {
		size.EstimatedMemoryBytes = WorkflowMemEstimate
	}

	if s, ok := testSize(task.GetCommand()); ok {
		size.EstimatedMemoryBytes, size.EstimatedMilliCpu = estimateFromTestSize(s)
	}

	if props.WorkloadIsolationType == string(platform.FirecrackerContainerType) {
		size.EstimatedMemoryBytes += FirecrackerAdditionalMemEstimateBytes
		// Note: props.InitDockerd is only supported for docker-in-firecracker.
		if props.InitDockerd {
			size.EstimatedFreeDiskBytes += DockerInFirecrackerAdditionalDiskEstimateBytes
			size.EstimatedMemoryBytes += DockerInFirecrackerAdditionalMemEstimateBytes
		}
	}
	return size
}

// Override uses all non-empty values from over to override values in base.
// Both arguments are unmodified. Values are NOT clamped within allowed ranges,
// so ApplyLimits should be called before using this size.
func Override(base, over *scpb.TaskSize) *scpb.TaskSize {
	res := base.CloneVT()
	if over.GetEstimatedMemoryBytes() > 0 {
		res.EstimatedMemoryBytes = over.GetEstimatedMemoryBytes()
	}
	if over.GetEstimatedMilliCpu() > 0 {
		res.EstimatedMilliCpu = over.GetEstimatedMilliCpu()
	}
	if over.GetEstimatedFreeDiskBytes() > 0 {
		res.EstimatedFreeDiskBytes = over.GetEstimatedFreeDiskBytes()
	}
	if len(over.GetCustomResources()) > 0 {
		res.CustomResources = over.GetCustomResources()
	}
	return res
}

// ApplyLimits clamps each value in size to within an allowed range.
func ApplyLimits(ctx context.Context, efp interfaces.ExperimentFlagProvider, cmd *repb.Command, props *platform.Properties, size *scpb.TaskSize) *scpb.TaskSize {
	if size == nil {
		return nil
	}
	clone := size.CloneVT()

	minMemoryBytes := MinimumMemoryBytes
	minMilliCPU := MinimumMilliCPU
	// Test actions have higher minimums, determined by the test size ("small",
	// "medium", etc.)
	if s, ok := testSize(cmd); ok {
		minMemoryBytes, minMilliCPU = estimateFromTestSize(s)
	}

	if clone.EstimatedMilliCpu < minMilliCPU {
		clone.EstimatedMilliCpu = minMilliCPU
	}
	if clone.EstimatedMemoryBytes < minMemoryBytes {
		clone.EstimatedMemoryBytes = minMemoryBytes
	}

	limitMaxDisk := true
	if efp != nil {
		limitMaxDisk = !efp.Boolean(ctx, "disable-task-sizing-disk-limit", false)
	}

	if limitMaxDisk && clone.EstimatedFreeDiskBytes > MaxEstimatedFreeDisk {
		request := clone.EstimatedFreeDiskBytes
		if props == nil {
			clone.EstimatedFreeDiskBytes = MaxEstimatedFreeDisk
		} else if props.RecycleRunner {
			clone.EstimatedFreeDiskBytes = MaxEstimatedFreeDisk
		} else {
			clone.EstimatedFreeDiskBytes = MaxEstimatedFreeDiskRecycleFalse
		}
		log.CtxInfof(ctx, "Task requested %d free disk, capped at %d", request, clone.EstimatedFreeDiskBytes)
	}
	return clone
}

// GetCgroupSettings returns cgroup settings for a task, based on server
// and scheduled task size.
func GetCgroupSettings(ctx context.Context, fp interfaces.ExperimentFlagProvider, size *scpb.TaskSize, metadata *scpb.SchedulingMetadata) *scpb.CgroupSettings {
	settings := &scpb.CgroupSettings{
		PidsMax: new(int64(*pidLimit + (*additionalPIDsLimitPerCPU*size.GetEstimatedMilliCpu())/1000)),

		// Set CPU weight using the same milliCPU => weight conversion used by k8s.
		// Using the same weight as k8s is not strictly required, since we are
		// weighting tasks relative to each other, not relative to k8s pods. But it
		// makes the scaling of the values a little more sensible/consistent when
		// inspecting cgroup weights on a node.
		CpuWeight: new(int64(CPUMillisToWeight(size.GetEstimatedMilliCpu()))),

		// Apply the global max CPU limit.
		CpuQuotaLimitUsec:  new(int64((*cpuQuotaLimit).Microseconds())),
		CpuQuotaPeriodUsec: new(int64((*cpuQuotaPeriod).Microseconds())),
	}
	if *memoryLimitBytes > 0 {
		settings.MemoryLimitBytes = new(int64(*memoryLimitBytes))
	}
	if *memoryOOMGroup {
		settings.MemoryOomGroup = new(*memoryOOMGroup)
	}
	// Allow experimenting with setting memory hard limits as a fraction of the
	// task size plus an additional fixed value.
	if fp != nil {
		// Add user-requested memory bytes and measured memory bytes (if we have
		// them) so that we can set different limits depending on how confident
		// we are in the size.
		options := []any{
			experiments.WithContext("measured_memory_bytes", metadata.GetMeasuredTaskSize().GetEstimatedMemoryBytes()),
			experiments.WithContext("requested_memory_bytes", metadata.GetRequestedTaskSize().GetEstimatedMemoryBytes()),
		}
		memoryHardLimitMultiplier := fp.Float64(ctx, "remote_execution.memory_hard_limit_size_multiplier", 0, options...)
		if memoryHardLimitMultiplier > 0 {
			memoryHardLimitAdditionalBytes := fp.Int64(ctx, "remote_execution.memory_hard_limit_additional_bytes", 0, options...)
			limit := int64(float64(size.GetEstimatedMemoryBytes())*memoryHardLimitMultiplier) + memoryHardLimitAdditionalBytes
			settings.MemoryLimitBytes = new(limit)
		}
	}
	return settings
}

func CPUMillisToWeight(cpuMillis int64) int64 {
	shares := CPUMillisToShares(cpuMillis)
	return CPUSharesToWeight(shares)
}

// CPUSharesToWeight converts "OCI" CPU share units (which are based on cgroup
// v1 CPU shares) to cgroup2 CPU weight units.
func CPUSharesToWeight(shares int64) int64 {
	// Clamp to min/max allowed values
	shares = min(shares, cpuSharesMax)
	shares = max(shares, cpuSharesMin)
	// Apply linear mapping
	return (cpuWeightMin + ((shares-cpuSharesMin)*(cpuWeightMax-cpuWeightMin))/cpuSharesMax)
}

// CPUMillisToShares converts a milliCPU value to an appropriate value of OCI
// CPU shares.
func CPUMillisToShares(cpuMillis int64) int64 {
	// Match what k8s does:
	// https://github.com/kubernetes/kubernetes/blob/40f222b6201d5c6476e5b20f57a4a7d8b2d71845/pkg/kubelet/cm/helpers_linux.go#L44
	cpuShares := cpuMillis * 1024 / 1000
	cpuShares = min(cpuShares, cpuSharesMax)
	cpuShares = max(cpuShares, cpuSharesMin)
	return cpuShares
}

func String(size *scpb.TaskSize) string {
	resources := []string{
		fmt.Sprintf("milli_cpu=%d", size.GetEstimatedMilliCpu()),
		fmt.Sprintf("memory_bytes=%d", size.GetEstimatedMemoryBytes()),
	}
	for _, r := range size.GetCustomResources() {
		resources = append(resources, fmt.Sprintf("%s=%f", r.GetName(), r.GetValue()))
	}
	return strings.Join(resources, ", ")
}
