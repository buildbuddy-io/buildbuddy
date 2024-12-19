package tasksize

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize_model"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	useMeasuredSizes    = flag.Bool("remote_execution.use_measured_task_sizes", false, "Whether to use measured usage stats to determine task sizes.")
	modelEnabled        = flag.Bool("remote_execution.task_size_model.enabled", false, "Whether to enable model-based task size prediction.")
	psiCorrectionFactor = flag.Float64("remote_execution.task_size_psi_correction", 1.0, "What percentage of full-stall time should be subtracted from the execution duration.")
	cpuQuotaLimit       = flag.Duration("remote_execution.cpu_quota_limit", 30*100*time.Millisecond /*30 cores*/, "Maximum CPU time allowed for each quota period.")
	cpuQuotaPeriod      = flag.Duration("remote_execution.cpu_quota_period", 100*time.Millisecond, "How often the CPU quota is refreshed.")
	pidLimit            = flag.Int64("remote_execution.pids_limit", 2048, "Maximum number of processes allowed per task at any time.")
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

	MaxEstimatedFreeDisk = int64(100 * 1e9) // 100GB

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

func (s *taskSizer) Get(ctx context.Context, task *repb.ExecutionTask) *scpb.TaskSize {
	if !*useMeasuredSizes {
		return nil
	}
	props, err := platform.ParseProperties(task)
	if err != nil {
		// TODO(sluongng): reject tasks that fail validation so users could catch errors sooner
		log.CtxInfof(ctx, "Failed to parse task properties: %s", err)
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
	recordedSize, err := s.lastRecordedSize(ctx, task)
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
	return ApplyLimits(task, &scpb.TaskSize{
		EstimatedMemoryBytes: recordedSize.EstimatedMemoryBytes,
		EstimatedMilliCpu:    recordedSize.EstimatedMilliCpu,
	})
}

func (s *taskSizer) Predict(ctx context.Context, task *repb.ExecutionTask) *scpb.TaskSize {
	if s.model == nil {
		return nil
	}
	return ApplyLimits(task, s.model.Predict(ctx, task))
}

func (s *taskSizer) Update(ctx context.Context, action *repb.Action, cmd *repb.Command, md *repb.ExecutedActionMetadata) error {
	if !*useMeasuredSizes {
		return nil
	}
	statusLabel := "ok"
	defer func() {
		props, err := platform.ParseProperties(&repb.ExecutionTask{Action: action, Command: cmd})
		if err != nil {
			log.CtxInfof(ctx, "Failed to parse task properties: %s", err)
		}
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
	milliCPU := computeMilliCPU(ctx, md)
	if milliCPU <= 0 {
		statusLabel = "missing_stats"
		return status.InvalidArgumentErrorf("execution duration is missing or invalid")
	}
	key, err := s.taskSizeKey(ctx, cmd)
	if err != nil {
		statusLabel = "error"
		return err
	}
	size := &scpb.TaskSize{
		EstimatedMilliCpu:    milliCPU,
		EstimatedMemoryBytes: stats.GetPeakMemoryBytes(),
	}
	b, err := proto.Marshal(size)
	if err != nil {
		statusLabel = "error"
		return err
	}
	s.rdb.Set(ctx, key, string(b), sizeMeasurementExpiration)
	return err
}

func computeMilliCPU(ctx context.Context, md *repb.ExecutedActionMetadata) int64 {
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

	return min(milliCPU, *milliCPULimit)
}

func (s *taskSizer) lastRecordedSize(ctx context.Context, task *repb.ExecutionTask) (*scpb.TaskSize, error) {
	key, err := s.taskSizeKey(ctx, task.GetCommand())
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

func testSize(task *repb.ExecutionTask) (s string, ok bool) {
	for _, envVar := range task.GetCommand().GetEnvironmentVariables() {
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

	if s, ok := testSize(task); ok {
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
func ApplyLimits(task *repb.ExecutionTask, size *scpb.TaskSize) *scpb.TaskSize {
	if size == nil {
		return nil
	}
	clone := size.CloneVT()

	minMemoryBytes := MinimumMemoryBytes
	minMilliCPU := MinimumMilliCPU
	// Test actions have higher minimums, determined by the test size ("small",
	// "medium", etc.)
	if s, ok := testSize(task); ok {
		minMemoryBytes, minMilliCPU = estimateFromTestSize(s)
	}

	if clone.EstimatedMilliCpu < minMilliCPU {
		clone.EstimatedMilliCpu = minMilliCPU
	}
	if clone.EstimatedMemoryBytes < minMemoryBytes {
		clone.EstimatedMemoryBytes = minMemoryBytes
	}
	if clone.EstimatedFreeDiskBytes > MaxEstimatedFreeDisk {
		log.Infof("Task %q requested %d free disk which is more than the max %d", task.GetExecutionId(), clone.EstimatedFreeDiskBytes, MaxEstimatedFreeDisk)
		clone.EstimatedFreeDiskBytes = MaxEstimatedFreeDisk
	}
	return clone
}

// GetCgroupSettings returns cgroup settings for a task, based on server
// and scheduled task size.
func GetCgroupSettings(size *scpb.TaskSize) *scpb.CgroupSettings {
	return &scpb.CgroupSettings{
		PidsMax: proto.Int64(*pidLimit),

		// Set CPU weight using the same milliCPU => weight conversion used by k8s.
		// Using the same weight as k8s is not strictly required, since we are
		// weighting tasks relative to each other, not relative to k8s pods. But it
		// makes the scaling of the values a little more sensible/consistent when
		// inspecting cgroup weights on a node.
		CpuWeight: proto.Int64(CPUMillisToWeight(size.GetEstimatedMilliCpu())),

		// Apply the global max CPU limit.
		CpuQuotaLimitUsec:  proto.Int64((*cpuQuotaLimit).Microseconds()),
		CpuQuotaPeriodUsec: proto.Int64((*cpuQuotaPeriod).Microseconds()),
	}
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
