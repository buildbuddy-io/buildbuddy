package tasksize

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	useMeasuredSizes = flag.Bool("remote_execution.use_measured_task_sizes", false, "Whether to use measured usage stats to determine task sizes.")
)

const (
	testSizeEnvVar = "TEST_SIZE"

	// Definitions for BCU ("BuildBuddy Compute Unit")

	ComputeUnitsToMilliCPU = 1000      // 1 BCU = 1000 milli-CPU
	ComputeUnitsToRAMBytes = 2.5 * 1e9 // 1 BCU = 2.5GB of memory

	// Default resource estimates

	DefaultMemEstimate      = int64(400 * 1e6)
	WorkflowMemEstimate     = int64(8 * 1e9)
	DefaultCPUEstimate      = int64(600)
	DefaultFreeDiskEstimate = int64(100 * 1e6) // 100 MB

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
	MaxResourceCapacityRatio = 0.8

	// The expiration for task usage measurements stored in Redis.
	sizeMeasurementExpiration = 5 * 24 * time.Hour

	// Redis key prefix used for holding current task size estimates.
	redisKeyPrefix = "taskSize"
)

// Register registers the task sizer with the env.
func Register(env environment.Env) error {
	sizer, err := NewSizer(env)
	if err != nil {
		return err
	}
	env.SetTaskSizer(sizer)
	return nil
}

type taskSizer struct {
	env environment.Env
	rdb redis.UniversalClient
}

func NewSizer(env environment.Env) (*taskSizer, error) {
	ts := &taskSizer{env: env}
	if *useMeasuredSizes {
		if env.GetRemoteExecutionRedisClient() == nil {
			return nil, status.FailedPreconditionError("missing Redis client configuration")
		}
		ts.rdb = env.GetRemoteExecutionRedisClient()
	}
	return ts, nil
}

func (s *taskSizer) Get(ctx context.Context, task *repb.ExecutionTask) *scpb.TaskSize {
	if !*useMeasuredSizes {
		return nil
	}
	props := platform.ParseProperties(task)
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
	return &scpb.TaskSize{
		EstimatedMemoryBytes: recordedSize.EstimatedMemoryBytes,
		EstimatedMilliCpu:    recordedSize.EstimatedMilliCpu,
	}
}

func (s *taskSizer) Update(ctx context.Context, cmd *repb.Command, md *repb.ExecutedActionMetadata) error {
	if !*useMeasuredSizes {
		return nil
	}
	statusLabel := "ok"
	defer func() {
		props := platform.ParseProperties(&repb.ExecutionTask{Command: cmd})
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
	execDuration := md.GetExecutionCompletedTimestamp().AsTime().Sub(md.GetExecutionStartTimestamp().AsTime())
	// If execution duration is missing or invalid, we won't be able to compute
	// milli-CPU usage.
	if execDuration <= 0 {
		statusLabel = "missing_stats"
		return status.InvalidArgumentErrorf("execution duration is missing or invalid")
	}
	key, err := s.taskSizeKey(ctx, cmd)
	if err != nil {
		statusLabel = "error"
		return err
	}
	// Compute milliCPU as CPU-milliseconds used per second of execution time.
	// Run through Ceil() to prevent storing 0 values in case the CPU usage
	// was greater than 0 but less than 1 CPU-millisecond.
	milliCPUFloat := (float64(stats.GetCpuNanos()) / 1e6) / execDuration.Seconds()
	milliCPU := int64(math.Ceil(milliCPUFloat))
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
	// (--remote_header=x-buildbuddy-platform.NAME=VALUE).
	cmdKey, err := commandKey(cmd)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s/%s/%s", redisKeyPrefix, groupKey, cmdKey), nil
}

func (s *taskSizer) groupKey(ctx context.Context) (string, error) {
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		if perms.IsAnonymousUserError(err) && s.env.GetAuthenticator().AnonymousUsageEnabled() {
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

func testSize(testSize string) (int64, int64) {
	mb := 0
	cpu := 0

	switch testSize {
	case "small":
		mb = 20
		cpu = 600
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

// Estimate returns the default task size estimate for a task. It respects hints
// from the task such as test size and estimated compute units, but does not use
// information about historical task executions.
func Estimate(task *repb.ExecutionTask) *scpb.TaskSize {
	props := platform.ParseProperties(task)

	memEstimate := DefaultMemEstimate
	// Set default mem estimate based on whether this is a workflow.
	if props.WorkflowID != "" {
		memEstimate = WorkflowMemEstimate
	}
	cpuEstimate := DefaultCPUEstimate
	freeDiskEstimate := DefaultFreeDiskEstimate

	for _, envVar := range task.GetCommand().GetEnvironmentVariables() {
		if envVar.GetName() == testSizeEnvVar {
			memEstimate, cpuEstimate = testSize(envVar.GetValue())
			break
		}
	}
	if props.WorkloadIsolationType == string(platform.FirecrackerContainerType) {
		memEstimate += FirecrackerAdditionalMemEstimateBytes
		// Note: props.InitDockerd is only supported for docker-in-firecracker.
		if props.InitDockerd {
			freeDiskEstimate += DockerInFirecrackerAdditionalDiskEstimateBytes
			memEstimate += DockerInFirecrackerAdditionalMemEstimateBytes
		}
	}

	if props.EstimatedComputeUnits > 0 {
		cpuEstimate = props.EstimatedComputeUnits * ComputeUnitsToMilliCPU
		memEstimate = props.EstimatedComputeUnits * ComputeUnitsToRAMBytes
	}
	if props.EstimatedFreeDiskBytes > 0 {
		freeDiskEstimate = props.EstimatedFreeDiskBytes
	}
	if freeDiskEstimate > MaxEstimatedFreeDisk {
		log.Warningf("Task requested %d free disk which is more than the max %d", freeDiskEstimate, MaxEstimatedFreeDisk)
		freeDiskEstimate = MaxEstimatedFreeDisk
	}

	return &scpb.TaskSize{
		EstimatedMemoryBytes:   memEstimate,
		EstimatedMilliCpu:      cpuEstimate,
		EstimatedFreeDiskBytes: freeDiskEstimate,
	}
}
