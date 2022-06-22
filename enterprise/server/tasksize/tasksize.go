package tasksize

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/gogo/protobuf/proto"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

var (
	adaptiveSizingEnabled = flag.Bool("remote_execution.adaptive_task_sizing_enabled", false, "Whether to adapt task sizing estimates based on task usage stats.")
)

const (
	testSizeEnvVar = "TEST_SIZE"

	// Definitions for BCU ("BuildBuddy Compute Unit")

	computeUnitsToMilliCPU = 1000      // 1 BCU = 1000 milli-CPU
	computeUnitsToRAMBytes = 2.5 * 1e9 // 1 BCU = 2.5GB of memory

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

	// The expiration for all task-sizing keys stored in Redis.
	redisKeyExpiration = 12 * time.Hour
	// Redis key prefix used for holding current task size estimates.
	redisKeyPrefix = "taskSize"
	// Redis hash key used to hold the CPU estimate for a task (in milli-CPU).
	// The hash is stored under a key derived from the task parameters.
	redisCPUHashKey = "milliCPU"
	// Redis hash key used to hold the memory estimate (in bytes). The hash is
	// stored under a key derived from the task.
	redisMemHashKey = "memBytes"

	// When using adaptive task sizing, multiply the stat-based memory estimate
	// by this much in order to determine effective the memory estimate. This
	// allows some wiggle room for new tasks to use more memory than the
	// previously recorded task.
	adaptiveSizingMemoryMultiplier = 1.10
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
	var rdb redis.UniversalClient
	if *adaptiveSizingEnabled {
		rdb = env.GetRemoteExecutionRedisClient()
		if rdb == nil {
			return nil, status.FailedPreconditionError("missing Redis client configuration")
		}
	}
	return &taskSizer{
		env: env,
		rdb: rdb,
	}, nil
}

func (s *taskSizer) Estimate(ctx context.Context, task *repb.ExecutionTask) *scpb.TaskSize {
	defaultSize := Estimate(task)
	if !*adaptiveSizingEnabled {
		return defaultSize
	}
	recordedSize, err := s.lastRecordedSize(ctx, task)
	if err != nil {
		log.Warningf("Failed to read task size from Redis; falling back to default size estimate: %s", err)
		return defaultSize
	}
	if recordedSize == nil {
		return defaultSize
	}
	return &scpb.TaskSize{
		EstimatedMemoryBytes: int64(float64(recordedSize.EstimatedMemoryBytes) * adaptiveSizingMemoryMultiplier),
		EstimatedMilliCpu:    recordedSize.EstimatedMilliCpu,
	}
}

func (s *taskSizer) Update(ctx context.Context, cmd *repb.Command, summary *espb.ExecutionSummary) error {
	if !*adaptiveSizingEnabled {
		return nil
	}
	// If we are missing CPU/memory stats, do nothing. This is expected for
	// tasks that either completed too quickly to get a sample of their CPU/mem
	// usage, or tasks where the workload isolation type doesn't yet support the
	// Stats() API.
	stats := summary.GetUsageStats()
	if stats.GetCpuNanos() == 0 || stats.GetPeakMemoryBytes() == 0 {
		return nil
	}
	md := summary.GetExecutedActionMetadata()
	execDuration := md.GetExecutionCompletedTimestamp().AsTime().Sub(md.GetExecutionStartTimestamp().AsTime())
	// If execution duration is missing or invalid, we won't be able to compute
	// milli-CPU usage.
	if execDuration <= 0 {
		return status.InvalidArgumentErrorf("execution duration is missing or invalid")
	}
	// Compute milliCPU as CPU-milliseconds used per second of execution time.
	milliCPU := int64((float64(stats.GetCpuNanos()) / 1e6) / execDuration.Seconds())
	key, err := s.taskSizeKey(ctx, cmd)
	if err != nil {
		return err
	}
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, key, redisMemHashKey, stats.GetPeakMemoryBytes())
	pipe.HSet(ctx, key, redisCPUHashKey, milliCPU)
	pipe.Expire(ctx, key, redisKeyExpiration)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *taskSizer) lastRecordedSize(ctx context.Context, task *repb.ExecutionTask) (*scpb.TaskSize, error) {
	key, err := s.taskSizeKey(ctx, task.GetCommand())
	if err != nil {
		return nil, err
	}
	m, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	// Note: if the key is not found, HGETALL returns an empty map, not a
	// redis.Nil error.
	if len(m) == 0 {
		return nil, nil
	}
	mInt64, err := redisutil.Int64Values(m)
	if err != nil {
		return nil, err
	}
	if mInt64[redisMemHashKey] == 0 || mInt64[redisCPUHashKey] == 0 {
		return nil, status.InternalError("found invalid task size stored in Redis")
	}
	return &scpb.TaskSize{
		EstimatedMemoryBytes: mInt64[redisMemHashKey],
		EstimatedMilliCpu:    mInt64[redisCPUHashKey],
	}, nil
}

func (s *taskSizer) taskSizeKey(ctx context.Context, cmd *repb.Command) (string, error) {
	// Get group ID (task sizing is segmented by group)
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		if !perms.IsAnonymousUserError(err) || !s.env.GetAuthenticator().AnonymousUsageEnabled() {
			return "", err
		}
	}
	groupKey := "ANON"
	if u != nil {
		groupKey = u.GetGroupID()
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

func commandKey(cmd *repb.Command) (string, error) {
	// Include the command executable name in the key for easier debugging.
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

// Estimate returns the default task size estimate for a task. It does not use
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
		cpuEstimate = props.EstimatedComputeUnits * computeUnitsToMilliCPU
		memEstimate = props.EstimatedComputeUnits * computeUnitsToRAMBytes
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
