package tasksize

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
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

type taskSizer struct{}

func NewSizer(env environment.Env) (*taskSizer, error) {
	return &taskSizer{}, nil
}

func (s *taskSizer) Estimate(ctx context.Context, task *repb.ExecutionTask) *scpb.TaskSize {
	// TODO(bduffany): Use previous execution stats
	return Estimate(task)
}

func (s *taskSizer) Update(ctx context.Context, cmd *repb.Command, summary *espb.ExecutionSummary) error {
	// TODO(bduffany): Implement
	return status.UnimplementedError("not implemented")
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
