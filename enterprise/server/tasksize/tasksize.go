package tasksize

import (
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

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

	MaxEstimatedFreeDisk = int64(20 * 1e9) // 20GB

	// The fraction of an executor's allocatable resources to make available for task sizing.
	MaxResourceCapacityRatio = 0.8
)

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
