package tasksize

import (
	"log"
	"strconv"
	"strings"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

const (
	testSizeEnvVar = "TEST_SIZE"
	// A BuildBuddy Compute Unit is defined as 1 cpu and 2.5GB of memory.
	estimatedComputeUnitsPropertyKey = "EstimatedComputeUnits"
	maxComputeUnits                  = 8         // We can't schedule tasks larger than 8 compute units.
	computeUnitsToMilliCPU           = 1000      // 1 BPU = 1000 milli-CPU
	computeUnitsToRAMBytes           = 2.5 * 1e9 // 1 BPU = 2.5GB of memory

	// This is the default resource estimate for a task that we can't
	// otherwise determine the size for.
	DefaultMemEstimate = int64(400 * 1e6)
	DefaultCPUEstimate = int64(600)
)

func testSize(testSize string) (int64, int64) {
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
		log.Printf("Unknown testsize: %q", testSize)
		mb = 800
		cpu = 1000
	}
	return int64(mb * 1e6), int64(cpu)
}

func Estimate(cmd *repb.Command) *scpb.TaskSize {
	memEstimate := DefaultMemEstimate
	cpuEstimate := DefaultCPUEstimate
	for _, envVar := range cmd.GetEnvironmentVariables() {
		if envVar.GetName() == testSizeEnvVar {
			memEstimate, cpuEstimate = testSize(envVar.GetValue())
			break
		}
	}
	for _, property := range cmd.GetPlatform().GetProperties() {
		if strings.ToLower(property.Name) == strings.ToLower(estimatedComputeUnitsPropertyKey) {
			if bpus, err := strconv.ParseInt(property.Value, 10, 64); err == nil && bpus > 0 && bpus <= maxComputeUnits {
				cpuEstimate = bpus * computeUnitsToMilliCPU
				// TODO(siggisim): Enable memory estimation once executors no longer hang on oversized tasks.
				// memEstimate = bpus * computeUnitsToRAMBytes
			}
		}
	}

	return &scpb.TaskSize{
		EstimatedMemoryBytes: memEstimate,
		EstimatedMilliCpu:    cpuEstimate,
	}
}
