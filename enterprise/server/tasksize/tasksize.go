package tasksize

import (
	"log"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

const (
	testSizeEnvVar = "TEST_SIZE"

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
	return &scpb.TaskSize{
		EstimatedMemoryBytes: memEstimate,
		EstimatedMilliCpu:    cpuEstimate,
	}
}
