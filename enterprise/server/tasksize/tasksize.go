package tasksize

import (
	"strconv"
	"strings"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

const (
	testSizeEnvVar = "TEST_SIZE"
	// A BuildBuddy Compute Unit is defined as 1 cpu and 2.5GB of memory.
	EstimatedComputeUnitsPropertyKey = "EstimatedComputeUnits"
	EstimatedFreeDiskPropertyKey     = "EstimatedFreeDiskBytes"
	MaxEstimatedFreeDisk             = int64(20 * 1e9) // 20GB
	computeUnitsToMilliCPU           = 1000            // 1 BCU = 1000 milli-CPU
	computeUnitsToRAMBytes           = 2.5 * 1e9       // 1 BCU = 2.5GB of memory

	// This is the default resource estimate for a task that we can't
	// otherwise determine the size for.
	DefaultMemEstimate      = int64(400 * 1e6)
	DefaultCPUEstimate      = int64(600)
	DefaultFreeDiskEstimate = int64(100 * 1e6) // 100 MB

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

func Estimate(cmd *repb.Command) *scpb.TaskSize {
	memEstimate := DefaultMemEstimate
	cpuEstimate := DefaultCPUEstimate
	freeDiskEstimate := DefaultFreeDiskEstimate
	for _, envVar := range cmd.GetEnvironmentVariables() {
		if envVar.GetName() == testSizeEnvVar {
			memEstimate, cpuEstimate = testSize(envVar.GetValue())
			break
		}
	}
	for _, property := range cmd.GetPlatform().GetProperties() {
		if strings.ToLower(property.Name) == strings.ToLower(EstimatedComputeUnitsPropertyKey) {
			if bcus, err := strconv.ParseInt(property.Value, 10, 64); err == nil && bcus > 0 {
				cpuEstimate = bcus * computeUnitsToMilliCPU
				memEstimate = bcus * computeUnitsToRAMBytes
			}
		}
		if strings.ToLower(property.Name) == strings.ToLower(EstimatedFreeDiskPropertyKey) {
			v, err := strconv.ParseInt(property.Value, 10, 64)
			if err != nil {
				log.Warningf("Could not parse disk space property: %s", err)
				continue
			}
			freeDiskEstimate = v
		}
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
