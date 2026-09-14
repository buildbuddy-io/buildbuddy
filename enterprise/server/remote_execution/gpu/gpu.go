package gpu

import (
	"errors"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	gpuMemoryTrackingEnabled = flag.Bool("executor.gpu_memory_tracking_enabled", false, "Whether to measure GPU memory used by task processes. Also required to schedule tasks on GPU memory. Requires a Linux executor with the NVIDIA Management Library (libnvidia-ml) available at runtime.")
	gpuMemoryPollInterval    = flag.Duration("executor.gpu_memory_poll_interval", 250*time.Millisecond, "How often to sample GPU process memory. Shorter poll intervals add more CPU overhead.")
)

// Configure validates the GPU memory tracking configuration and initializes
// its platform implementation when tracking is enabled. Memory polling starts
// on the first CgroupUsage call. The configure and cgroupUsage functions are
// defined per platform in gpu_linux.go and gpu_unsupported.go. Static builds
// use gpu_unsupported.go, since the NVML bindings cannot be linked statically.
func Configure() error {
	if !*gpuMemoryTrackingEnabled {
		return nil
	}
	if *gpuMemoryPollInterval < time.Millisecond {
		return errors.New("executor.gpu_memory_poll_interval must be at least 1ms")
	}
	return configure()
}

// CgroupUsage returns the latest GPU memory reading for a cgroup v2 path.
// The reading holds point-in-time usage; callers that need task-level peaks
// must fold successive readings, which container.UsageStats.Update does.
func CgroupUsage(cgroupPath string) *repb.GPUUsage {
	if !*gpuMemoryTrackingEnabled {
		return nil
	}
	return cgroupUsage(cgroupPath)
}

// memoryDetector reports the total memory of the GPUs discovered by Configure
// so that resources.ConfigureGPU can detect the executor's GPU memory capacity.
type memoryDetector struct{}

// MemoryDetector returns the detector for resources.ConfigureGPU, or nil when
// GPU memory tracking is disabled, since Configure only initializes NVML with
// tracking enabled.
func MemoryDetector() resources.GPUMemoryDetector {
	if !*gpuMemoryTrackingEnabled {
		return nil
	}
	return memoryDetector{}
}

func (memoryDetector) GetTotalGPUMemoryBytes() (int64, error) {
	return GetTotalGPUMemoryBytes()
}
