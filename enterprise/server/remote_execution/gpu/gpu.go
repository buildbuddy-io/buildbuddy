package gpu

import (
	"errors"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	gpuMemoryTrackingEnabled = flag.Bool("executor.gpu_memory_tracking_enabled", false, "Whether to measure GPU memory used by task processes. Requires a Linux executor with the NVIDIA Management Library (libnvidia-ml) available at runtime.")
	gpuMemoryPollInterval    = flag.Duration("executor.gpu_memory_poll_interval", 250*time.Millisecond, "How often to sample GPU process memory. Shorter poll intervals add more CPU overhead.")
)

// Configure validates the GPU memory tracking configuration and initializes
// its platform implementation when tracking is enabled. The configure and
// cgroupUsage functions are defined per platform in gpu_linux.go and
// gpu_unsupported.go.
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
