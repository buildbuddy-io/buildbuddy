//go:build !linux || android || !cgo || static

package gpu

import (
	"errors"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// GetTotalGPUMemoryBytes returns an error on platforms without NVML support.
func GetTotalGPUMemoryBytes() (int64, error) {
	return 0, errors.New("GPU memory queries require a dynamically linked Linux build with cgo enabled")
}

// configure does nothing on platforms without NVML support.
func configure() error {
	return nil
}

func cgroupUsage(cgroupPath string) *repb.GPUUsage {
	return nil
}
