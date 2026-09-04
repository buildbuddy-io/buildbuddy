//go:build !linux || android || !cgo || static

package gpu

import (
	"errors"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// configure rejects GPU memory tracking on platforms where NVML is
// unavailable. This includes static builds (the "static" Go build tag, set by
// the musl platforms in //platforms), because go-nvml loads libnvidia-ml with
// dlopen and needs glibc-only dlfcn symbols, so it cannot be built into a
// static musl binary.
func configure() error {
	return errors.New("GPU memory tracking requires a dynamically linked Linux build with cgo enabled")
}

func cgroupUsage(cgroupPath string) *repb.GPUUsage {
	return nil
}
