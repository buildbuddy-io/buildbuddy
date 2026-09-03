//go:build !linux || android || !cgo

package gpu

import (
	"errors"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func configure() error {
	return errors.New("GPU memory tracking requires a Linux build with cgo enabled")
}

func cgroupUsage(cgroupPath string) *repb.GPUUsage {
	return nil
}
