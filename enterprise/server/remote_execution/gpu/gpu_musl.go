package gpu

import (
	"errors"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func configure() error {
	return errors.New("GPU memory tracking is not supported by musl builds")
}

func cgroupUsage(cgroupPath string) *repb.GPUUsage {
	return nil
}
