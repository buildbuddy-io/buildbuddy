//go:build !linux && !darwin

package install

import (
	"fmt"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
)

func Install(cfg *config.Config, user string) error {
	return fmt.Errorf("bbaccess tunnel install is not supported on %s", runtime.GOOS)
}

func Uninstall(cfg *config.Config) error {
	return fmt.Errorf("bbaccess tunnel uninstall is not supported on %s", runtime.GOOS)
}
