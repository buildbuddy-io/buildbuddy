//go:build !linux && !darwin

package install

import (
	"fmt"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
)

func Install(cfg *tunnelconfig.Config) error {
	return fmt.Errorf("bbaccess tunnel install is not supported on %s", runtime.GOOS)
}

func Uninstall(cfg *tunnelconfig.Config) error {
	return fmt.Errorf("bbaccess tunnel uninstall is not supported on %s", runtime.GOOS)
}
