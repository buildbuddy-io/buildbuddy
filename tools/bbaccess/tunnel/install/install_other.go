//go:build !linux && !darwin

package install

import (
	"fmt"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
)

// helperPath is unused: there is no helper to install here.
var helperPath string

func Install(cfg *tunnelconfig.Config) error {
	return fmt.Errorf("bbaccess tunnel install is not supported on %s", runtime.GOOS)
}

func needed(cfg *tunnelconfig.Config) (string, error) { return "", nil }

func Uninstall(cfg *tunnelconfig.Config) error {
	return fmt.Errorf("bbaccess tunnel uninstall is not supported on %s", runtime.GOOS)
}
