//go:build !linux && !darwin

package tundev

import (
	"fmt"
	"runtime"

	"golang.zx2c4.com/wireguard/tun"
)

// Open is unimplemented outside Linux and macOS.
func Open(name string) (tun.Device, error) {
	return nil, fmt.Errorf("the tunnel is not supported on %s", runtime.GOOS)
}

func EnsureConfigured(dev tun.Device, cidr string) error { return nil }

func RequiresRoot() bool { return false }
