//go:build !linux && !darwin

package tundev

import (
	"fmt"
	"runtime"

	"golang.zx2c4.com/wireguard/tun"
)

// Open is unimplemented outside Linux and macOS.
func Open(name string) (tun.Device, <-chan struct{}, error) {
	return nil, nil, fmt.Errorf("the tunnel is not supported on %s", runtime.GOOS)
}

func Describe(configured string) string { return configured }
