//go:build windows

package block_io

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type Device struct{}

func LookupDevice(path string) (*Device, error) {
	return nil, status.UnimplementedError("not yet implemented on windows")
}
