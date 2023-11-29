//go:build windows

package fastcopy

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func reflink(source, destination string) error {
	return status.UnimplementedError("reflink not supported")
}
