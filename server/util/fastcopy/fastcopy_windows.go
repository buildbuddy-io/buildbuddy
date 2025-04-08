//go:build windows

package fastcopy

import (
	"github.com/buildbuddy-io/buildbuddy/v2/server/util/status"
)

func reflink(source, destination string) error {
	return status.UnimplementedError("reflink not supported")
}
