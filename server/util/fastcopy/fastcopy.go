//go:build !darwin
// +build !darwin

package fastcopy

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/os"
)

func FastCopy(ctx context.Context, source, destination string) error {
	err := os.Link(ctx, source, destination)
	if !os.IsExist(err) {
		return err
	}
	return nil
}
