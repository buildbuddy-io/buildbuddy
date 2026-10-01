//go:build !linux

package dirtools

import "context"

func waitForExecutableReady(ctx context.Context, path string) error {
	return nil
}
