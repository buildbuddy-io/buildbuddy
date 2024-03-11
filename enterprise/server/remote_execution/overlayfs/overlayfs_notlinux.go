//go:build (darwin && !ios) || windows

package overlayfs

import (
	"context"
	"runtime"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func Convert(ctx context.Context, path string, opts Opts) (*Overlay, error) {
	return nil, status.UnimplementedErrorf("overlayfs is not supported on %s", runtime.GOOS)
}

func (o *Overlay) Remove(ctx context.Context) error {
	return status.UnimplementedErrorf("overlayfs is not supported on %s", runtime.GOOS)
}

func (o *Overlay) Apply(ctx context.Context, opts ApplyOpts) error {
	return status.UnimplementedErrorf("overlayfs is not supported on %s", runtime.GOOS)
}
