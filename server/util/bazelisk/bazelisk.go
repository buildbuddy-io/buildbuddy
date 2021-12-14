package bazelisk

import (
	"context"
	"embed"

	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/fs"
)

//go:embed *
var embedFS embed.FS

func Open(ctx context.Context) (fs.File, error) {
	return fs.CtxFSWrapper(embedFS).Open(ctx, "bazelisk-1.10.1")
}
