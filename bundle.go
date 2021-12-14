package buildbuddy

import (
	"context"
	"embed"

	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/fs"
)

// NB: We cannot embed "static", "bazel-out", etc here, because when this
// package is built as dependency of the enterprise package, those files
// do not exist. Instead we bundle *, which never fails, although the
// resulting filesystem may be empty.
//
//go:embed *
var all embed.FS

func Get(ctx context.Context) (fs.FS, error) {
	return fileresolver.GetBundleFS(ctx, all)
}
