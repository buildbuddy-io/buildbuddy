package buildbuddy

import (
	"embed"
	"io/fs"

	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
)

// NB: We cannot embed "static", "bazel-out", etc here, because when this
// package is built as dependency of the enterprise package, those files
// do not exist. Instead we bundle *, which never fails, although the
// resulting filesystem may be empty.
//
//go:embed *
var all embed.FS

func Get() (fs.FS, error) {
	return fileresolver.GetBundleFS(all)
}
