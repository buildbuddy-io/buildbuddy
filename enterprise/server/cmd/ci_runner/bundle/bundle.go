package bundle

import (
	"embed"
	"io/fs"

	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
)

// NB: Include everything in bazel `embedsrcs` with `*`.
//
//go:embed *
var all embed.FS

func Get() fs.FS {
	return fileresolver.New(all, "enterprise/server/cmd/ci_runner")
}
