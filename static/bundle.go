package static

import (
	"embed"
	"io/fs"

	"github.com/buildbuddy-io/buildbuddy/v2/server/util/fileresolver"
)

// NB: Include everything in bazel `embedsrcs` with `*`.
//
//go:embed *
var all embed.FS

func GetStaticFS() (fs.FS, error) {
	path := "static"
	return fs.Sub(fileresolver.New(all, path), path)
}
