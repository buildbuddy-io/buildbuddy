package app

import (
	"embed"
	"io/fs"

	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
)

// NB: Include everything in bazel `embedsrcs` with `*`.
//
//go:embed *
var all embed.FS

func GetAppFS() (fs.FS, error) {
	path := "app"
	return fs.Sub(fileresolver.New(all, path), path)
}
