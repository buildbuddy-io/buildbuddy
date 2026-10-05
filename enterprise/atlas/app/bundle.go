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

// GetAppFS returns the built frontend: app_bundle/, style.css and sha.sum.
// Release builds embed it; in fastbuild it comes from runfiles.
func GetAppFS() (fs.FS, error) {
	path := "enterprise/atlas/app"
	return fs.Sub(fileresolver.New(all, path), path)
}
