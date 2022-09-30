package sidecar_bundle

import (
	"embed"
	"io/fs"
)

//go:embed *
var embedFS embed.FS

func Open() (fs.File, error) {
	return embedFS.Open("sidecar-binary")
}
