package enterprise

import (
	"embed"
	"io/fs"

	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
)

//go:embed LICENSE
var all embed.FS

func Get() fs.FS {
	return fileresolver.New(all, "enterprise")
}
