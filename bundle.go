package buildbuddy

import (
	"embed"
	"io/fs"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// NB: We cannot embed "static", "bazel-out", etc here, because when this
// package is built as dependency of the enterprise package, those files
// do not exist. Instead we bundle *, which never fails, although the
// resulting filesystem may be empty.
//
//go:embed *
var all embed.FS

type aliasFS struct {
	embed.FS
	remappedPaths map[string]string
}

func (a *aliasFS) Open(name string) (fs.File, error) {
	fileAlias, ok := a.remappedPaths[name]
	if ok {
		return a.FS.Open(fileAlias)
	}
	return a.FS.Open(name)
}

func Get() (fs.FS, error) {
	afs := &aliasFS{
		FS:            all,
		remappedPaths: make(map[string]string, 0),
	}
	// This is annoying but necesary -- we bundle some binary files that
	// end up in a bazel-out/$arch/bin directory. Rather than try to read
	// them by their full name, which is $arch dependent, we glob them
	// and alias them under a path that does not contain the $arch
	// component. This path also matches what is present in the os.FS,
	// so callsites that read these files do not need to change at all.
	pathsToAlias := []string{
		"bazel-out/*/bin",
	}
	for _, p := range pathsToAlias {
		matches, err := fs.Glob(all, p)
		if err != nil {
			return nil, err
		}
		for _, match := range matches {
			fs.WalkDir(all, match, func(path string, d fs.DirEntry, err error) error {
				if path == match {
					return nil
				}
				// This basically takes a path like:
				//   'bazel-out/k8-fastbuild/bin/app/app_bundle.js'
				// and aliases it under the new name:
				//   'bin/app/app_bundle.js'
				// complaining if anything was already aliased there.
				unaliasedPath := strings.TrimPrefix(path, match+"/")
				if _, ok := afs.remappedPaths[unaliasedPath]; ok {
					log.Warningf("Named collision in bundled files: %q => %q", path, unaliasedPath)
					return nil
				}
				afs.remappedPaths[unaliasedPath] = path
				return nil
			})
		}
	}
	return afs, nil
}
