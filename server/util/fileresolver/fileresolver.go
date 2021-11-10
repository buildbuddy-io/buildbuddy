package fileresolver

import (
	"embed"
	"io/fs"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

type aliasFS struct {
	FS            embed.FS
	remappedPaths map[string]string
}

// GetBundleFS returns an FS that reads files directly from the given embed.FS,
// aliasing any paths like "bazel-out/$arch/bin/$path" to "bin/$path" to provide
// a simpler interface for dealing with architecture-specific paths.
func GetBundleFS(bundle embed.FS) (fs.FS, error) {
	afs := &aliasFS{
		FS:            bundle,
		remappedPaths: make(map[string]string, 0),
	}
	// This is annoying but necessary -- we bundle some binary files that
	// end up in a bazel-out/$arch/bin directory. Rather than try to read
	// them by their full name, which is $arch dependent, we glob them
	// and alias them under a path that does not contain the $arch
	// component. This path also matches what is present in the os.FS,
	// so callsites that read these files do not need to change at all.
	pathsToAlias := []string{
		"bazel-out/*/bin",
	}
	for _, p := range pathsToAlias {
		matches, err := fs.Glob(bundle, p)
		if err != nil {
			return nil, err
		}
		for _, match := range matches {
			fs.WalkDir(bundle, match, func(path string, d fs.DirEntry, err error) error {
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

func (a *aliasFS) Open(name string) (fs.File, error) {
	fileAlias, ok := a.remappedPaths[name]
	if ok {
		return a.FS.Open(fileAlias)
	}
	return a.FS.Open(name)
}

type fileResolver struct {
	bundleFS     fs.FS
	bundlePrefix string
}

// Open resolves a logical path to a local file, where the path is relative to
// the Bazel workspace root. It first consults the bundle, then Bazel runfiles.
//
// `os.NotExists` can be used to check whether the file does not exist, if an
// error is returned. The caller is responsible for closing the returned file.
func (r *fileResolver) Open(name string) (fs.File, error) {
	if strings.HasPrefix(name, r.bundlePrefix) {
		f, err := r.bundleFS.Open(strings.TrimPrefix(name, r.bundlePrefix))
		if err == nil {
			return f, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	runfilePath, err := bazelgo.Runfile(name)
	if err != nil {
		return nil, err
	}
	return os.Open(runfilePath)
}

// New returns an FS that looks up files first from the bundle, then from
// bazel runfiles.
//
// The bundleRoot specifies the parent directory of the bundle FS, relative
// to the workspace root. For example, if the bundle is located at
// foo/bar/bundle.go, bundleRoot should be set to foo/bar. Files will only
// be looked up from the bundle if they start with this prefix. We will always
// consult runfiles regardless of whether they begin with this prefix.
func New(bundleFS fs.FS, bundleRoot string) fs.FS {
	bundlePrefix := ""
	if bundleRoot != "" && !strings.HasSuffix(bundleRoot, string(os.PathSeparator)) {
		bundlePrefix = bundleRoot + string(os.PathSeparator)
	}
	return &fileResolver{
		bundleFS:     bundleFS,
		bundlePrefix: bundlePrefix,
	}
}
