package fileresolver

import (
	"io/fs"
	"os"
	"strings"

	"github.com/bazelbuild/rules_go/go/runfiles"
)

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

	runfilePath, err := runfiles.Rlocation(name)
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
