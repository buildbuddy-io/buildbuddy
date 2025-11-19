package fileresolver

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/bazelbuild/rules_go/go/runfiles"
)

var fileresolverGoRlocation string

type fileResolver struct {
	bundleFS     fs.FS
	bundlePrefix string
	moduleName   string
}

// Open resolves a logical path to a local file, where the path is relative to
// the Bazel workspace root. It first consults the bundle, then Bazel runfiles.
//
// `os.NotExists` can be used to check whether the file does not exist, if an
// error is returned. The caller is responsible for closing the returned file.
func (r *fileResolver) Open(name string) (fs.File, error) {
	rlocation := strings.Builder{}
	rlocation.WriteString(r.moduleName)
	if name != "" && !strings.HasPrefix(name, "/") {
		rlocation.WriteString("/")
	}
	rlocation.WriteString(name)
	return r.OpenFromRlocation(rlocation.String())
}

// Open resolves a logical path to a local file, where the path is relative to
// the Bazel workspace root. It first consults the bundle, then Bazel runfiles.
//
// `os.NotExists` can be used to check whether the file does not exist, if an
// error is returned. The caller is responsible for closing the returned file.
func (r *fileResolver) OpenFromRlocation(rlocation string) (fs.File, error) {
	if moduleName, _, _ := strings.Cut(rlocation, "/"); r.moduleName != moduleName {
		return nil, &fs.PathError{
			Op:   "open",
			Path: rlocation,
			Err:  fmt.Errorf("invalid rlocation for file resolver with module name %s", r.moduleName),
		}
	}
	if strings.HasPrefix(rlocation, path.Join(r.moduleName, r.bundlePrefix)) {
		f, err := r.bundleFS.Open(strings.TrimPrefix(rlocation, path.Join(r.moduleName, r.bundlePrefix)))
		if err == nil {
			return f, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	runfilePath, err := runfiles.Rlocation(rlocation)
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
	// fs.FS as well as runfiles use forward slashes as path separators on all
	// platforms.
	if !strings.HasSuffix(bundleRoot, "/") {
		bundleRoot = bundleRoot + "/"
	}
	if bundleRoot == "/" {
		bundleRoot = ""
	}
	moduleName, _, _ := strings.Cut(fileresolverGoRlocation, "/")
	return &fileResolver{
		bundleFS:     bundleFS,
		bundlePrefix: bundleRoot,
		moduleName:   moduleName,
	}
}
