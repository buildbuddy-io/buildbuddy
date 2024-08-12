package execgraph

import (
	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"strings"
)

type File struct {
	Path   string
	Digest string
}

func (f File) isSourceFile() bool {
	return !strings.HasPrefix(f.Path, "bazel-out/")
}

func (f File) mappedPath() string {
	if f.isSourceFile() {
		return f.Path
	}
	secondSlash := strings.Index(f.Path[len("bazel-out/"):], "/")
	if secondSlash == -1 {
		panic("invalid bazel-out path")
	}
	return "bazel-out/cfg/" + f.Path[len("bazel-out/")+secondSlash+1:]
}

func FileHash(f File) string {
	return f.Path
}

func ProtoToFile(f *spawn.File) File {
	return File{
		Path:   f.Path,
		Digest: f.Digest.GetHash(),
	}
}

type Spawn struct {
}
