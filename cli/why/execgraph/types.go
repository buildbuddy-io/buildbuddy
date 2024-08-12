package execgraph

import (
	"encoding/hex"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"strings"
)

type File struct {
	Path   string
	Digest []byte
}

func (f File) String() string {
	return f.Path
}

func (f File) IsSourceFile() bool {
	return !strings.HasPrefix(f.Path, "bazel-out/")
}

func (f File) MappedPath() string {
	if f.IsSourceFile() {
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
	digest, err := hex.DecodeString(f.Digest.GetHash())
	if err != nil {
		panic(fmt.Sprint("invalid digest: ", f.Digest.GetHash()))
	}
	return File{
		Path:   f.Path,
		Digest: digest,
	}
}

type Spawn struct {
	Mnmemonic string
	Label     string
}

func ProtoToSpawn(s *spawn.SpawnExec) *Spawn {
	return &Spawn{
		Mnmemonic: s.Mnemonic,
	}
}

func (s Spawn) String() string {
	return s.Mnmemonic
}
