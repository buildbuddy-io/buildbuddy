package printlog

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"

	rlpb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution_log"
	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

const (
	usage = `
usage: bb print --grpc_log=PATH

Prints a human-readable representation of log files output by Bazel.

Currently supported log types:
  --grpc_log: Path to a file saved with --experimental_remote_grpc_log.
`
)

var (
	flags          = flag.NewFlagSet("print", flag.ContinueOnError)
	grpcLog        = flags.String("grpc_log", "", "gRPC log path.")
	compactExecLog = flags.String("compact_execution_log", "", "compact execution log path.")
)

func HandlePrint(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}
	if *grpcLog != "" {
		if err := printLog(*grpcLog, &rlpb.LogEntry{}); err != nil {
			return -1, err
		}
		return 0, nil
	}
	if *compactExecLog != "" {
		if err := printCompactExecLog(*compactExecLog); err != nil {
			return -1, err
		}
		return 0, nil
	}
	log.Print(usage)
	return 1, nil
}

func printCompactExecLog(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := zstd.NewReader(f)
	if err != nil {
		return err
	}
	defer r.Close()
	slr := NewSpawnLogReconstructor(r)

	for {
		sr, err := slr.GetSpawnExec()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed getting spawn exec: %s", err)
		}
		b, err := protojson.MarshalOptions{Multiline: true}.Marshal(sr)
		if err != nil {
			return fmt.Errorf("failed to marshal remote gRPC log entry: %s", err)
		}
		if _, err := os.Stdout.Write(append(b, []byte{'\n'}...)); err != nil {
			return fmt.Errorf("failed to write to stdout: %s", err)
		}
	}
}

// SpawnLogReconstructor reconstructs "compact execution log" format back to the original format.
// As of Bazel 7.1, this is the recommended way to consume the new compact format.
type SpawnLogReconstructor struct {
	input *bufio.Reader

	hashFunc string
	files    map[int32]*spb.File
	dirs     map[int32]*reconstructedDir
	symlinks map[int32]*spb.File
	sets     map[int32]*spb.ExecLogEntry_InputSet
}

type reconstructedDir struct {
	path  string
	files []*spb.File
}

func NewSpawnLogReconstructor(input io.Reader) *SpawnLogReconstructor {
	return &SpawnLogReconstructor{
		input:    bufio.NewReader(input),
		hashFunc: "",
		files:    make(map[int32]*spb.File),
		dirs:     make(map[int32]*reconstructedDir),
		symlinks: make(map[int32]*spb.File),
		sets:     make(map[int32]*spb.ExecLogEntry_InputSet),
	}
}

func (slr *SpawnLogReconstructor) GetSpawnExec() (*spb.SpawnExec, error) {
	entry := &spb.ExecLogEntry{}
	for {
		err := protodelim.UnmarshalFrom(slr.input, entry)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read execution log entry: %s", err)
		}

		switch e := entry.GetType().(type) {
		case *spb.ExecLogEntry_Invocation_:
			slr.hashFunc = e.Invocation.GetHashFunctionName()
		case *spb.ExecLogEntry_File_:
			slr.files[entry.GetId()] = reconstructFile(nil, e.File)
		case *spb.ExecLogEntry_Directory_:
			slr.dirs[entry.GetId()] = reconstructDir(e.Directory)
		case *spb.ExecLogEntry_UnresolvedSymlink_:
			slr.symlinks[entry.GetId()] = reconstructSymlink(e.UnresolvedSymlink)
		case *spb.ExecLogEntry_InputSet_:
			slr.sets[entry.GetId()] = e.InputSet
		case *spb.ExecLogEntry_Spawn_:
			return slr.reconstructSpawn(e.Spawn)
		default:
			log.Warnf("unknown exec log entry: %v", entry)
		}
	}
}

func (slr *SpawnLogReconstructor) reconstructSpawn(s *spb.ExecLogEntry_Spawn) (*spb.SpawnExec, error) {
	se := &spb.SpawnExec{
		CommandArgs:          s.GetArgs(),
		EnvironmentVariables: s.GetEnvVars(),
		TargetLabel:          s.GetTargetLabel(),
		Mnemonic:             s.GetMnemonic(),
		ExitCode:             s.GetExitCode(),
		Status:               s.GetStatus(),
		Runner:               s.GetRunner(),
		CacheHit:             s.GetCacheHit(),
		Remotable:            s.GetRemotable(),
		Cacheable:            s.GetCacheable(),
		RemoteCacheable:      s.GetRemoteCacheable(),
		TimeoutMillis:        s.GetTimeoutMillis(),
		Metrics:              s.GetMetrics(),
		Platform:             s.GetPlatform(),
		Digest:               s.GetDigest(),
	}

	// Handle inputs
	inputs := slr.reconstructInputs(s.GetInputSetId())
	toolInputs := slr.reconstructInputs(s.GetToolSetId())
	var spawnInputs []*spb.File
	for path, file := range inputs {
		if _, ok := toolInputs[path]; ok {
			file.IsTool = true
		}
		spawnInputs = append(spawnInputs, file)
	}
	se.Inputs = spawnInputs

	// Handle outputs
	var listedOutputs []string
	var actualOutputs []*spb.File
	for _, output := range s.GetOutputs() {
		switch o := output.GetType().(type) {
		case *spb.ExecLogEntry_Output_FileId:
			f := slr.files[o.FileId]
			listedOutputs = append(listedOutputs, f.GetPath())
			actualOutputs = append(actualOutputs, f)
		case *spb.ExecLogEntry_Output_DirectoryId:
			d := slr.dirs[o.DirectoryId]
			listedOutputs = append(listedOutputs, d.path)
			actualOutputs = append(actualOutputs, d.files...)
		case *spb.ExecLogEntry_Output_UnresolvedSymlinkId:
			symlink := slr.symlinks[o.UnresolvedSymlinkId]
			listedOutputs = append(listedOutputs, symlink.GetPath())
			actualOutputs = append(actualOutputs, symlink)
		case *spb.ExecLogEntry_Output_InvalidOutputPath:
			listedOutputs = append(listedOutputs, o.InvalidOutputPath)
		default:
			log.Warnf("unknown output type: %v", output)
		}
	}
	se.ListedOutputs = listedOutputs
	se.ActualOutputs = actualOutputs

	return se, nil
}

func (slr *SpawnLogReconstructor) reconstructInputs(setID int32) map[string]*spb.File {
	inputs := make(map[string]*spb.File)
	setsToVisit := []int32{}
	visited := make(map[int32]struct{})
	if setID != 0 {
		setsToVisit = append(setsToVisit, setID)
		visited[setID] = struct{}{}
	}
	for len(setsToVisit) > 0 {
		currentID := setsToVisit[0]
		setsToVisit = setsToVisit[1:]
		set := slr.sets[currentID]

		for _, fileID := range set.GetFileIds() {
			if _, ok := visited[fileID]; !ok {
				visited[fileID] = struct{}{}
				f := slr.files[fileID]
				inputs[f.GetPath()] = f
			}
		}
		for _, dirID := range set.GetDirectoryIds() {
			if _, ok := visited[dirID]; !ok {
				visited[dirID] = struct{}{}
				d := slr.dirs[dirID]
				for _, f := range d.files {
					inputs[f.GetPath()] = f
				}
			}
		}
		for _, symlinkID := range set.GetUnresolvedSymlinkIds() {
			if _, ok := visited[symlinkID]; !ok {
				visited[symlinkID] = struct{}{}
				s := slr.symlinks[symlinkID]
				inputs[s.GetPath()] = s
			}
		}
		for _, setID := range set.GetTransitiveSetIds() {
			if _, ok := visited[setID]; !ok {
				visited[setID] = struct{}{}
				setsToVisit = append(setsToVisit, setID)
			}
		}
	}
	return inputs
}

func reconstructDir(d *spb.ExecLogEntry_Directory) *reconstructedDir {
	filesInDir := make([]*spb.File, 0, len(d.GetFiles()))
	for _, file := range d.GetFiles() {
		filesInDir = append(filesInDir, reconstructFile(d, file))
	}
	return &reconstructedDir{
		path:  d.GetPath(),
		files: filesInDir,
	}
}

func reconstructFile(parentDir *spb.ExecLogEntry_Directory, file *spb.ExecLogEntry_File) *spb.File {
	f := &spb.File{Digest: file.GetDigest()}
	if parentDir != nil {
		f.Path = path.Join(parentDir.GetPath(), file.GetPath())
	} else {
		f.Path = file.GetPath()
	}
	return f
}

func reconstructSymlink(s *spb.ExecLogEntry_UnresolvedSymlink) *spb.File {
	return &spb.File{
		Path:              s.GetPath(),
		SymlinkTargetPath: s.GetTargetPath(),
	}
}

func printLog(path string, m proto.Message) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := copyUnmarshaled(os.Stdout, f, m); err != nil {
		return err
	}
	return nil
}

func copyUnmarshaled(w io.Writer, grpcLog io.Reader, m proto.Message) error {
	br := bufio.NewReader(grpcLog)
	for {
		err := protodelim.UnmarshalFrom(br, m)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read LogEntry: %s", err)
		}
		b, err := protojson.MarshalOptions{Multiline: true}.Marshal(m)
		if err != nil {
			return fmt.Errorf("failed to marshal remote gRPC log entry: %s", err)
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		if _, err := w.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
}
