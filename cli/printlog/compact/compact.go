package compact

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"

	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

func printSpawnExec(s *spb.SpawnExec) error {
	b, err := protojson.MarshalOptions{Multiline: true}.Marshal(s)
	if err != nil {
		return fmt.Errorf("failed to marshal remote gRPC log entry: %s", err)
	}
	// protojson output is unstable so we need to reformat the json output
	// to make sure it's stable
	var bb bytes.Buffer
	if err = json.Indent(&bb, b, "", "  "); err != nil {
		return fmt.Errorf("failed to format json outputs: %s", err)
	}
	if _, err = bb.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to format json outputs: %s", err)
	}
	if _, err = io.Copy(os.Stdout, &bb); err != nil {
		return fmt.Errorf("failed to write to stdout: %s", err)
	}
	return nil
}

func PrintCompactExecLog(path string, sort bool) error {
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

	var spawns []*spb.SpawnExec
	for {
		s, err := slr.GetSpawnExec()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed getting spawn exec: %s", err)
		}
		if sort {
			spawns = append(spawns, s)
			continue
		}

		if err := printSpawnExec(s); err != nil {
			return err
		}
	}

	if sort {
		return StableSortExec(spawns, printSpawnExec)
	}
	return nil
}

// StableSortExec reimplements the sorting logic from Bazel's StableSort.java.
//
// https://cs.opensource.google/bazel/bazel/+/master:src/main/java/com/google/devtools/build/lib/exec/StableSort.java;l=40;drc=5d4feefed7e39b20a6c5deb5f74394abbf622a52
//
// Assuming there is no cyclic dependencies between spawns, the sort order has
// the following properties:
//   - If an output of spawn A is an input to spawn B, A sorts before B.
//   - When not constrained by the above, spawns sort in lexicographic order of their primary output path.
func StableSortExec(spawns []*spb.SpawnExec, forEachFn func(*spb.SpawnExec) error) error {
	outputToSpawns := make(map[string][]*spb.SpawnExec)
	for _, s := range spawns {
		for _, o := range s.GetActualOutputs() {
			ss, ok := outputToSpawns[o.GetPath()]
			if !ok {
				outputToSpawns[o.GetPath()] = []*spb.SpawnExec{s}
				continue
			}
			outputToSpawns[o.GetPath()] = append(ss, s)
		}
	}

	blockedBy := newSpawnSetMultiMap()
	blocking := newSpawnSetMultiMap()

	queue := make(priorityQueue, 0, len(spawns))

	for _, s := range spawns {
		blocked := false
		for _, input := range s.GetInputs() {
			for _, blocker := range outputToSpawns[input.GetPath()] {
				blockedBy.put(s, blocker)
				blocking.put(blocker, s)
				blocked = true
			}
		}
		if !blocked {
			heap.Push(&queue, s)
		}
	}

	for queue.Len() > 0 {
		s := heap.Pop(&queue).(*spb.SpawnExec)
		if err := forEachFn(s); err != nil {
			return err
		}

		for blocked := range blocking.get(s) {
			blockedBy.remove(blocked, s)
			if !blockedBy.contains(blocked) {
				heap.Push(&queue, blocked)
			}
		}
	}

	return nil
}

// priorityQueue implements container/heap.PriorityQueue
type priorityQueue []*spb.SpawnExec

func (q priorityQueue) Len() int {
	return len(q)
}

func (q priorityQueue) Less(i, j int) bool {
	return toCompareString(q[i]) < toCompareString(q[j])
}

func toCompareString(s *spb.SpawnExec) string {
	// Sort by comparing the path of the first output. We don't want the sorting to
	// rely on file hashes because we want the same action graph to be sorted in the
	// same way regardless of file contents.
	if len(s.GetListedOutputs()) > 0 {
		return "1_" + s.GetListedOutputs()[0]
	}

	// Get a proto with only stable information from this proto
	stripped := &spb.SpawnExec{
		CommandArgs:          s.GetCommandArgs(),
		EnvironmentVariables: s.GetEnvironmentVariables(),
		Platform:             s.GetPlatform(),
		Inputs:               s.GetInputs(),
		Mnemonic:             s.GetMnemonic(),
	}
	return "2_" + stripped.String()
}

func (q priorityQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *priorityQueue) Push(s any) {
	if len(*q) == 0 {
		*q = []*spb.SpawnExec{s.(*spb.SpawnExec)}
		return
	}
	*q = append(*q, s.(*spb.SpawnExec))
}

func (q *priorityQueue) Pop() any {
	old := *q
	n := len(old)
	s := old[n-1]
	old[n-1] = nil
	*q = old[0 : n-1]
	return s
}

type spawnSet map[*spb.SpawnExec]struct{}

type spawnSetMultiMap struct {
	m map[*spb.SpawnExec]spawnSet
}

func newSpawnSetMultiMap() spawnSetMultiMap {
	return spawnSetMultiMap{
		m: make(map[*spb.SpawnExec]spawnSet),
	}
}

func (ismm spawnSetMultiMap) contains(key *spb.SpawnExec) bool {
	_, ok := ismm.m[key]
	return ok
}

func (ismm spawnSetMultiMap) get(key *spb.SpawnExec) spawnSet {
	return ismm.m[key]
}

func (ismm spawnSetMultiMap) put(key *spb.SpawnExec, val *spb.SpawnExec) {
	s, ok := ismm.m[key]
	if !ok || len(s) == 0 {
		s = make(spawnSet)
		ismm.m[key] = s
	}
	s[val] = struct{}{}
}

func (ismm spawnSetMultiMap) remove(key *spb.SpawnExec, val *spb.SpawnExec) {
	if s, ok := ismm.m[key]; ok {
		delete(s, val)
		if len(s) == 0 {
			delete(ismm.m, key)
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
			return slr.reconstructSpawn(e.Spawn), nil
		default:
			log.Warnf("unknown exec log entry: %v", entry)
		}
	}
}

func (slr *SpawnLogReconstructor) reconstructSpawn(s *spb.ExecLogEntry_Spawn) *spb.SpawnExec {
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
	order, inputs := slr.reconstructInputs(s.GetInputSetId())
	_, toolInputs := slr.reconstructInputs(s.GetToolSetId())
	var spawnInputs []*spb.File
	for _, path := range order {
		file := inputs[path]
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

	return se
}

func (slr *SpawnLogReconstructor) reconstructInputs(setID int32) ([]string, map[string]*spb.File) {
	var order []string
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
				order = append(order, f.GetPath())
				inputs[f.GetPath()] = f
			}
		}
		for _, dirID := range set.GetDirectoryIds() {
			if _, ok := visited[dirID]; !ok {
				visited[dirID] = struct{}{}
				d := slr.dirs[dirID]
				for _, f := range d.files {
					order = append(order, f.GetPath())
					inputs[f.GetPath()] = f
				}
			}
		}
		for _, symlinkID := range set.GetUnresolvedSymlinkIds() {
			if _, ok := visited[symlinkID]; !ok {
				visited[symlinkID] = struct{}{}
				s := slr.symlinks[symlinkID]
				order = append(order, s.GetPath())
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
	return order, inputs
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
