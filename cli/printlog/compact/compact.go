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
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"

	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

func printProtoMsg[M proto.Message](m M) error {
	b, err := protojson.MarshalOptions{Multiline: true}.Marshal(m)
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

func PrintCompactExecLog(path string, raw bool, sort bool) error {
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
	reader := bufio.NewReader(r)

	if raw {
		entry := &spb.ExecLogEntry{}
		for {
			err := protodelim.UnmarshalOptions{MaxSize: -1}.UnmarshalFrom(reader, entry)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("failed to read execution log entry: %s", err)
			}

			if err := printProtoMsg(entry); err != nil {
				return err
			}
		}
	}

	slr := NewSpawnLogReconstructor(reader)
	var spawns []*spb.SpawnExec
	for {
		s, err := slr.GetSpawnExec()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed getting spawn exec: %s", err)
		}
		if s == nil { // Skip non-spawn entries after processing
			continue
		}
		if sort {
			spawns = append(spawns, s)
			continue
		}

		if err := printProtoMsg(s); err != nil {
			return err
		}
	}

	if sort {
		return StableSortExec(spawns, printProtoMsg)
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

	queue := make(PriorityQueue, 0, len(spawns))

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

// RelPath calculates the relative path from base to target.
// It uses filepath.Rel for potentially better cross-platform handling,
// although Bazel paths typically use '/'.
func RelPath(basepath, targpath string) (string, error) {
	// Ensure forward slashes for consistency before using filepath.Rel,
	// as filepath.Rel might behave differently based on OS separators.
	basepath = filepath.ToSlash(basepath)
	targpath = filepath.ToSlash(targpath)
	return filepath.Rel(basepath, targpath)
}

// priorityQueue implements container/heap.PriorityQueue
type PriorityQueue []*spb.SpawnExec

func (q PriorityQueue) Len() int {
	return len(q)
}

func (q PriorityQueue) Less(i, j int) bool {
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
	// Sort inputs by path for stability when comparing stripped proto strings
	inputFiles := make([]*spb.File, len(stripped.Inputs))
	copy(inputFiles, stripped.Inputs)

	sort.Slice(inputFiles, func(i, j int) bool {
		return inputFiles[i].GetPath() < inputFiles[j].GetPath()
	})
	stripped.Inputs = inputFiles

	return "2_" + stripped.String()
}

func (q PriorityQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *PriorityQueue) Push(s any) {
	item := s.(*spb.SpawnExec)
	*q = append(*q, item)
}

func (q *PriorityQueue) Pop() any {
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
	input protodelim.Reader

	// Invocation info
	hashFunc             string
	workspaceRunfilesDir string
	siblingRepoLayout    bool

	// Stored entries by ID
	files            map[uint32]*spb.File
	dirs             map[uint32]*reconstructedDir
	symlinks         map[uint32]*spb.File
	sets             map[uint32]*spb.ExecLogEntry_InputSet
	symlinkEntrySets map[uint32]*spb.ExecLogEntry_SymlinkEntrySet
	runfilesTrees    map[uint32]*reconstructedDir // Store reconstructed RunfilesTree as a directory
}

type reconstructedDir struct {
	path  string
	files []*spb.File
}

// SymlinkConsumer is a callback function to process reconstructed symlink entries.
// It receives the path relative to the runfiles tree root and the reconstructed target files. Return error to stop processing.
type SymlinkConsumer func(rootRelativePath string, targetFiles []*spb.File) error

func NewSpawnLogReconstructor(input protodelim.Reader) *SpawnLogReconstructor {
	return &SpawnLogReconstructor{
		input:            input,
		hashFunc:         "", // Will be set by Invocation message
		files:            make(map[uint32]*spb.File),
		dirs:             make(map[uint32]*reconstructedDir),
		symlinks:         make(map[uint32]*spb.File),
		sets:             make(map[uint32]*spb.ExecLogEntry_InputSet),
		symlinkEntrySets: make(map[uint32]*spb.ExecLogEntry_SymlinkEntrySet),
		runfilesTrees:    make(map[uint32]*reconstructedDir),
	}
}

// GetSpawnExec reads the next log entry. If it's a Spawn, it reconstructs and
// returns it. If it's another entry type, it stores it for future reference and
// returns nil. Returns io.EOF when the log ends.
func (slr *SpawnLogReconstructor) GetSpawnExec() (*spb.SpawnExec, error) {
	entry := &spb.ExecLogEntry{}
	for {
		err := protodelim.UnmarshalOptions{MaxSize: -1}.UnmarshalFrom(slr.input, entry)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read execution log entry: %s", err)
		}

		switch e := entry.GetType().(type) {
		case *spb.ExecLogEntry_Invocation_:
			slr.hashFunc = e.Invocation.GetHashFunctionName()
			slr.workspaceRunfilesDir = e.Invocation.GetWorkspaceRunfilesDirectory()
			slr.siblingRepoLayout = e.Invocation.GetSiblingRepositoryLayout()
		case *spb.ExecLogEntry_File_:
			// Store file, applying hash function if digest exists
			fileEntry := reconstructFile(nil, e.File)
			if fileEntry.GetDigest() != nil && fileEntry.GetDigest().GetHashFunctionName() == "" {
				fileEntry.Digest.HashFunctionName = slr.hashFunc
			}
			slr.files[entry.GetId()] = fileEntry
		case *spb.ExecLogEntry_Directory_:
			// Store reconstructed directory, applying hash function to contained files
			dirEntry := reconstructDir(e.Directory)
			for _, f := range dirEntry.files {
				if f.GetDigest() != nil && f.GetDigest().GetHashFunctionName() == "" {
					f.Digest.HashFunctionName = slr.hashFunc
				}
			}
			slr.dirs[entry.GetId()] = dirEntry
		case *spb.ExecLogEntry_UnresolvedSymlink_:
			slr.symlinks[entry.GetId()] = reconstructSymlink(e.UnresolvedSymlink)
		case *spb.ExecLogEntry_InputSet_:
			slr.sets[entry.GetId()] = e.InputSet
		case *spb.ExecLogEntry_Spawn_:
			// Reconstruct and return the SpawnExec
			spawnExec := slr.reconstructSpawn(e.Spawn)
			// Apply hash function to action digest if present
			if spawnExec.GetDigest() != nil && spawnExec.GetDigest().GetHashFunctionName() == "" {
				spawnExec.Digest.HashFunctionName = slr.hashFunc
			}
			return spawnExec, nil // Return the reconstructed spawn
		case *spb.ExecLogEntry_SymlinkAction_:
			// Symlink actions are not represented in the expanded SpawnExec format.
			log.Debugf("Skipping SymlinkAction entry ID %d", entry.GetId())
		case *spb.ExecLogEntry_SymlinkEntrySet_:
			// Store the set for potential use by RunfilesTree reconstruction
			slr.symlinkEntrySets[entry.GetId()] = e.SymlinkEntrySet
			log.Debugf("Stored SymlinkEntrySet entry ID %d", entry.GetId())
		case *spb.ExecLogEntry_RunfilesTree_:
			// Reconstruct the RunfilesTree into a directory representation
			reconstructed, err := slr.reconstructRunfilesDir(e.RunfilesTree)
			if err != nil {
				log.Warnf("Failed to reconstruct RunfilesTree entry ID %d: %v. Skipping.", entry.GetId(), err)
				continue // Skip storing if reconstruction fails
			}
			slr.runfilesTrees[entry.GetId()] = reconstructed
			log.Debugf("Reconstructed and stored RunfilesTree entry ID %d as directory %s", entry.GetId(), reconstructed.path)
		default:
			log.Warnf("Unknown exec log entry type: %T (ID: %d)", entry.GetType(), entry.GetId())
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
		Digest:               s.GetDigest(), // Hash function name applied in GetSpawnExec
	}

	// Handle inputs (including deprecated fields)
	order, inputs := slr.reconstructInputs(s.GetInputSetId())
	_, toolInputs := slr.reconstructInputs(s.GetToolSetId())
	var spawnInputs []*spb.File
	visitedInputPaths := make(map[string]struct{}) // Avoid duplicates from different IDs mapping to same effective input
	for _, path := range order {
		if _, visited := visitedInputPaths[path]; visited {
			continue
		}
		file := proto.Clone(inputs[path]).(*spb.File) // Clone to safely modify IsTool
		if _, ok := toolInputs[path]; ok {
			file.IsTool = true
		}
		spawnInputs = append(spawnInputs, file)
		visitedInputPaths[path] = struct{}{}
	}
	se.Inputs = spawnInputs

	// Handle outputs (including deprecated fields)
	var listedOutputs []string
	var actualOutputs []*spb.File
	outputPaths := make(map[string]struct{}) // Track paths added to avoid duplicates

	addOutput := func(f *spb.File) {
		if _, exists := outputPaths[f.GetPath()]; !exists {
			actualOutputs = append(actualOutputs, f)
			outputPaths[f.GetPath()] = struct{}{}
		}
	}

	for _, output := range s.GetOutputs() {
		switch o := output.GetType().(type) {
		// Modern field first
		case *spb.ExecLogEntry_Output_OutputId:
			listedPath := ""
			if f, ok := slr.files[o.OutputId]; ok {
				listedPath = f.GetPath()
				addOutput(f)
			} else if d, ok := slr.dirs[o.OutputId]; ok {
				listedPath = d.path
				for _, df := range d.files {
					addOutput(df)
				}
			} else if symlink, ok := slr.symlinks[o.OutputId]; ok {
				listedPath = symlink.GetPath()
				addOutput(symlink)
			} else if rft, ok := slr.runfilesTrees[o.OutputId]; ok {
				listedPath = rft.path
				for _, rfFile := range rft.files {
					addOutput(rfFile)
				}
			} else {
				log.Warnf("Output ID %d referenced in spawn not found in stored entries.", o.OutputId)
				continue // Skip if ID is invalid
			}
			listedOutputs = append(listedOutputs, listedPath)

		// Deprecated fields (handle if modern field wasn't used)
		case *spb.ExecLogEntry_Output_FileId:
			if f, ok := slr.files[o.FileId]; ok {
				listedOutputs = append(listedOutputs, f.GetPath())
				addOutput(f)
			} else {
				log.Warnf("Deprecated Output FileId %d referenced in spawn not found.", o.FileId)
			}
		case *spb.ExecLogEntry_Output_DirectoryId:
			if d, ok := slr.dirs[o.DirectoryId]; ok {
				listedOutputs = append(listedOutputs, d.path)
				for _, df := range d.files {
					addOutput(df)
				}
			} else {
				// Check if it's a RunfilesTree (handled similar to Directory for outputs)
				if rft, ok := slr.runfilesTrees[o.DirectoryId]; ok {
					listedOutputs = append(listedOutputs, rft.path)
					for _, rfFile := range rft.files {
						addOutput(rfFile)
					}
				} else {
					log.Warnf("Deprecated Output DirectoryId %d referenced in spawn not found.", o.DirectoryId)
				}
			}
		case *spb.ExecLogEntry_Output_UnresolvedSymlinkId:
			if symlink, ok := slr.symlinks[o.UnresolvedSymlinkId]; ok {
				listedOutputs = append(listedOutputs, symlink.GetPath())
				addOutput(symlink)
			} else {
				log.Warnf("Deprecated Output UnresolvedSymlinkId %d referenced in spawn not found.", o.UnresolvedSymlinkId)
			}

		// Invalid path
		case *spb.ExecLogEntry_Output_InvalidOutputPath:
			listedOutputs = append(listedOutputs, o.InvalidOutputPath)

		default:
			log.Warnf("Unknown output type in spawn: %T", output.GetType())
		}
	}
	se.ListedOutputs = listedOutputs
	se.ActualOutputs = actualOutputs

	return se
}

func (slr *SpawnLogReconstructor) reconstructInputs(setID uint32) ([]string, map[string]*spb.File) {
	// This function retains support for deprecated fields `file_ids`, etc.
	order := []string{}
	inputs := make(map[string]*spb.File)
	setsToVisit := []uint32{} // Acts like a stack for the first pass
	visitedSets := make(map[uint32]struct{})
	visitedInputs := make(map[uint32]struct{}) // Track individual input IDs

	if setID != 0 {
		setsToVisit = append(setsToVisit, setID)
		visitedSets[setID] = struct{}{}
	}

	postOrderSets := []uint32{}                    // To store the set IDs in post-order
	processedForPostOrder := make(map[uint32]bool) // Track sets added to post-order list

	// Iterative approach similar to Java's first pass (building post-order list)
	for len(setsToVisit) > 0 {
		currentSetID := setsToVisit[len(setsToVisit)-1] // Peek top of stack

		set, ok := slr.sets[currentSetID]
		// Handle missing set gracefully during post-order construction
		if !ok {
			if !processedForPostOrder[currentSetID] { // Avoid adding multiple times if referenced multiple times
				log.Warnf("InputSet ID %d referenced but not found during post-order build.", currentSetID)
				postOrderSets = append(postOrderSets, currentSetID) // Add to post-order to maintain structure
				processedForPostOrder[currentSetID] = true
			}
			setsToVisit = setsToVisit[:len(setsToVisit)-1] // Pop
			continue
		}

		// Check if already processed for post-order
		if processedForPostOrder[currentSetID] {
			setsToVisit = setsToVisit[:len(setsToVisit)-1] // Pop
			continue
		}

		// Check transitive dependencies
		allTransitiveProcessed := true
		// Process in reverse to push dependencies onto stack in the correct order for post-order
		for i := len(set.GetTransitiveSetIds()) - 1; i >= 0; i-- {
			transitiveSetID := set.GetTransitiveSetIds()[i]
			if _, visited := visitedSets[transitiveSetID]; !visited {
				allTransitiveProcessed = false
				visitedSets[transitiveSetID] = struct{}{}
				setsToVisit = append(setsToVisit, transitiveSetID) // Push dependency
			} else if !processedForPostOrder[transitiveSetID] {
				// Dependency visited but not yet added to post-order list
				allTransitiveProcessed = false
			}
		}

		if allTransitiveProcessed {
			// All dependencies processed, add this set to post-order list
			postOrderSets = append(postOrderSets, currentSetID)
			processedForPostOrder[currentSetID] = true
			setsToVisit = setsToVisit[:len(setsToVisit)-1] // Pop
		}
	}

	// Second pass: Process direct inputs in the calculated post-order
	for _, currentSetID := range postOrderSets {
		set, ok := slr.sets[currentSetID]
		if !ok {
			// Already warned during post-order build, skip processing direct inputs
			continue
		}

		// --- Handle Modern `input_ids` Field ---
		for _, inputID := range set.GetInputIds() {
			if _, visited := visitedInputs[inputID]; visited {
				continue
			}
			visitedInputs[inputID] = struct{}{}

			if f, ok := slr.files[inputID]; ok {
				if _, exists := inputs[f.GetPath()]; !exists { // Avoid duplicates in map/order
					order = append(order, f.GetPath())
					inputs[f.GetPath()] = f
				}
			} else if d, ok := slr.dirs[inputID]; ok {
				// Add directory contents
				for _, df := range d.files {
					if _, exists := inputs[df.GetPath()]; !exists {
						order = append(order, df.GetPath())
						inputs[df.GetPath()] = df
					}
				}
			} else if symlink, ok := slr.symlinks[inputID]; ok {
				if _, exists := inputs[symlink.GetPath()]; !exists {
					order = append(order, symlink.GetPath())
					inputs[symlink.GetPath()] = symlink
				}
			} else if rft, ok := slr.runfilesTrees[inputID]; ok {
				// If input is a RunfilesTree, add all its files
				for _, rfFile := range rft.files {
					if _, exists := inputs[rfFile.GetPath()]; !exists {
						order = append(order, rfFile.GetPath())
						inputs[rfFile.GetPath()] = rfFile
					}
				}
			} else if _, ok := slr.symlinkEntrySets[inputID]; ok {
				// InputSet can contain SymlinkEntrySet, but it doesn't directly contribute files here.
				// RunfilesTree reconstruction handles these.
			} else if _, ok := slr.sets[inputID]; ok {
				// InputSet can contain InputSet, handled by transitive logic.
			} else {
				log.Debugf("Input ID %d in InputSet %d is not a File, Directory, Symlink, RunfilesTree or Set. Skipping for direct input list.", inputID, currentSetID)
			}
		}

		// --- Handle Deprecated Fields (if modern field was empty or for back compat) ---
		// Process regardless for safety, respecting visitedInputs.
		for _, fileID := range set.GetFileIds() {
			if _, visited := visitedInputs[fileID]; visited {
				continue
			}
			visitedInputs[fileID] = struct{}{}
			if f, ok := slr.files[fileID]; ok {
				if _, exists := inputs[f.GetPath()]; !exists {
					order = append(order, f.GetPath())
					inputs[f.GetPath()] = f
				}
			}
		}
		for _, dirID := range set.GetDirectoryIds() {
			if _, visited := visitedInputs[dirID]; visited {
				continue
			}
			visitedInputs[dirID] = struct{}{}
			if d, ok := slr.dirs[dirID]; ok {
				for _, df := range d.files {
					if _, exists := inputs[df.GetPath()]; !exists {
						order = append(order, df.GetPath())
						inputs[df.GetPath()] = df
					}
				}
			} else {
				// Check if it's a RunfilesTree (handled similar to Directory for inputs)
				if rft, ok := slr.runfilesTrees[dirID]; ok {
					for _, rfFile := range rft.files {
						if _, exists := inputs[rfFile.GetPath()]; !exists {
							order = append(order, rfFile.GetPath())
							inputs[rfFile.GetPath()] = rfFile
						}
					}
				}
			}
		}
		for _, symlinkID := range set.GetUnresolvedSymlinkIds() {
			if _, visited := visitedInputs[symlinkID]; visited {
				continue
			}
			visitedInputs[symlinkID] = struct{}{}
			if s, ok := slr.symlinks[symlinkID]; ok {
				if _, exists := inputs[s.GetPath()]; !exists {
					order = append(order, s.GetPath())
					inputs[s.GetPath()] = s
				}
			}
		}
	}

	return order, inputs
}

// reconstructRunfilesDir reconstructs the directory structure represented by a RunfilesTree entry.
func (slr *SpawnLogReconstructor) reconstructRunfilesDir(runfilesTree *spb.ExecLogEntry_RunfilesTree) (*reconstructedDir, error) {
	// Use a slice to maintain insertion order (for correct overrides) and a map for quick lookups.
	orderedPaths := []string{}
	runfiles := make(map[string]*spb.File)
	hasWorkspaceRunfilesDirectory := runfilesTree.GetLegacyExternalRunfiles()

	// Helper to add a file, respecting override order.
	addFile := func(file *spb.File) {
		if _, exists := runfiles[file.GetPath()]; !exists {
			orderedPaths = append(orderedPaths, file.GetPath())
		}
		runfiles[file.GetPath()] = file
	}

	// 1. Symlinks
	visitedSymlinkSets := make(map[uint32]struct{})
	err := slr.reconstructSymlinkEntries(
		runfilesTree,
		runfilesTree.GetSymlinksId(),
		false, // rootSymlinks = false
		func(rootRelativePath string, targetFiles []*spb.File) error {
			if strings.HasPrefix(rootRelativePath, slr.workspaceRunfilesDir+"/") {
				hasWorkspaceRunfilesDirectory = true
			}
			for _, f := range targetFiles {
				addFile(f)
			}
			return nil
		},
		visitedSymlinkSets,
	)
	if err != nil {
		return nil, fmt.Errorf("processing runfiles symlinks: %w", err)
	}

	// 2. Artifacts at canonical locations
	artifactOrder, artifacts := slr.reconstructInputs(runfilesTree.GetInputSetId())
	for _, execPath := range artifactOrder {
		file := artifacts[execPath]
		// Determine if the original exec path implies the workspace directory exists
		rPathInfo, err := extractRunfilesPath(execPath, slr.siblingRepoLayout)
		if err != nil {
			log.Warnf("Could not extract runfiles path info from artifact exec path '%s' in runfiles tree %s: %v", execPath, runfilesTree.GetPath(), err)
			// Attempt to add anyway, might be missing info but better than nothing
		} else if rPathInfo.repo == "" { // Artifact in the main repo implies workspace dir presence
			hasWorkspaceRunfilesDirectory = true
		}

		runfilesPaths, err := slr.getRunfilesPaths(execPath, runfilesTree.GetLegacyExternalRunfiles())
		if err != nil {
			log.Warnf("Could not determine runfiles paths for artifact exec path '%s' in runfiles tree %s: %v", execPath, runfilesTree.GetPath(), err)
			continue // Skip this artifact if paths can't be determined
		}
		for _, relativePath := range runfilesPaths {
			newFile := proto.Clone(file).(*spb.File)
			newFile.Path = path.Join(runfilesTree.GetPath(), relativePath)
			// Make sure digest hash function is set
			if newFile.Digest != nil && newFile.Digest.HashFunctionName == "" {
				newFile.Digest.HashFunctionName = slr.hashFunc
			}
			addFile(newFile)
		}
	}

	// 3. Empty files
	for _, emptyFileRelative := range runfilesTree.GetEmptyFiles() {
		var newPath string
		if strings.HasPrefix(emptyFileRelative, "../") {
			// Relative to sibling directory (e.g., ../repo/foo)
			// Note: Go's path.Join cleans "../", need careful construction
			if len(runfilesTree.GetPath()) > 0 {
				// Base path + relative path without "../"
				newPath = path.Join(path.Dir(runfilesTree.GetPath()), emptyFileRelative[3:])
			} else {
				// If runfiles tree path is empty (unlikely), just use relative path
				newPath = emptyFileRelative[3:]
			}
		} else {
			// Relative to workspace directory inside runfiles tree (e.g., pkg/foo)
			newPath = path.Join(runfilesTree.GetPath(), slr.workspaceRunfilesDir, emptyFileRelative)
		}
		// Empty files don't inherently create the workspace dir if it wouldn't exist otherwise.
		addFile(&spb.File{Path: newPath}) // Digest is implicitly nil (empty file)
	}

	// 4. Root symlinks
	visitedSymlinkSets = make(map[uint32]struct{}) // Reset visited sets for root symlinks
	err = slr.reconstructSymlinkEntries(
		runfilesTree,
		runfilesTree.GetRootSymlinksId(),
		true, // rootSymlinks = true
		func(rootRelativePath string, targetFiles []*spb.File) error {
			if strings.HasPrefix(rootRelativePath, slr.workspaceRunfilesDir+"/") {
				hasWorkspaceRunfilesDirectory = true
			}
			for _, f := range targetFiles {
				addFile(f)
			}
			return nil
		},
		visitedSymlinkSets,
	)
	if err != nil {
		return nil, fmt.Errorf("processing runfiles root symlinks: %w", err)
	}

	// 5. Repo mapping manifest
	if runfilesTree.GetRepoMappingManifest() != nil && runfilesTree.GetRepoMappingManifest().GetDigest() != nil {
		repoMappingPath := path.Join(runfilesTree.GetPath(), "_repo_mapping")
		repoMappingFile := &spb.File{
			Path:   repoMappingPath,
			Digest: proto.Clone(runfilesTree.GetRepoMappingManifest().GetDigest()).(*spb.Digest),
		}
		// Ensure hash function name is set
		if repoMappingFile.Digest.GetHashFunctionName() == "" {
			repoMappingFile.Digest.HashFunctionName = slr.hashFunc
		}
		addFile(repoMappingFile)
		// Presence of repo mapping doesn't imply workspace dir existence itself.
	}

	// 6. .runfile marker
	if !runfilesTree.GetLegacyExternalRunfiles() && !hasWorkspaceRunfilesDirectory && slr.workspaceRunfilesDir != "" {
		dotRunfilePath := path.Join(runfilesTree.GetPath(), slr.workspaceRunfilesDir, ".runfile")
		addFile(&spb.File{Path: dotRunfilePath}) // Implicitly empty
	}

	// Collect final files in the correct order
	finalFiles := make([]*spb.File, 0, len(orderedPaths))
	for _, p := range orderedPaths {
		finalFiles = append(finalFiles, runfiles[p])
	}

	return &reconstructedDir{
		path:  runfilesTree.GetPath(),
		files: finalFiles,
	}, nil
}

// reconstructSymlinkEntries traverses a SymlinkEntrySet (transitively, in post-order) and calls the consumer for each direct entry.
// This function is prepared for use by RunfilesTree reconstruction.
func (slr *SpawnLogReconstructor) reconstructSymlinkEntries(
	runfilesTree *spb.ExecLogEntry_RunfilesTree, // Context needed for paths
	symlinkEntrySetID uint32,
	rootSymlinks bool, // Determines relative path calculation
	consumer SymlinkConsumer,
	visitedSetIDs map[uint32]struct{}, // Track visited sets to avoid cycles and redundant work
) error {

	// Iterative post-order traversal (similar to reconstructInputs)
	setsToVisit := []uint32{}                   // Stack for traversal
	setsInPostOrder := []uint32{}               // Stores IDs in post-order
	locallyVisited := make(map[uint32]struct{}) // Track visits within *this* call

	// First pass: Build the post-order list
	if symlinkEntrySetID != 0 {
		// Check global visited set first before starting traversal
		if _, visited := visitedSetIDs[symlinkEntrySetID]; !visited {
			setsToVisit = append(setsToVisit, symlinkEntrySetID)
			locallyVisited[symlinkEntrySetID] = struct{}{}
		}
	}

	processedForPostOrder := make(map[uint32]bool) // Track sets added to post-order list

	for len(setsToVisit) > 0 {
		currentSetID := setsToVisit[len(setsToVisit)-1] // Peek top

		// Check if already processed for post-order in *this* traversal
		if processedForPostOrder[currentSetID] {
			setsToVisit = setsToVisit[:len(setsToVisit)-1] // Pop
			continue
		}

		set, ok := slr.symlinkEntrySets[currentSetID]
		if !ok {
			// Log warning but continue traversal if possible
			log.Warnf("SymlinkEntrySet ID %d referenced but not found during traversal.", currentSetID)
			processedForPostOrder[currentSetID] = true // Mark as processed to avoid infinite loop
			setsInPostOrder = append(setsInPostOrder, currentSetID)
			setsToVisit = setsToVisit[:len(setsToVisit)-1] // Pop
			continue
		}

		// Check transitive dependencies
		allTransitiveProcessed := true
		// Iterate reverse to push dependencies onto stack in intended order
		for i := len(set.GetTransitiveSetIds()) - 1; i >= 0; i-- {
			transitiveSetID := set.GetTransitiveSetIds()[i]
			// Check global visited set first (passed in)
			if _, globalVisited := visitedSetIDs[transitiveSetID]; globalVisited {
				continue
			}
			// Check if visited/processed within this specific traversal run
			if _, localVisited := locallyVisited[transitiveSetID]; !localVisited {
				allTransitiveProcessed = false
				locallyVisited[transitiveSetID] = struct{}{}
				setsToVisit = append(setsToVisit, transitiveSetID) // Push dependency
			} else if !processedForPostOrder[transitiveSetID] {
				allTransitiveProcessed = false // Dependency visited but not yet post-ordered
			}
		}

		if allTransitiveProcessed {
			// All dependencies handled, process this node
			processedForPostOrder[currentSetID] = true
			setsInPostOrder = append(setsInPostOrder, currentSetID)
			setsToVisit = setsToVisit[:len(setsToVisit)-1] // Pop
		}
	}

	// Second pass: Process direct entries in post-order
	for _, currentSetID := range setsInPostOrder {
		// Check global visited set - if visited by a prior call, skip entire processing for this ID
		if _, visited := visitedSetIDs[currentSetID]; visited {
			continue
		}
		visitedSetIDs[currentSetID] = struct{}{} // Mark globally visited now

		set, ok := slr.symlinkEntrySets[currentSetID]
		if !ok {
			// Already warned in first pass
			continue
		}

		// Process direct entries (sort map keys for deterministic order)
		directPaths := make([]string, 0, len(set.GetDirectEntries()))
		for p := range set.GetDirectEntries() {
			directPaths = append(directPaths, p)
		}
		sort.Strings(directPaths)

		for _, entryPath := range directPaths {
			targetID := set.GetDirectEntries()[entryPath]
			err := slr.processSingleSymlinkEntry(runfilesTree, rootSymlinks, entryPath, targetID, consumer)
			if err != nil {
				return err // Propagate error
			}
		}
	}

	return nil
}

// processSingleSymlinkEntry handles the path calculation and target reconstruction for one entry.
func (slr *SpawnLogReconstructor) processSingleSymlinkEntry(
	runfilesTree *spb.ExecLogEntry_RunfilesTree,
	rootSymlinks bool,
	entryPath string, // Path from the SymlinkEntrySet map key
	targetID uint32,
	consumer SymlinkConsumer,
) error {
	var runfilesTreeRelativePath string
	if rootSymlinks {
		runfilesTreeRelativePath = entryPath
	} else if strings.HasPrefix(entryPath, "../") {
		// Path is relative to runfiles root sibling (e.g., ../repo/file)
		runfilesTreeRelativePath = entryPath[3:] // Remove "../"
	} else {
		// Path is relative to workspace dir inside runfiles (e.g., pkg/file)
		if slr.workspaceRunfilesDir == "" {
			// This case might indicate an issue if non-root, non-../ symlinks exist without a workspace dir defined.
			// The Java code implicitly joins with workspaceRunfilesDirectory. We replicate this, potentially joining with "".
			log.Debugf("Workspace runfiles directory is empty, joining symlink path '%s' directly under runfiles root %s", entryPath, runfilesTree.GetPath())
		}
		runfilesTreeRelativePath = path.Join(slr.workspaceRunfilesDir, entryPath)
	}

	// Calculate the final absolute path within the execution root
	symlinkPath := path.Join(runfilesTree.GetPath(), runfilesTreeRelativePath)

	// Reconstruct the target(s) based on the ID
	targetFiles, err := slr.reconstructRunfilesSymlinkTarget(symlinkPath, targetID)
	if err != nil {
		log.Warnf("Error reconstructing target for symlink %s (targetID %d): %v", symlinkPath, targetID, err)
		return nil // Don't fail the whole process, just skip this entry
	}

	// Call the consumer with the primary path
	if err := consumer(runfilesTreeRelativePath, targetFiles); err != nil {
		return err // Propagate consumer errors
	}

	// Handle legacy external runfiles path if needed
	// Condition: legacy enabled, AND NOT root symlinks, AND relative path is NOT under workspace dir (or ws dir is empty), AND workspace dir exists
	isExternal := false
	if slr.workspaceRunfilesDir == "" {
		// If no workspace dir, any non-root symlink not starting with ../ is considered "external" for legacy purposes if legacy is on.
		isExternal = !rootSymlinks && !strings.HasPrefix(entryPath, "../")
	} else {
		isExternal = !rootSymlinks && !strings.HasPrefix(runfilesTreeRelativePath, slr.workspaceRunfilesDir+"/")
	}

	if runfilesTree.GetLegacyExternalRunfiles() && isExternal && slr.workspaceRunfilesDir != "" {
		// Determine the original repo-relative path to join with "external/"
		originalRepoRelativePath := ""
		if strings.HasPrefix(entryPath, "../") {
			originalRepoRelativePath = entryPath[3:] // e.g., "my_repo/path/to/file"
		} else if rootSymlinks {
			// This case should not happen based on the 'isExternal' check above
		} else {
			// Path was relative to workspace dir, e.g., "pkg/file", but isExternal was true.
			// This implies wsDir was empty OR relative path didn't start with it.
			// If wsDir was empty, runfilesTreeRelativePath is "pkg/file".
			// If wsDir was not empty, but path didn't start with it (e.g. path.Join("", "pkg/file")),
			// runfilesTreeRelativePath is still "pkg/file".
			// The Java logic seems to rely on the relative path *not* containing wsDir when external.
			// Let's assume the relative path computed is the one to be prefixed.
			originalRepoRelativePath = runfilesTreeRelativePath
		}

		legacyRelativePath := path.Join(slr.workspaceRunfilesDir, "external", originalRepoRelativePath)
		legacySymlinkPath := path.Join(runfilesTree.GetPath(), legacyRelativePath)

		// Reconstruct target again with the legacy path
		legacyTargetFiles, err := slr.reconstructRunfilesSymlinkTarget(legacySymlinkPath, targetID)
		if err != nil {
			log.Warnf("Error reconstructing target for legacy symlink %s (targetID %d): %v", legacySymlinkPath, targetID, err)
			// Continue without failing
		} else if err := consumer(legacyRelativePath, legacyTargetFiles); err != nil {
			return err // Propagate consumer error
		}
	}

	return nil
}

// reconstructRunfilesSymlinkTarget finds the target entry (File, Symlink, Dir) for a runfiles symlink
// and returns the corresponding File protos with their paths adjusted to the new symlink location.
func (slr *SpawnLogReconstructor) reconstructRunfilesSymlinkTarget(newPath string, targetId uint32) ([]*spb.File, error) {
	if targetId == 0 {
		// Target ID 0 likely represents an empty file or directory created implicitly.
		// Represent it as an empty file at the new path.
		return []*spb.File{{Path: newPath}}, nil
	}

	if f, ok := slr.files[targetId]; ok {
		// Target is a file
		newFile := proto.Clone(f).(*spb.File)
		newFile.Path = newPath
		// Ensure hash function name is set
		if newFile.Digest != nil && newFile.Digest.HashFunctionName == "" {
			newFile.Digest.HashFunctionName = slr.hashFunc
		}
		return []*spb.File{newFile}, nil
	}
	if sym, ok := slr.symlinks[targetId]; ok {
		// Target is another symlink
		newSymlink := proto.Clone(sym).(*spb.File)
		newSymlink.Path = newPath
		// Keep original SymlinkTargetPath
		return []*spb.File{newSymlink}, nil
	}
	if d, ok := slr.dirs[targetId]; ok {
		// Target is a directory
		reconstructedFiles := make([]*spb.File, 0, len(d.files))
		for _, fileInDir := range d.files {
			// Calculate the file's path relative to the original directory root
			// Use path.Rel for robustness, handle potential errors
			relativePath, err := RelPath(d.path, fileInDir.GetPath())
			if err != nil {
				// Fallback to simple TrimPrefix if Rel fails
				log.Warnf("path.Rel failed for dir '%s' and file '%s': %v. Falling back to TrimPrefix.", d.path, fileInDir.GetPath(), err)
				if strings.HasPrefix(fileInDir.GetPath(), d.path+"/") {
					relativePath = strings.TrimPrefix(fileInDir.GetPath(), d.path+"/")
				} else if fileInDir.GetPath() == d.path {
					// Should not happen if file is *in* dir, but handle defensively
					relativePath = "."
				} else {
					// Cannot determine relative path, skip this file
					log.Warnf("Cannot determine relative path for file '%s' within directory '%s'. Skipping.", fileInDir.GetPath(), d.path)
					continue
				}
			}

			// Construct the new path relative to the symlink location
			finalPath := path.Join(newPath, relativePath)

			newFile := proto.Clone(fileInDir).(*spb.File)
			newFile.Path = finalPath
			// Ensure hash function name is set
			if newFile.Digest != nil && newFile.Digest.HashFunctionName == "" {
				newFile.Digest.HashFunctionName = slr.hashFunc
			}
			reconstructedFiles = append(reconstructedFiles, newFile)
		}
		return reconstructedFiles, nil
	}
	// Check RunfilesTree last, as it's represented as a directory
	if rft, ok := slr.runfilesTrees[targetId]; ok {
		// Target is a runfiles tree (treat like a directory)
		reconstructedFiles := make([]*spb.File, 0, len(rft.files))
		for _, fileInTree := range rft.files {
			relativePath, err := RelPath(rft.path, fileInTree.GetPath())
			if err != nil {
				log.Warnf("path.Rel failed for runfiles tree '%s' and file '%s': %v. Falling back to TrimPrefix.", rft.path, fileInTree.GetPath(), err)
				if strings.HasPrefix(fileInTree.GetPath(), rft.path+"/") {
					relativePath = strings.TrimPrefix(fileInTree.GetPath(), rft.path+"/")
				} else if fileInTree.GetPath() == rft.path {
					relativePath = "."
				} else {
					log.Warnf("Cannot determine relative path for file '%s' within runfiles tree '%s'. Skipping.", fileInTree.GetPath(), rft.path)
					continue
				}
			}
			finalPath := path.Join(newPath, relativePath)
			newFile := proto.Clone(fileInTree).(*spb.File)
			newFile.Path = finalPath
			// Ensure hash function name is set (already done during tree reconstruction, but double check)
			if newFile.Digest != nil && newFile.Digest.HashFunctionName == "" {
				newFile.Digest.HashFunctionName = slr.hashFunc
			}
			reconstructedFiles = append(reconstructedFiles, newFile)
		}
		return reconstructedFiles, nil
	}

	return nil, fmt.Errorf("symlink target ID %d not found or is not a file, symlink, directory, or runfiles tree", targetId)
}

func reconstructDir(d *spb.ExecLogEntry_Directory) *reconstructedDir {
	filesInDir := make([]*spb.File, 0, len(d.GetFiles()))
	for _, file := range d.GetFiles() {
		// Note: Hash function applied later in the main loop
		filesInDir = append(filesInDir, reconstructFile(d, file))
	}
	return &reconstructedDir{
		path:  d.GetPath(),
		files: filesInDir,
	}
}

func reconstructFile(parentDir *spb.ExecLogEntry_Directory, file *spb.ExecLogEntry_File) *spb.File {
	f := &spb.File{Digest: file.GetDigest()} // Note: Hash function applied later
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

// --- Runfiles Path Calculation Logic (Mirrors Java) ---

// Define patterns similar to Java (using Go's regexp syntax)
var (
	// Non-sibling layout patterns
	defaultGeneratedFileRunfilesPathPattern = regexp.MustCompile(`^(?:bazel|blaze)-out/[^/]+/[^/]+/(?:external/(?P<repo>[^/]+)/)?(?P<path>.+)$`)
	defaultSourceFileRunfilesPathPattern    = regexp.MustCompile(`^(?:external/(?P<repo>[^/]+)/)?(?P<path>.+)$`)

	// Sibling layout patterns
	// Go doesn't support lookaheads directly in the standard regexp. We simplify.
	// This might misinterpret some edge cases involving hyphens in output dirs vs repo names.
	// The Java lookahead `(?=/[^/]+-[^/]+/)(?!/coverage-metadata/)` checks if the *next* segment
	// looks like a configuration mnemonic (contains a hyphen) but isn't "coverage-metadata".
	// We approximate this by checking if the segment *after* the potential repo name contains a hyphen.
	// This is less precise but captures the common case.
	siblingLayoutGeneratedFileRunfilesPathPattern = regexp.MustCompile(`^(?:bazel|blaze)-out/(?P<repo>[^/]+)/(?P<config>[^/]+(?:-[^/]+)?)/[^/]+/(?P<path>.+)$`) // Tries to capture config
	siblingLayoutSourceFileRunfilesPathPattern    = regexp.MustCompile(`^(?:\.\./(?P<repo>[^/]+)/)?(?P<path>.+)$`)

	// Need a pattern to *try* matching the sibling layout generated file *without* a repo first
	siblingLayoutGeneratedFileNoRepoPattern = regexp.MustCompile(`^(?:bazel|blaze)-out/(?P<config>[^/]+(?:-[^/]+)?)/[^/]+/(?P<path>.+)$`) // Includes config check
)

type runfilesPathInfo struct {
	repo string // "" if main repository
	path string // Path relative to the repository root
}

func extractRunfilesPath(execPath string, siblingRepositoryLayout bool) (*runfilesPathInfo, error) {
	var matcher []string
	var groupNames []string

	findMatch := func(p *regexp.Regexp) bool {
		matcher = p.FindStringSubmatch(execPath)
		if matcher != nil {
			groupNames = p.SubexpNames()
			return true
		}
		return false
	}

	getGroup := func(name string) string {
		for i, n := range groupNames {
			if n == name && i < len(matcher) && i < len(matcher) { // Check bounds
				return matcher[i]
			}
		}
		return ""
	}

	if siblingRepositoryLayout {
		// Try generated pattern with repo first
		if findMatch(siblingLayoutGeneratedFileRunfilesPathPattern) {
			// Check if the 'config' segment looks like a config (contains hyphen, not just repo name)
			configSegment := getGroup("config")
			if strings.Contains(configSegment, "-") && configSegment != "coverage-metadata" {
				// Looks like repo/config/..., so repo is correct
				return &runfilesPathInfo{repo: getGroup("repo"), path: getGroup("path")}, nil
			}
			// If it doesn't look like config, maybe the first segment WAS the config, not the repo. Fall through.
		}
		// Try generated pattern without repo (higher precedence than source)
		if findMatch(siblingLayoutGeneratedFileNoRepoPattern) {
			// Check if the 'config' segment looks like a config
			configSegment := getGroup("config")
			if strings.Contains(configSegment, "-") && configSegment != "coverage-metadata" {
				// Looks like config/bin/..., so no repo
				return &runfilesPathInfo{repo: "", path: getGroup("path")}, nil
			}
			// If not, this pattern matched but didn't represent the no-repo case correctly. Fall through.
		}
		// Try source pattern
		if findMatch(siblingLayoutSourceFileRunfilesPathPattern) {
			return &runfilesPathInfo{repo: getGroup("repo"), path: getGroup("path")}, nil
		}
	} else { // Default layout
		if findMatch(defaultGeneratedFileRunfilesPathPattern) || findMatch(defaultSourceFileRunfilesPathPattern) {
			return &runfilesPathInfo{repo: getGroup("repo"), path: getGroup("path")}, nil
		}
	}

	// Fallback/Error: If sibling layout was true, but generated patterns didn't definitively identify repo/no-repo based on config segment,
	// we might have misparsed. The default patterns might still work if the structure resembles them.
	if siblingRepositoryLayout {
		if findMatch(defaultGeneratedFileRunfilesPathPattern) || findMatch(defaultSourceFileRunfilesPathPattern) {
			log.Warnf("Exec path '%s' with sibling layout enabled matched default pattern after failing sibling patterns. Using default interpretation.", execPath)
			return &runfilesPathInfo{repo: getGroup("repo"), path: getGroup("path")}, nil
		}
	}

	return nil, fmt.Errorf("could not match execPath '%s' to any known runfiles pattern (siblingLayout: %v)", execPath, siblingRepositoryLayout)
}

func (slr *SpawnLogReconstructor) getRunfilesPaths(execPath string, legacyExternalRunfiles bool) ([]string, error) {
	info, err := extractRunfilesPath(execPath, slr.siblingRepoLayout)
	if err != nil {
		return nil, err
	}

	if info.repo == "" { // Main repository
		// Should not be empty if pattern matched correctly
		if slr.workspaceRunfilesDir == "" {
			log.Warnf("Workspace runfiles directory is empty, but needed for main repo path calculation for %s", execPath)
		}
		// Use path.Join, it handles empty workspaceRunfilesDir correctly (joins with just info.path)
		return []string{path.Join(slr.workspaceRunfilesDir, info.path)}, nil
	} else { // External repository
		paths := []string{}
		// Default path (repo name relative to runfiles root)
		paths = append(paths, path.Join(info.repo, info.path))
		// Legacy path (under workspace dir/external)
		if legacyExternalRunfiles && slr.workspaceRunfilesDir != "" {
			paths = append(paths, path.Join(slr.workspaceRunfilesDir, "external", info.repo, info.path))
		}
		return paths, nil
	}
}
