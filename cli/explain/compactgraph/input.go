package compactgraph

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"golang.org/x/exp/maps"
)

type Hash = []byte

// Content hash differentiators for inputs.
const (
	fileContent = iota
	unresolvedSymlinkContent
	directoryContent
	inputSetContent
	invalidOutputContent
	symlinkEntrySetContent
	runfilesTreeContent
)

// Path hash differentiators for inputs.
const (
	directPath = iota
	transitivePaths
)

const workspaceRunfilesDirectory = "_main"

// Input represents a file, a directory or a nested set of such, all of which can be inputs to an action.
type Input interface {
	// Path of the input, which is unique within the scope of a single execution.
	Path() string

	// ShallowPathHash is a hash of all input paths.
	// It is shallow in the sense that it is fast to compute and input paths differ if their shallow digests differ,
	// but different sets of inputs can have the same shallow digest.
	ShallowPathHash() Hash

	// ShallowContentHash is a hash of the contents of all inputs.
	// It is shallow in the sense that it is fast to compute and input contents differ if their shallow digests differ,
	// but different sets of inputs can have the same shallow digest.
	// ShallowContentHash can only be meaningfully compared if ShallowPathDigest is equal.
	ShallowContentHash() Hash

	// Proto returns the spawn.ExecLogEntry representation of the input.
	Proto() any

	fmt.Stringer
}

// File represents either a regular file or an unresolved symlink.
type File struct {
	path        string
	pathHash    Hash
	contentHash Hash

	// Exactly one of these fields is non-empty.
	Digest     *spawn.Digest
	TargetPath string
}

func (f *File) Path() string             { return f.path }
func (f *File) ShallowPathHash() Hash    { return f.pathHash }
func (f *File) ShallowContentHash() Hash { return f.contentHash }
func (f *File) Proto() any {
	if f.TargetPath != "" {
		return &spawn.ExecLogEntry_UnresolvedSymlink{
			Path:       f.path,
			TargetPath: f.TargetPath,
		}
	} else {
		return &spawn.ExecLogEntry_File{
			Path:   f.path,
			Digest: f.Digest,
		}
	}
}

func (f *File) String() string { return "file:" + f.path }

func (f *File) IsSourceFile() bool { return isSourcePath(f.path) }

func protoToFile(f *spawn.ExecLogEntry_File, hashFunction string) *File {
	pathHash := sha256.New()
	pathHash.Write([]byte{directPath})
	pathHash.Write([]byte(f.Path))

	contentHash := sha256.New()
	contentHash.Write([]byte{fileContent})
	contentHash.Write([]byte(f.Digest.GetHash()))

	if f.Digest != nil {
		f.Digest.HashFunctionName = hashFunction
	}

	return &File{
		path:        f.Path,
		pathHash:    pathHash.Sum(nil),
		contentHash: contentHash.Sum(nil),
		Digest:      f.Digest,
	}
}

func protoToSymlink(s *spawn.ExecLogEntry_UnresolvedSymlink) *File {
	pathHash := sha256.New()
	pathHash.Write([]byte{directPath})
	pathHash.Write([]byte(s.Path))

	contentHash := sha256.New()
	contentHash.Write([]byte{unresolvedSymlinkContent})
	contentHash.Write([]byte(s.TargetPath))

	return &File{
		path:        s.Path,
		pathHash:    pathHash.Sum(nil),
		contentHash: contentHash.Sum(nil),
		TargetPath:  s.TargetPath,
	}
}

// A Directory represents one of the following Bazel artifact types:
//   - an output directory (aka TreeArtifact), which is declared via `ctx.actions.declare_directory` and can be
//     expanded into the contained file paths in command lines via `ctx.actions.args#add_all`;
//   - a runfiles directory (`foo.runfiles` for an executable `foo`), which is a symlink tree and generally not
//     mentioned in command lines, but instead discovered by the executable at runtime;
//   - a source directory (which requires --host_jvm_args=BAZEL_TRACK_SOURCE_DIRECTORIES=1 and isn't well-supported by
//     all parts of Bazel), which is always staged as a directory and never as the contained files.
type Directory struct {
	files       []*File
	path        string
	pathHash    Hash
	contentHash Hash
}

func (d *Directory) Path() string             { return d.path }
func (d *Directory) ShallowPathHash() Hash    { return d.pathHash }
func (d *Directory) ShallowContentHash() Hash { return d.contentHash }
func (d *Directory) Proto() any {
	fileProtos := make([]*spawn.ExecLogEntry_File, 0, len(d.files))
	for _, file := range d.files {
		fileProtos = append(fileProtos, file.Proto().(*spawn.ExecLogEntry_File))
	}
	return &spawn.ExecLogEntry_Directory{
		Path:  d.path,
		Files: fileProtos,
	}
}
func (d *Directory) Flatten() []Input {
	if !d.IsTreeArtifact() {
		return []Input{d}
	}
	var inputs []Input
	for _, file := range d.files {
		fullPath := d.path + "/" + file.Path()
		pathHash := sha256.New()
		pathHash.Write([]byte(fullPath))
		resolvedFile := &File{
			path:        fullPath,
			pathHash:    pathHash.Sum(nil),
			contentHash: file.contentHash,
		}
		inputs = append(inputs, resolvedFile)
	}
	return inputs
}

func isTreeArtifactPath(path string) bool {
	return !isSourcePath(path) && !strings.HasSuffix(path, ".runfiles")
}

func (d *Directory) IsTreeArtifact() bool {
	return isTreeArtifactPath(d.path)
}

func (d *Directory) String() string { return "dir:" + d.path }

func protoToDirectory(d *spawn.ExecLogEntry_Directory, hashFunction string) *Directory {
	pathHash := sha256.New()
	pathHash.Write([]byte{directPath})
	pathsAreContent := !isTreeArtifactPath(d.Path)
	// Implicitly encodes pathsAreContent and thus the hashing strategy used below.
	_ = binary.Write(pathHash, binary.LittleEndian, uint64(len(d.Path)))
	pathHash.Write([]byte(d.Path))

	contentHash := sha256.New()
	contentHash.Write([]byte{directoryContent})

	files := make([]*File, 0, len(d.Files))
	for _, f := range d.Files {
		file := protoToFile(f, hashFunction)
		files = append(files, file)
		if pathsAreContent {
			contentHash.Write(file.ShallowPathHash())
		} else {
			pathHash.Write(file.ShallowPathHash())
		}
		contentHash.Write(file.ShallowContentHash())
	}

	return &Directory{
		files:       files,
		path:        d.Path,
		pathHash:    pathHash.Sum(nil),
		contentHash: contentHash.Sum(nil),
	}
}

type InputSet struct {
	directEntries  []Input
	transitiveSets []*InputSet

	shallowPathHash    Hash
	shallowContentHash Hash
}

var emptyInputSet = &InputSet{}

func (s *InputSet) Path() string             { panic(fmt.Sprintf("InputSet %s doesn't have a path", s.String())) }
func (s *InputSet) ShallowPathHash() Hash    { return s.shallowPathHash }
func (s *InputSet) ShallowContentHash() Hash { return s.shallowContentHash }
func (s *InputSet) Proto() any {
	panic(fmt.Sprintf("InputSet %s doesn't support Proto()", s.String()))
}
func (s *InputSet) DirectRunfiles() runfilesSeq {
	return func(yield func(string, Input) bool) {
		for _, input := range s.directEntries {
			if !yield(computeRunfilesPath(input), input) {
				return
			}
		}
	}
}

func (s *InputSet) TransitiveRunfilesBackward() depsetSeq {
	return func(yield func(depset) bool) {
		for i := len(s.transitiveSets) - 1; i >= 0; i-- {
			if !yield(s.transitiveSets[i]) {
				return
			}
		}
	}
}

func (s *InputSet) markAsTools() {
	// Runfiles trees are always contained in the top-level input set, so we don't need to recurse.
	for _, input := range s.directEntries {
		if rt, ok := input.(*RunfilesTree); ok {
			rt.markAsTool()
		}
	}
}

func (s *InputSet) Flatten() []Input {
	inputsSet := make(map[Input]struct{})
	setsToVisit := []*InputSet{s}
	visitedSets := make(map[*InputSet]struct{})
	for len(setsToVisit) > 0 {
		set := setsToVisit[0]
		setsToVisit = setsToVisit[1:]
		visitedSets[set] = struct{}{}
		for _, input := range set.directEntries {
			switch in := input.(type) {
			case *File:
				inputsSet[in] = struct{}{}
			case *Directory:
				for _, file := range in.Flatten() {
					inputsSet[file] = struct{}{}
				}
			}
		}
		for _, ts := range set.transitiveSets {
			if _, visited := visitedSets[ts]; !visited {
				setsToVisit = append(setsToVisit, ts)
			}
		}
	}

	inputs := maps.Keys(inputsSet)
	sort.Slice(inputs, func(i, j int) bool {
		return inputs[i].Path() < inputs[j].Path()
	})
	return inputs
}

func (s *InputSet) String() string {
	return fmt.Sprintf("set:(direct=%v, transitive=%v)", s.directEntries, s.transitiveSets)
}

func protoToInputSet(s *spawn.ExecLogEntry_InputSet, previousInputs map[uint32]Input) *InputSet {
	pathHash := sha256.New()
	pathHash.Write([]byte{transitivePaths})

	contentHash := sha256.New()
	contentHash.Write([]byte{inputSetContent})

	directInputs := make([]Input, 0, len(s.InputIds)+len(s.FileIds)+len(s.DirectoryIds)+len(s.UnresolvedSymlinkIds))
	addInputs := func(ids []uint32) {
		for _, id := range ids {
			input := previousInputs[id]
			directInputs = append(directInputs, input)
			pathHash.Write(input.ShallowPathHash())
			contentHash.Write(input.ShallowContentHash())
		}
	}
	addInputs(s.InputIds)
	addInputs(s.FileIds)
	addInputs(s.DirectoryIds)
	addInputs(s.UnresolvedSymlinkIds)

	transitiveSets := make([]*InputSet, 0, len(s.TransitiveSetIds))
	for _, id := range s.TransitiveSetIds {
		set := previousInputs[id].(*InputSet)
		transitiveSets = append(transitiveSets, set)
		pathHash.Write(set.ShallowPathHash())
		contentHash.Write(set.ShallowContentHash())
	}

	return &InputSet{
		directEntries:      directInputs,
		transitiveSets:     transitiveSets,
		shallowPathHash:    pathHash.Sum(nil),
		shallowContentHash: contentHash.Sum(nil),
	}
}

type SymlinkEntrySet struct {
	directEntries  map[string]Input
	transitiveSets []*SymlinkEntrySet

	shallowPathHash    Hash
	shallowContentHash Hash
}

func (s *SymlinkEntrySet) Path() string {
	panic(fmt.Sprintf("SymlinkEntrySet %s doesn't have a path", s.String()))
}
func (s *SymlinkEntrySet) ShallowPathHash() Hash    { return s.shallowPathHash }
func (s *SymlinkEntrySet) ShallowContentHash() Hash { return s.shallowContentHash }
func (s *SymlinkEntrySet) Proto() any {
	panic(fmt.Sprintf("SymlinkEntrySet %s doesn't support Proto()", s.String()))
}
func (s *SymlinkEntrySet) String() string {
	return fmt.Sprintf("symlinks:(direct=%v, transitive=%v)", s.directEntries, s.transitiveSets)
}
func (s *SymlinkEntrySet) DirectRunfiles() runfilesSeq {
	return func(yield func(string, Input) bool) {
		// The order of direct entries is non-deterministic, but since there can't be path collisions, it doesn't
		// matter.
		for runfilesPath, input := range s.directEntries {
			if !yield(runfilesPath, input) {
				return
			}
		}
	}
}
func (s *SymlinkEntrySet) TransitiveRunfilesBackward() depsetSeq {
	return func(yield func(depset) bool) {
		for i := len(s.transitiveSets) - 1; i >= 0; i-- {
			if !yield(s.transitiveSets[i]) {
				return
			}
		}
	}
}

func protoToSymlinkEntrySet(s *spawn.ExecLogEntry_SymlinkEntrySet, previousInputs map[uint32]Input) *SymlinkEntrySet {
	pathHash := sha256.New()
	pathHash.Write([]byte{transitivePaths})
	contentHash := sha256.New()
	contentHash.Write([]byte{symlinkEntrySetContent})

	paths := maps.Keys(s.DirectEntries)
	slices.Sort(paths)
	directEntries := make(map[string]Input, len(paths))
	for _, p := range paths {
		directEntries[p] = previousInputs[s.DirectEntries[p]]
		_ = binary.Write(pathHash, binary.LittleEndian, uint64(len(p)))
		pathHash.Write([]byte(p))
		contentHash.Write(directEntries[p].ShallowContentHash())
	}

	transitiveSets := make([]*SymlinkEntrySet, 0, len(s.TransitiveSetIds))
	for _, id := range s.TransitiveSetIds {
		set := previousInputs[id].(*SymlinkEntrySet)
		transitiveSets = append(transitiveSets, set)
		pathHash.Write(set.ShallowPathHash())
		contentHash.Write(set.ShallowContentHash())
	}

	return &SymlinkEntrySet{
		directEntries:      directEntries,
		transitiveSets:     transitiveSets,
		shallowPathHash:    pathHash.Sum(nil),
		shallowContentHash: contentHash.Sum(nil),
	}
}

type RunfilesTree struct {
	Artifacts              *InputSet
	Symlinks               *SymlinkEntrySet
	RootSymlinks           *SymlinkEntrySet
	RepoMappingManifest    *File
	HasEmptyFiles          bool
	LegacyExternalRunfiles bool

	path               string
	shallowPathHash    Hash
	shallowContentHash Hash

	getCachedMapping func() (map[string]Input, bool)
}

func (r *RunfilesTree) Path() string             { return r.path }
func (r *RunfilesTree) ShallowPathHash() Hash    { return r.shallowPathHash }
func (r *RunfilesTree) ShallowContentHash() Hash { return r.shallowContentHash }
func (r *RunfilesTree) Proto() any {
	panic(fmt.Sprintf("RunfilesTree %s doesn't support Proto()", r.String()))
}
func (r *RunfilesTree) String() string {
	return fmt.Sprintf("runfiles:(path=%s, artifacts=%s, symlinks=%s, root_symlinks=%s, repo_mapping_manifest=%s, has_empty_files=%t, legacy_external_runfiles=%t)",
		r.path, r.Artifacts, r.Symlinks, r.RootSymlinks, r.RepoMappingManifest, r.HasEmptyFiles, r.LegacyExternalRunfiles)
}

func (r *RunfilesTree) markAsTool() {
	// Tool runfiles trees can be reused by multiple spawns, so it usually pays off to cache the mapping.
	if r.getCachedMapping != nil {
		return
	}
	r.getCachedMapping = sync.OnceValues(r.computeMapping)
}

func (r *RunfilesTree) ComputeMapping() (map[string]Input, bool) {
	if r.getCachedMapping != nil {
		return r.getCachedMapping()
	}
	return r.computeMapping()
}

func (r *RunfilesTree) computeMapping() (map[string]Input, bool) {
	m := make(map[string]Input)
	hasExternalRunfiles := false
	// Reconstruct runfiles with the same order of precedence as Bazel would (see spawn.proto):
	// 1. symlinks
	for workspaceRelativeRunfilesPath, artifact := range runfilesMapping(r.Symlinks) {
		m[path.Join(workspaceRunfilesDirectory, workspaceRelativeRunfilesPath)] = artifact
	}
	// 2. artifacts at canonical locations
	for runfilesPath, artifact := range runfilesMapping(r.Artifacts) {
		m[runfilesPath] = artifact
	}
	// 3. empty files
	// Empty files, if generated at all, are a pure function of the other paths and thus don't need to flattened
	// explicitly for the purpose of diffing runfiles trees.
	// 4. root symlinks (root_symlinks_id)
	for runfilesPath, artifact := range runfilesMapping(r.RootSymlinks) {
		m[runfilesPath] = artifact
	}
	// 5. the _repo_mapping file with the repo mapping manifest
	if r.RepoMappingManifest != nil {
		m["_repo_mapping"] = r.RepoMappingManifest
	}
	// 6. the <workspace runfiles directory>/.runfile file
	// Similar to empty files, this is a pure function of the other paths and doesn't need to be flattened explicitly.
	return m, hasExternalRunfiles
}

func protoToRunfilesTree(r *spawn.ExecLogEntry_RunfilesTree, previousInputs map[uint32]Input, hashFunctionName string) *RunfilesTree {
	pathHash := sha256.New()
	pathHash.Write([]byte{directPath})
	pathHash.Write([]byte(r.Path))

	// All paths of artifacts under the runfiles tree are considered content as they are usually opaque to the consuming
	// action's implementation function.
	contentHash := sha256.New()
	contentHash.Write([]byte{runfilesTreeContent})

	artifacts := previousInputs[r.InputSetId].(*InputSet)
	contentHash.Write(artifacts.ShallowPathHash())
	contentHash.Write(artifacts.ShallowContentHash())
	symlinks := previousInputs[r.SymlinksId].(*SymlinkEntrySet)
	contentHash.Write(symlinks.ShallowPathHash())
	contentHash.Write(symlinks.ShallowContentHash())
	rootSymlinks := previousInputs[r.RootSymlinksId].(*SymlinkEntrySet)
	contentHash.Write(rootSymlinks.ShallowPathHash())
	contentHash.Write(rootSymlinks.ShallowContentHash())

	var repoMappingManifest *File
	repoMappingManifestProto := r.RepoMappingManifest
	hasRepoMappingManifest := repoMappingManifestProto != nil
	writeBool(contentHash, hasRepoMappingManifest)
	if hasRepoMappingManifest {
		repoMappingManifestProto.Path = "_repo_mapping"
		repoMappingManifest = protoToFile(repoMappingManifestProto, hashFunctionName)
		contentHash.Write(repoMappingManifest.ShallowContentHash())
	}
	hasEmptyFiles := len(r.EmptyFiles) > 0
	writeBool(contentHash, hasEmptyFiles)
	writeBool(contentHash, r.LegacyExternalRunfiles)

	return &RunfilesTree{
		path:                   r.Path,
		Artifacts:              artifacts,
		Symlinks:               symlinks,
		RootSymlinks:           rootSymlinks,
		RepoMappingManifest:    repoMappingManifest,
		HasEmptyFiles:          hasEmptyFiles,
		LegacyExternalRunfiles: r.LegacyExternalRunfiles,
		shallowPathHash:        pathHash.Sum(nil),
		shallowContentHash:     contentHash.Sum(nil),
	}
}

type InvalidOutput struct {
	path string
}

func (i InvalidOutput) Path() string { return i.path }
func (i InvalidOutput) ShallowPathHash() Hash {
	panic(fmt.Sprintf("InvalidOutput %s doesn't support ShallowPathHash()", i.String()))
}

var invalidOutputHash = sha256.Sum256([]byte{invalidOutputContent})

func (i InvalidOutput) ShallowContentHash() Hash { return invalidOutputHash[:] }
func (i InvalidOutput) Proto() any               { return i.path }
func (i InvalidOutput) String() string           { return fmt.Sprintf("invalid:%s", i.path) }

type Spawn struct {
	Mnemonic    string
	TargetLabel string
	Args        []string
	ParamFiles  *InputSet
	Env         map[string]string
	Inputs      *InputSet
	Tools       *InputSet
	Outputs     []Input
	ExitCode    int32
}

const testRunnerXmlGeneration = "TestRunner (XML generation)"
const testRunnerCoverageCollection = "TestRunner (coverage collection)"

func protoToSpawn(s *spawn.ExecLogEntry_Spawn, previousInputs map[uint32]Input) (*Spawn, []string) {
	outputs := make([]Input, 0, len(s.Outputs))
	outputPaths := make([]string, 0, len(s.Outputs))
	for _, outputProto := range s.Outputs {
		var id uint32
		switch outputProto.Type.(type) {
		case *spawn.ExecLogEntry_Output_OutputId:
			id = outputProto.GetOutputId()
		case *spawn.ExecLogEntry_Output_FileId:
			id = outputProto.GetFileId()
		case *spawn.ExecLogEntry_Output_UnresolvedSymlinkId:
			id = outputProto.GetUnresolvedSymlinkId()
		case *spawn.ExecLogEntry_Output_DirectoryId:
			id = outputProto.GetDirectoryId()
		case *spawn.ExecLogEntry_Output_InvalidOutputPath:
			p := outputProto.GetInvalidOutputPath()
			if (s.Mnemonic == "Javac" || s.Mnemonic == "JavacTurbine") && strings.HasSuffix(p, ".jar") {
				// Java (header) compilation actions may run two spawns with --experimental_java_classpath=bazel.
				// The first one has a zero exit code even if it fails to compile the sources, but will not have
				// produced the output jar. Ignore it in favor of the second one which we know will come.
				return nil, nil
			}
			// TODO: Add full support for test shards.
			if s.Mnemonic == "TestRunner" {
				// If a test action output isn't generated by the spawn, it is created by the action as an empty file
				// or directory.
				base := path.Base(p)
				if base == "test.outputs" || base == "_coverage" {
					outputs = append(outputs, protoToDirectory(&spawn.ExecLogEntry_Directory{Path: p}, ""))
				} else {
					outputs = append(outputs, protoToFile(&spawn.ExecLogEntry_File{Path: p}, ""))
				}
				outputPaths = append(outputPaths, p)
				continue
			}
			if s.ExitCode != 0 {
				// If the spawn failed, we can't expect the output to have been created. Don't fail, but also don't add
				// any outputs as there can't be any executions that depend on the failing one.
				continue
			}
			// The action succeeded, but failed to create an output or created one of an unexpected file type.
			outputs = append(outputs, InvalidOutput{path: p})
			outputPaths = append(outputPaths, p)
			continue
		default:
			panic(fmt.Sprintf("%s %s: unsupported output type: %T", s.Mnemonic, s.TargetLabel, outputProto.Type))
		}
		output := previousInputs[id]
		outputs = append(outputs, output)
		outputPaths = append(outputPaths, output.Path())
		if path.Base(output.Path()) == "test.log" {
			panic(fmt.Sprintf("test.log output from %s %s", s.Mnemonic, s.TargetLabel))
		}
	}
	env := make(map[string]string, len(s.EnvVars))
	for _, kv := range s.EnvVars {
		env[kv.Name] = kv.Value
	}
	inputs := previousInputs[s.InputSetId].(*InputSet)
	paramFiles := drainParamFiles(inputs)

	// TestRunner actions can run multiple spawns. We distinguish between them by the output file name and assign
	// synthetic mnemonics.
	mnemonic := s.Mnemonic
	if mnemonic == "TestRunner" {
		if len(s.Outputs) == 1 {
			base := path.Base(outputPaths[0])
			if base == "test.xml" {
				mnemonic = testRunnerXmlGeneration
			} else if base == "coverage.dat" {
				mnemonic = testRunnerCoverageCollection
			}
		} else {
			// The test.log file is created by the TestRunner action itself, not by the spawn (it's obtained from its
			// stdout/stderr, which is not recorded in the execution log). We need to create a File input for it to
			// track the dependence between the test spawn and the split XML generation.
			testLog := path.Dir(outputPaths[0]) + "/test.log"
			outputPaths = append([]string{testLog}, outputPaths...)
			outputs = append([]Input{protoToFile(&spawn.ExecLogEntry_File{Path: testLog}, "")}, outputs...)
		}
	}

	tools := previousInputs[s.ToolSetId].(*InputSet)
	tools.markAsTools()
	return &Spawn{
		Mnemonic:    mnemonic,
		TargetLabel: s.TargetLabel,
		Args:        s.Args,
		ParamFiles:  paramFiles,
		Env:         env,
		Inputs:      inputs,
		Tools:       tools,
		Outputs:     outputs,
		ExitCode:    s.ExitCode,
	}, outputPaths
}

func (s *Spawn) String() string {
	return fmt.Sprintf("%s %s (%s)", s.Mnemonic, s.TargetLabel, s.PrimaryOutputPath())
}

func (s *Spawn) PrimaryOutputPath() string {
	return s.Outputs[0].Path()
}

var paramsFileRegexp = regexp.MustCompile(`.*-[0-9]+\.params`)

// drainParamFiles removes all param files from the input set and returns a new input set containing only the param
// files.
func drainParamFiles(set *InputSet) *InputSet {
	var paramFiles, nonParamFiles []Input
	for _, input := range set.directEntries {
		if file, ok := input.(*File); ok && paramsFileRegexp.MatchString(file.Path()) {
			paramFiles = append(paramFiles, file)
		} else {
			nonParamFiles = append(nonParamFiles, input)
		}
	}
	if len(paramFiles) == 0 {
		return emptyInputSet
	}
	set.directEntries = nonParamFiles
	return &InputSet{directEntries: paramFiles}
}

func writeBool(w io.Writer, b bool) {
	if b {
		_, _ = w.Write([]byte{1})
	} else {
		_, _ = w.Write([]byte{0})
	}
}

func isSourcePath(path string) bool {
	return !strings.HasPrefix(path, "bazel-out/")
}

func computeRunfilesPath(input Input) string {
	p := input.Path()
	// For a path such as "bazel-out/k8-fastbuild/bin/pkg/foo", trim to "pkg/foo".
	if strings.HasPrefix(p, "bazel-out/") {
		p = strings.SplitN(p, "/", 4)[3]
	}
	if strings.HasPrefix(p, "external/") {
		return p[len("external/"):]
	} else {
		return workspaceRunfilesDirectory + "/" + p
	}
}

type depset interface {
	DirectRunfiles() runfilesSeq
	TransitiveRunfilesBackward() depsetSeq
}
type runfilesSeq iter.Seq2[string, Input]
type depsetSeq iter.Seq[depset]

func runfilesMapping(s depset) runfilesSeq {
	return func(yield func(string, Input) bool) {
		toVisit := []depset{s}
		type entry struct {
			runfilesPath string
			artifact     Input
		}
		visited := make(map[any]struct{})
		for len(toVisit) > 0 {
			current := toVisit[len(toVisit)-1]
			toVisit = toVisit[:len(toVisit)-1]
			if _, visitedBefore := visited[current]; !visitedBefore {
				// First visit, queue transitive sets for visit before revisiting the current set.
				toVisit = append(toVisit, current)
				for ts := range current.TransitiveRunfilesBackward() {
					if _, ok := visited[ts]; !ok {
						toVisit = append(toVisit, ts)
					}
				}
			} else {
				// Second visit, visit the direct entries only.
				for runfilesPath, artifact := range current.DirectRunfiles() {
					e := entry{runfilesPath: runfilesPath, artifact: artifact}
					if _, ok := visited[e]; ok {
						continue
					}
					visited[e] = struct{}{}
					if !yield(runfilesPath, artifact) {
						return
					}
				}
			}
		}
	}
}
