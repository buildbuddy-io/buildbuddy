package compactgraph

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"iter"
	"path"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
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

// With Bzlmod, the workspace runfiles directory is fixed to "_main". It's almost impossible for two builds with
// different workspace runfiles directories to not differ and the only case in which the exact name matters is for rules
// that manually prefix root symlinks with it, which is extremely rare. For these reasons, we consider it as a fixed
// value.
const fixedWorkspaceRunfilesDirectory = "_main"

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
//     mentioned in command lines, but instead discovered by the executable at runtime (only represented as a Directory
//     in Bazel 7.3.2 and earlier);
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
func IsTreeArtifactPath(path string) bool {
	return !isSourcePath(path) && !strings.HasSuffix(path, ".runfiles")
}

func (d *Directory) String() string { return "dir:" + d.path }

func protoToDirectory(d *spawn.ExecLogEntry_Directory, hashFunction string) *Directory {
	pathHash := sha256.New()
	pathHash.Write([]byte{directPath})
	pathsAreContent := !IsTreeArtifactPath(d.Path)
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
	DirectEntries  []Input
	TransitiveSets []*InputSet

	shallowPathHash    Hash
	shallowContentHash Hash
}

var emptyInputSet = protoToInputSet(&spawn.ExecLogEntry_InputSet{}, nil)

func (s *InputSet) Path() string {
	log.Fatalf("InputSet %s doesn't have a path", s.String())
	panic("unreachable")
}
func (s *InputSet) ShallowPathHash() Hash    { return s.shallowPathHash }
func (s *InputSet) ShallowContentHash() Hash { return s.shallowContentHash }
func (s *InputSet) Proto() any {
	log.Fatalf("InputSet %s doesn't support Proto()", s.String())
	panic("unreachable")
}
func (s *InputSet) DirectRunfiles(filter InputFilter) RunfilesSeq {
	return func(yield func(string, Input) bool) {
		for _, input := range s.DirectEntries {
			if filter(input) {
				if !yield(computeRunfilesPath(input), input) {
					return
				}
			}
		}
	}
}
func (s *InputSet) TransitiveRunfilesBackward() DepsetSeq {
	return depsetsBackward(s.TransitiveSets)
}

func (s *InputSet) Flatten() []Input {
	inputsSet := make(map[Input]struct{})
	setsToVisit := []*InputSet{s}
	visitedSets := make(map[*InputSet]struct{})
	for len(setsToVisit) > 0 {
		set := setsToVisit[0]
		setsToVisit = setsToVisit[1:]
		visitedSets[set] = struct{}{}
		for _, input := range set.DirectEntries {
			inputsSet[input] = struct{}{}
		}
		for _, ts := range set.TransitiveSets {
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
	return fmt.Sprintf("set:(direct=%v, transitive=%v)", s.DirectEntries, s.TransitiveSets)
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
		DirectEntries:      directInputs,
		TransitiveSets:     transitiveSets,
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

var emptySymlinkEntrySet = protoToSymlinkEntrySet(&spawn.ExecLogEntry_SymlinkEntrySet{}, nil)

func (s *SymlinkEntrySet) Path() string {
	log.Fatalf("SymlinkEntrySet %s doesn't have a path", s.String())
	panic("unreachable")
}
func (s *SymlinkEntrySet) ShallowPathHash() Hash    { return s.shallowPathHash }
func (s *SymlinkEntrySet) ShallowContentHash() Hash { return s.shallowContentHash }
func (s *SymlinkEntrySet) Proto() any {
	log.Fatalf("SymlinkEntrySet %s doesn't support Proto()", s.String())
	panic("unreachable")
}
func (s *SymlinkEntrySet) String() string {
	return fmt.Sprintf("symlinks:(direct=%v, transitive=%v)", s.directEntries, s.transitiveSets)
}
func (s *SymlinkEntrySet) DirectRunfiles(filter InputFilter) RunfilesSeq {
	return func(yield func(string, Input) bool) {
		// The order of direct entries is non-deterministic, but since there can't be path collisions, it doesn't
		// matter.
		for runfilesPath, input := range s.directEntries {
			if filter(input) {
				if !yield(runfilesPath, input) {
					return
				}
			}
		}
	}
}
func (s *SymlinkEntrySet) TransitiveRunfilesBackward() DepsetSeq {
	return depsetsBackward(s.transitiveSets)
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
		_ = binary.Write(contentHash, binary.LittleEndian, uint64(len(p)))
		contentHash.Write([]byte(p))
		contentHash.Write(directEntries[p].ShallowContentHash())
	}

	transitiveSets := make([]*SymlinkEntrySet, 0, len(s.TransitiveSetIds))
	for _, id := range s.TransitiveSetIds {
		set := previousInputs[id].(*SymlinkEntrySet)
		transitiveSets = append(transitiveSets, set)
		contentHash.Write(set.ShallowPathHash())
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
	Artifacts           *InputSet
	Symlinks            *SymlinkEntrySet
	RootSymlinks        *SymlinkEntrySet
	RepoMappingManifest *File
	EmptyFiles          []string

	path               string
	shallowPathHash    Hash
	shallowContentHash Hash

	exactContentHash Hash
}

func (r *RunfilesTree) Path() string             { return r.path }
func (r *RunfilesTree) ShallowPathHash() Hash    { return r.shallowPathHash }
func (r *RunfilesTree) ShallowContentHash() Hash { return r.shallowContentHash }
func (r *RunfilesTree) Proto() any {
	log.Fatalf("RunfilesTree %s doesn't support Proto()", r.String())
	panic("unreachable")
}
func (r *RunfilesTree) String() string {
	return fmt.Sprintf("runfiles:(path=%s, artifacts=%s, symlinks=%s, root_symlinks=%s, repo_mapping_manifest=%s)",
		r.path, r.Artifacts, r.Symlinks, r.RootSymlinks, r.RepoMappingManifest)
}

var emptyDigest = map[string]*spawn.Digest{
	"SHA-1": {
		Hash:             "da39a3ee5e6b4b0d3255bfef95601890afd80709",
		HashFunctionName: "SHA-1",
	},
	"SHA-256": {
		Hash:             "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		HashFunctionName: "SHA-256",
	},
	"BLAKE3": {
		Hash:             "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
		HashFunctionName: "BLAKE3",
	},
}

func (r *RunfilesTree) ComputeMapping(workspaceRunfilesDirectory, hashFunction string) map[string]Input {
	m := make(map[string]Input)
	// Reconstruct runfiles with the same order of precedence as Bazel would (see spawn.proto):
	// 1. Symlinks.
	// Bazel internally represents symlinks as NestedSets of (path, artifact) pairs that use reference equality,
	// effectively turning them into a list - hence noFilter is used here. Since the same pair object can theoretically
	// appear multiple times in the same NestedSet (but only through highly pathological Starlark), this is not always
	// accurate, but this behavior is inherent to the compact execution log format and deemed acceptable.
	for workspaceRelativeRunfilesPath, artifact := range iterateAsRunfiles(r.Symlinks, noFilter) {
		m[path.Join(fixedWorkspaceRunfilesDirectory, workspaceRelativeRunfilesPath)] = artifact
	}
	// 2. Artifacts at canonical locations.
	// Later artifacts override earlier ones, but only after removing duplicates. Bazel internally uses a NestedSet,
	// which deduplicates artifacts and then maps them to their potentially duplicate runfiles paths).
	for runfilesPath, artifact := range iterateAsRunfiles(r.Artifacts, newDuplicateFilter()) {
		m[runfilesPath] = artifact
	}
	// 3. Empty files.
	for _, emptyFilePath := range r.EmptyFiles {
		// Empty file paths as contained in the log are not prefixed with the workspace runfiles directory.
		m[path.Join(workspaceRunfilesDirectory, emptyFilePath)] = protoToFile(&spawn.ExecLogEntry_File{
			Path:   emptyFilePath,
			Digest: emptyDigest[hashFunction],
		}, hashFunction)
	}
	// 4. Root symlinks.
	for runfilesPath, artifact := range iterateAsRunfiles(r.RootSymlinks, noFilter) {
		m[runfilesPath] = artifact
	}
	// 5. The repo mapping manifest at its fixed location.
	if r.RepoMappingManifest != nil {
		m["_repo_mapping"] = r.RepoMappingManifest
	}
	// 6. The existence of the <workspace runfiles directory>/.runfile file, is a pure function, so it doesn't need to
	// be considered here.

	// Populate the exact content hash so that consumers don't need to recompute the mapping to determine whether the
	// tree changed.
	contentHash := sha256.New()
	sortedRunfilesPaths := maps.Keys(m)
	sort.Strings(sortedRunfilesPaths)
	for _, p := range sortedRunfilesPaths {
		_ = binary.Write(contentHash, binary.LittleEndian, uint64(len(p)))
		contentHash.Write([]byte(p))
		contentHash.Write(m[p].ShallowContentHash())
	}
	r.exactContentHash = contentHash.Sum(nil)

	return m
}

type OpaqueRunfilesDirectory struct {
	runfilesTree *RunfilesTree
}

func (o *OpaqueRunfilesDirectory) Path() string {
	return o.runfilesTree.Path()
}

func (o *OpaqueRunfilesDirectory) ShallowPathHash() Hash {
	// This hash is already opaque, i.e., it doesn't reveal the contents of the runfiles tree.
	return o.runfilesTree.ShallowPathHash()
}

func (o *OpaqueRunfilesDirectory) ShallowContentHash() Hash {
	if o.runfilesTree.exactContentHash != nil {
		return o.runfilesTree.exactContentHash
	}
	// The exact hash isn't available yet while computing shallow hashes of InputSets, so fall back to the shallow hash.
	return o.runfilesTree.shallowContentHash
}

func (o *OpaqueRunfilesDirectory) Proto() any {
	// Opaque runfiles directories are inputs of other spawns and should only result in an indication that the tree
	// changed, the exact diff of its contents will be available on the dedicated "Runfiles directory" spawn. Otherwise
	// a change in a tool would be reported on every spawn that uses it.
	return &spawn.ExecLogEntry_Directory{Path: o.Path()}
}

func (o *OpaqueRunfilesDirectory) String() string {
	return fmt.Sprintf("runfilesDirectory:%s", o.runfilesTree.Path())
}

func protoToRunfilesTree(r *spawn.ExecLogEntry_RunfilesTree, previousInputs map[uint32]Input, hashFunctionName string, interner func(string) string) *RunfilesTree {
	pathHash := sha256.New()
	pathHash.Write([]byte{directPath})
	pathHash.Write([]byte(r.Path))

	contentHash := sha256.New()
	contentHash.Write([]byte{runfilesTreeContent})

	artifacts := previousInputs[r.InputSetId].(*InputSet)
	contentHash.Write(artifacts.ShallowPathHash())
	contentHash.Write(artifacts.ShallowContentHash())
	var symlinks, rootSymlinks *SymlinkEntrySet
	if r.SymlinksId == 0 {
		symlinks = emptySymlinkEntrySet
	} else {
		symlinks = previousInputs[r.SymlinksId].(*SymlinkEntrySet)
	}
	contentHash.Write(symlinks.ShallowPathHash())
	contentHash.Write(symlinks.ShallowContentHash())
	if r.RootSymlinksId == 0 {
		rootSymlinks = emptySymlinkEntrySet
	} else {
		rootSymlinks = previousInputs[r.RootSymlinksId].(*SymlinkEntrySet)
	}
	contentHash.Write(rootSymlinks.ShallowPathHash())
	contentHash.Write(rootSymlinks.ShallowContentHash())

	var repoMappingManifest *File
	repoMappingManifestProto := r.RepoMappingManifest
	if repoMappingManifestProto != nil {
		repoMappingManifestProto.Path = "_repo_mapping"
		repoMappingManifest = protoToFile(repoMappingManifestProto, hashFunctionName)
		contentHash.Write(repoMappingManifest.ShallowPathHash())
		contentHash.Write(repoMappingManifest.ShallowContentHash())
	}

	binary.Write(contentHash, binary.LittleEndian, uint64(len(r.EmptyFiles)))
	internedEmptyFiles := make([]string, 0, len(r.EmptyFiles))
	for _, emptyFilePath := range r.EmptyFiles {
		binary.Write(contentHash, binary.LittleEndian, uint64(len(emptyFilePath)))
		contentHash.Write([]byte(emptyFilePath))
		// Bazel emits an empty file for each parent of a directory with a Python file in it. This typically results in
		// many duplicated paths across Python runfiles trees. Interning them reduces the retained memory to be roughly
		// linear in the compressed size of the exec log.
		internedEmptyFiles = append(internedEmptyFiles, interner(emptyFilePath))
	}

	return &RunfilesTree{
		path:                r.Path,
		Artifacts:           artifacts,
		Symlinks:            symlinks,
		RootSymlinks:        rootSymlinks,
		RepoMappingManifest: repoMappingManifest,
		EmptyFiles:          internedEmptyFiles,
		shallowPathHash:     pathHash.Sum(nil),
		shallowContentHash:  contentHash.Sum(nil),
	}
}

type InvalidOutput struct {
	path string
}

func (i InvalidOutput) Path() string { return i.path }
func (i InvalidOutput) ShallowPathHash() Hash {
	log.Fatalf("InvalidOutput %s doesn't support ShallowPathHash()", i.String())
	panic("unreachable")
}

var invalidOutputHash = sha256.Sum256([]byte{invalidOutputContent})

func (i InvalidOutput) ShallowContentHash() Hash { return invalidOutputHash[:] }
func (i InvalidOutput) Proto() any               { return i.path }
func (i InvalidOutput) String() string           { return fmt.Sprintf("invalid:%s", i.path) }

type Spawn struct {
	Mnemonic       string
	TargetLabel    string
	Args           []string
	ParamFiles     *InputSet
	Env            map[string]string
	ExecProperties map[string]string
	Inputs         *InputSet
	Tools          *InputSet
	Outputs        []Input
	ExitCode       int32
}

const testRunnerXmlGeneration = "TestRunner (XML generation)"
const testRunnerCoverageCollection = "TestRunner (coverage collection)"

// Names of environment variables whose set of values is expected to be closer
// to O(n) than O(1) in the number of spawns, mostly due to the values being
// dependent on the spawn's primary output path.
var volatileEnvVars = map[string]struct{}{
	"COVERAGE_DIR":                            {},
	"COVERAGE_MANIFEST":                       {},
	"COVERAGE_OUTPUT_FILE":                    {},
	"JAVA_RUNFILES":                           {},
	"PYTHON_RUNFILES":                         {},
	"RUNFILES_DIR":                            {},
	"TEST_BINARY":                             {},
	"TEST_INFRASTRUCTURE_FAILURE_FILE":        {},
	"TEST_LOGSPLITTER_OUTPUT_FILE":            {},
	"TEST_NAME":                               {},
	"TEST_PREMATURE_EXIT_FILE":                {},
	"TEST_SHARD_STATUS_FILE":                  {},
	"TEST_SRCDIR":                             {},
	"TEST_TARGET":                             {},
	"TEST_TMPDIR":                             {},
	"TEST_UNDECLARED_OUTPUTS_ANNOTATIONS":     {},
	"TEST_UNDECLARED_OUTPUTS_ANNOTATIONS_DIR": {},
	"TEST_UNDECLARED_OUTPUTS_DIR":             {},
	"TEST_UNDECLARED_OUTPUTS_MANIFEST":        {},
	"TEST_UNDECLARED_OUTPUTS_ZIP":             {},
	"TEST_UNUSED_RUNFILES_LOG_FILE":           {},
	"TEST_WARNINGS_OUTPUT_FILE":               {},
	"XML_OUTPUT_FILE":                         {},
}

func protoToSpawn(s *spawn.ExecLogEntry_Spawn, previousInputs map[uint32]Input, interner func(string) string) (*Spawn, []string) {
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
			log.Fatalf("%s %s: unsupported output type: %T", s.Mnemonic, s.TargetLabel, outputProto.Type)
		}
		output := previousInputs[id]
		outputs = append(outputs, output)
		outputPaths = append(outputPaths, output.Path())
		if path.Base(output.Path()) == "test.log" {
			log.Fatalf("test.log output from %s %s", s.Mnemonic, s.TargetLabel)
		}
	}
	// Environment variable names are typically repeated across spawns, but some
	// values are not.
	env := make(map[string]string, len(s.EnvVars))
	for _, kv := range s.EnvVars {
		var value string
		if _, volatile := volatileEnvVars[kv.Name]; volatile {
			value = kv.Value
		} else {
			value = interner(kv.Value)
		}
		env[interner(kv.Name)] = value
	}
	// Exec property keys and values are typically repeated across spawns.
	execProperties := make(map[string]string, len(s.Platform.GetProperties()))
	for _, kv := range s.Platform.GetProperties() {
		execProperties[interner(kv.Name)] = interner(kv.Value)
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

	return &Spawn{
		Mnemonic:       mnemonic,
		TargetLabel:    s.TargetLabel,
		Args:           s.Args,
		ParamFiles:     paramFiles,
		Env:            env,
		ExecProperties: execProperties,
		Inputs:         inputs,
		Tools:          previousInputs[s.ToolSetId].(*InputSet),
		Outputs:        outputs,
		ExitCode:       s.ExitCode,
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
	for _, input := range set.DirectEntries {
		if file, ok := input.(*File); ok && paramsFileRegexp.MatchString(file.Path()) {
			paramFiles = append(paramFiles, file)
		} else {
			nonParamFiles = append(nonParamFiles, input)
		}
	}
	if len(paramFiles) == 0 {
		return emptyInputSet
	}
	set.DirectEntries = nonParamFiles
	return &InputSet{DirectEntries: paramFiles}
}

func isSourcePath(path string) bool {
	return !strings.HasPrefix(path, "bazel-out/")
}

// Exec-configured output paths look like:
// bazel-out/darwin_arm64-opt-exec-ST-d57f47055a04/bin/pkg/foo
// bazel-out/my_platform-opt-exec/bin/pkg/foo
var execOutputRegexp = regexp.MustCompile("bazel-out/[^/]+-exec[-/].*")

// Whether the given path is an output path of an artifact built in the exec configuration.
func isExecOutputPath(path string) bool {
	return execOutputRegexp.MatchString(path)
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
		return fixedWorkspaceRunfilesDirectory + "/" + p
	}
}

type InputFilter func(Input) bool

var noFilter = func(Input) bool { return true }

func newDuplicateFilter() InputFilter {
	seen := make(map[Input]struct{})
	return func(i Input) bool {
		if _, seenBefore := seen[i]; seenBefore {
			return false
		}
		seen[i] = struct{}{}
		return true
	}
}

type depset interface {
	DirectRunfiles(filter InputFilter) RunfilesSeq
	TransitiveRunfilesBackward() DepsetSeq
}
type RunfilesSeq = iter.Seq2[string, Input]
type DepsetSeq = iter.Seq[depset]

// iterateAsRunfiles iterates the sequence of runfiles paths and artifacts staged at those paths in the given depset.
func iterateAsRunfiles(s depset, filter InputFilter) RunfilesSeq {
	return func(yield func(string, Input) bool) {
		setsToVisit := []depset{s}
		visitedSets := make(map[depset]struct{})
		for len(setsToVisit) > 0 {
			currentSet := setsToVisit[len(setsToVisit)-1]
			setsToVisit = setsToVisit[:len(setsToVisit)-1]
			// Visit entries in postorder, i.e., transitive sets before direct entries and each of them in left-to-right
			// order. Order matters in case of runfiles path collisions.
			if _, visitedBefore := visitedSets[currentSet]; !visitedBefore {
				// First visit, queue transitive sets for visit before revisiting the current set.
				setsToVisit = append(setsToVisit, currentSet)
				for ts := range currentSet.TransitiveRunfilesBackward() {
					if _, visited := visitedSets[ts]; !visited {
						setsToVisit = append(setsToVisit, ts)
					}
				}
				visitedSets[currentSet] = struct{}{}
			} else {
				// Second visit, visit the direct entries only.
				for runfilesPath, artifact := range currentSet.DirectRunfiles(filter) {
					if !yield(runfilesPath, artifact) {
						return
					}
				}
			}
		}
	}
}

// depsetsBackward returns a sequence that iterates the given slice of depsets in reverse order.
// This helper is necessary since there is no covariant type (slices aren't) that can be efficiently iterated in
// reverse order (generators can't).
func depsetsBackward[T depset](depsets []T) DepsetSeq {
	return func(yield func(depset) bool) {
		for i := len(depsets) - 1; i >= 0; i-- {
			if !yield(depsets[i]) {
				return
			}
		}
	}
}
