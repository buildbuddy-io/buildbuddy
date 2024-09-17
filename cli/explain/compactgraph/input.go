package compactgraph

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"golang.org/x/exp/maps"
)

type Hash = []byte

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
	contentHash := sha256.New()
	// Distinguish between regular files and symlinks.
	contentHash.Write([]byte{0})
	contentHash.Write([]byte(f.Digest.GetHash()))
	if f.Digest != nil {
		f.Digest.HashFunctionName = hashFunction
	}
	return &File{
		path:        f.Path,
		pathHash:    sha256.New().Sum([]byte(f.Path)),
		contentHash: contentHash.Sum(nil),
		Digest:      f.Digest,
	}
}

func protoToSymlink(s *spawn.ExecLogEntry_UnresolvedSymlink) *File {
	contentHash := sha256.New()
	// Distinguish between regular files and symlinks.
	contentHash.Write([]byte{1})
	contentHash.Write([]byte(s.TargetPath))
	return &File{
		path:        s.Path,
		pathHash:    sha256.New().Sum([]byte(s.Path)),
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
		resolvedFile := &File{
			path:        fullPath,
			pathHash:    sha256.New().Sum([]byte(fullPath)),
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
	contentHash := sha256.New()

	pathsAreContent := !isTreeArtifactPath(d.Path)
	// Implicitly encodes pathsAreContent and thus the hashing strategy used below.
	_ = binary.Write(pathHash, binary.LittleEndian, uint64(len(d.Path)))
	pathHash.Write([]byte(d.Path))

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
	Files          []*File
	Directories    []*Directory
	TransitiveSets []*InputSet

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

func (s *InputSet) Flatten() []Input {
	inputsSet := make(map[Input]struct{})
	setsToVisit := []*InputSet{s}
	visitedSets := make(map[*InputSet]struct{})
	for len(setsToVisit) > 0 {
		set := setsToVisit[0]
		setsToVisit = setsToVisit[1:]
		visitedSets[set] = struct{}{}
		for _, file := range set.Files {
			inputsSet[file] = struct{}{}
		}
		for _, directory := range set.Directories {
			for _, input := range directory.Flatten() {
				inputsSet[input] = struct{}{}
			}
		}
		for _, transitiveSet := range set.TransitiveSets {
			if _, visited := visitedSets[transitiveSet]; !visited {
				setsToVisit = append(setsToVisit, transitiveSet)
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
	return fmt.Sprintf("set:(files=%v,dirs=%v,transitiveSets=%v)", s.Files, s.Directories, s.TransitiveSets)
}

func protoToInputSet(s *spawn.ExecLogEntry_InputSet, previousInputs map[int32]Input) *InputSet {
	pathHash := sha256.New()
	contentHash := sha256.New()

	pathHash.Write([]byte{0})
	contentHash.Write([]byte{0})
	files := make([]*File, 0, len(s.FileIds))
	for _, fid := range s.FileIds {
		file := previousInputs[fid].(*File)
		files = append(files, file)
		pathHash.Write(file.ShallowPathHash())
		contentHash.Write(file.ShallowContentHash())
	}

	pathHash.Write([]byte{1})
	contentHash.Write([]byte{1})
	directories := make([]*Directory, 0, len(s.DirectoryIds))
	for _, did := range s.DirectoryIds {
		directory := previousInputs[did].(*Directory)
		directories = append(directories, directory)
		pathHash.Write(directory.ShallowPathHash())
		contentHash.Write(directory.ShallowContentHash())
	}

	pathHash.Write([]byte{2})
	contentHash.Write([]byte{2})
	transitiveSets := make([]*InputSet, 0, len(s.TransitiveSetIds))
	for _, tsid := range s.TransitiveSetIds {
		transitiveSet := previousInputs[tsid].(*InputSet)
		transitiveSets = append(transitiveSets, transitiveSet)
		pathHash.Write(transitiveSet.ShallowPathHash())
		contentHash.Write(transitiveSet.ShallowContentHash())
	}

	return &InputSet{
		Files:          files,
		Directories:    directories,
		TransitiveSets: transitiveSets,

		shallowPathHash:    pathHash.Sum(nil),
		shallowContentHash: contentHash.Sum(nil),
	}
}

func isSourcePath(path string) bool {
	return !strings.HasPrefix(path, "bazel-out/")
}

type Spawn struct {
	Mnemonic    string
	TargetLabel string
	Args        []string
	ParamFiles  *InputSet
	Env         map[string]string
	Inputs      *InputSet
	Tools       *InputSet
	Outputs     []Input
}

func protoToSpawn(s *spawn.ExecLogEntry_Spawn, previousInputs map[int32]Input) (*Spawn, []string) {
	if s.ExitCode != 0 {
		return nil, nil
	}

	outputs := make([]Input, 0, len(s.Outputs))
	outputPaths := make([]string, 0, len(s.Outputs))
	for _, output := range s.Outputs {
		switch output.Type.(type) {
		case *spawn.ExecLogEntry_Output_FileId:
			file := previousInputs[output.GetFileId()]
			outputs = append(outputs, file)
			outputPaths = append(outputPaths, file.Path())
		case *spawn.ExecLogEntry_Output_UnresolvedSymlinkId:
			symlink := previousInputs[output.GetUnresolvedSymlinkId()]
			outputs = append(outputs, symlink)
			outputPaths = append(outputPaths, symlink.Path())
		case *spawn.ExecLogEntry_Output_DirectoryId:
			directory := previousInputs[output.GetDirectoryId()]
			outputs = append(outputs, directory)
			outputPaths = append(outputPaths, directory.Path())
		case *spawn.ExecLogEntry_Output_InvalidOutputPath:
			path := output.GetInvalidOutputPath()
			if (s.Mnemonic == "Javac" || s.Mnemonic == "JavacTurbine") && strings.HasSuffix(path, ".jar") {
				// Java (header) compilation actions may run two spawns with --experimental_java_classpath=bazel.
				// The first one has a zero exit code even if it fails to compile the sources, but will not have
				// produced the output jar. Ignore it in favor of the second one which we know will come.
				return nil, nil
			}
			if s.Mnemonic == "TestRunner" {
				// Test actions have many optional outputs such as e.g. test.exited_prematurely.
				continue
			}
			panic(fmt.Sprintf("%s %s: invalid output: %s", s.Mnemonic, s.TargetLabel, path))
		default:
			panic(fmt.Sprintf("%s %s: unsupported output type: %T", s.Mnemonic, s.TargetLabel, output.Type))
		}
	}
	env := make(map[string]string, len(s.EnvVars))
	for _, kv := range s.EnvVars {
		env[kv.Name] = kv.Value
	}
	inputs := previousInputs[s.InputSetId].(*InputSet)
	paramFiles := drainParamFiles(inputs)
	return &Spawn{
		Mnemonic:    s.Mnemonic,
		TargetLabel: s.TargetLabel,
		Args:        s.Args,
		ParamFiles:  paramFiles,
		Env:         env,
		Inputs:      inputs,
		Tools:       previousInputs[s.ToolSetId].(*InputSet),
		Outputs:     outputs,
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
// files (or nil if there are none).
func drainParamFiles(set *InputSet) *InputSet {
	var paramFiles, nonParamFiles []*File
	for _, file := range set.Files {
		if paramsFileRegexp.MatchString(file.Path()) {
			paramFiles = append(paramFiles, file)
		} else {
			nonParamFiles = append(nonParamFiles, file)
		}
	}
	if len(paramFiles) == 0 {
		return emptyInputSet
	}
	set.Files = nonParamFiles
	return &InputSet{Files: paramFiles}
}