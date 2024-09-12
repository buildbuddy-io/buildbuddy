package compactgraph

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"golang.org/x/exp/maps"
)

type Digest = []byte

// Input represents a file, a directory or a nested set of such, all of which can be inputs to an action.
type Input interface {
	// Path of the input, which is unique within the scope of a single execution.
	Path() string

	// ShallowPathDigest is a digest of all input paths.
	// It is shallow in the sense that it is fast to compute and input paths differ if their shallow digests differ,
	// but different sets of inputs can have the same shallow digest.
	ShallowPathDigest() Digest

	// ShallowContentDigest is a digest of the contents of all inputs.
	// It is shallow in the sense that it is fast to compute and input contents differ if their shallow digests differ,
	// but different sets of inputs can have the same shallow digest.
	// ShallowContentDigest can only be meaningfully compared if ShallowPathDigest is equal.
	ShallowContentDigest() Digest

	fmt.Stringer
}

type File struct {
	path          string
	pathDigest    Digest
	contentDigest Digest
}

func (f *File) Path() string                 { return f.path }
func (f *File) ShallowPathDigest() Digest    { return f.pathDigest }
func (f *File) ShallowContentDigest() Digest { return f.contentDigest }

func (f *File) String() string { return "file:" + f.path }

func (f *File) IsSourceFile() bool { return isSourcePath(f.path) }

func ProtoToFile(f *spawn.ExecLogEntry_File) *File {
	digest, err := hex.DecodeString(f.Digest.GetHash())
	if err != nil {
		panic(fmt.Sprint("invalid digest: ", f.Digest.GetHash()))
	}
	// A missing digest means that the file is empty.
	if len(digest) == 0 {
		digest = sha256.New().Sum([]byte(("")))
	}
	return &File{
		path:          f.Path,
		pathDigest:    sha256.New().Sum([]byte(f.Path)),
		contentDigest: digest,
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
	files         []*File
	path          string
	pathDigest    Digest
	contentDigest Digest
}

func (d *Directory) Path() string                 { return d.path }
func (d *Directory) ShallowPathDigest() Digest    { return d.pathDigest }
func (d *Directory) ShallowContentDigest() Digest { return d.contentDigest }
func (d *Directory) ListInputs() []Input {
	if !d.IsTreeArtifact() {
		return []Input{d}
	}
	var inputs []Input
	for _, file := range d.files {
		fullPath := d.path + "/" + file.Path()
		resolvedFile := &File{
			path:          fullPath,
			pathDigest:    sha256.New().Sum([]byte(fullPath)),
			contentDigest: file.contentDigest,
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

func ProtoToDirectory(d *spawn.ExecLogEntry_Directory) *Directory {
	pathDigest := sha256.New()
	contentDigest := sha256.New()

	pathsAreContent := !isTreeArtifactPath(d.Path)
	// Implicitly encodes pathsAreContent and thus the hashing strategy used below.
	_ = binary.Write(pathDigest, binary.LittleEndian, len(d.Path))
	pathDigest.Write([]byte(d.Path))

	files := make([]*File, 0, len(d.Files))
	for _, f := range d.Files {
		file := ProtoToFile(f)
		files = append(files, file)
		if pathsAreContent {
			contentDigest.Write(file.ShallowPathDigest())
		} else {
			pathDigest.Write(file.ShallowPathDigest())
		}
		contentDigest.Write(file.ShallowContentDigest())
	}

	return &Directory{
		files:         files,
		path:          d.Path,
		pathDigest:    pathDigest.Sum(nil),
		contentDigest: contentDigest.Sum(nil),
	}
}

type InputSet struct {
	Files          []*File
	Directories    []*Directory
	TransitiveSets []*InputSet

	shallowPathDigest    Digest
	shallowContentDigest Digest
}

var emptyInputSet = &InputSet{}

func (s *InputSet) Path() string                 { panic(fmt.Sprintf("InputSet %s doesn't have a path", s.String())) }
func (s *InputSet) ShallowPathDigest() Digest    { return s.shallowPathDigest }
func (s *InputSet) ShallowContentDigest() Digest { return s.shallowContentDigest }

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
			for _, input := range directory.ListInputs() {
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

func ProtoToInputSet(s *spawn.ExecLogEntry_InputSet, previousInputs map[int32]Input) *InputSet {
	pathDigest := sha256.New()
	contentDigest := sha256.New()

	pathDigest.Write([]byte{0})
	contentDigest.Write([]byte{0})
	files := make([]*File, 0, len(s.FileIds))
	for _, fid := range s.FileIds {
		file := previousInputs[fid].(*File)
		files = append(files, file)
		pathDigest.Write(file.ShallowPathDigest())
		contentDigest.Write(file.ShallowContentDigest())
	}

	pathDigest.Write([]byte{1})
	contentDigest.Write([]byte{1})
	directories := make([]*Directory, 0, len(s.DirectoryIds))
	for _, did := range s.DirectoryIds {
		directory := previousInputs[did].(*Directory)
		directories = append(directories, directory)
		pathDigest.Write(directory.ShallowPathDigest())
		contentDigest.Write(directory.ShallowContentDigest())
	}

	pathDigest.Write([]byte{2})
	contentDigest.Write([]byte{2})
	transitiveSets := make([]*InputSet, 0, len(s.TransitiveSetIds))
	for _, tsid := range s.TransitiveSetIds {
		transitiveSet := previousInputs[tsid].(*InputSet)
		transitiveSets = append(transitiveSets, transitiveSet)
		pathDigest.Write(transitiveSet.ShallowPathDigest())
		contentDigest.Write(transitiveSet.ShallowContentDigest())
	}

	return &InputSet{
		Files:          files,
		Directories:    directories,
		TransitiveSets: transitiveSets,

		shallowPathDigest:    pathDigest.Sum(nil),
		shallowContentDigest: contentDigest.Sum(nil),
	}
}

func isSourcePath(path string) bool {
	return !strings.HasPrefix(path, "bazel-out/")
}

type Spawn struct {
	Mnemonic   string
	Label      string
	Args       []string
	ParamFiles *InputSet
	Env        map[string]string
	Inputs     *InputSet
	Tools      *InputSet
	Outputs    []Input
}

func ProtoToSpawn(s *spawn.ExecLogEntry_Spawn, previousInputs map[int32]Input) (*Spawn, []string) {
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
		Mnemonic:   s.Mnemonic,
		Label:      s.TargetLabel,
		Args:       s.Args,
		ParamFiles: paramFiles,
		Env:        env,
		Inputs:     inputs,
		Tools:      previousInputs[s.ToolSetId].(*InputSet),
		Outputs:    outputs,
	}, outputPaths
}

func (s *Spawn) String() string {
	return fmt.Sprintf("%s %s", s.Mnemonic, s.Label)
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
