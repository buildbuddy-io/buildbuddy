package compactgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"golang.org/x/exp/maps"
	"regexp"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

type Digest = [sha256.Size]byte

func HashString(s string) Digest {
	return Digest(sha256.New().Sum([]byte(s)))
}

var emptyFileDigest = HashString("")

type Input interface {
	fmt.Stringer

	Path() string
	AnalysisDigest() Digest
	ExecutionDigest() Digest
}

type File struct {
	Digest Digest

	path           string
	analysisDigest Digest
}

func (f *File) Path() string            { return f.path }
func (f *File) AnalysisDigest() Digest  { return f.analysisDigest }
func (f *File) ExecutionDigest() Digest { return f.Digest }

func (f *File) String() string { return "file:" + f.path }

func (f *File) IsSourceFile() bool { return isSourcePath(f.path) }

func ProtoToFile(f *spawn.ExecLogEntry_File) *File {
	digest, err := hex.DecodeString(f.Digest.GetHash())
	if err != nil || (len(digest) != sha256.Size && len(digest) != 0) {
		panic(fmt.Sprint("invalid digest: ", f.Digest.GetHash()))
	}
	// A missing digest means that the file is empty.
	if len(digest) == 0 {
		digest = emptyFileDigest[:]
	}
	return &File{
		Digest:         Digest(digest),
		path:           f.Path,
		analysisDigest: HashString(f.Path),
	}
}

type Directory struct {
	Files []*File

	path            string
	analysisDigest  Digest
	executionDigest Digest
}

func (d *Directory) Path() string            { return d.path }
func (d *Directory) AnalysisDigest() Digest  { return d.analysisDigest }
func (d *Directory) ExecutionDigest() Digest { return d.executionDigest }

func (d *Directory) String() string { return "dir:" + d.path }

func (d *Directory) IsSourceDirectory() bool { return isSourcePath(d.path) }

func ProtoToDirectory(d *spawn.ExecLogEntry_Directory) *Directory {
	executionHash := sha256.New()

	files := make([]*File, 0, len(d.Files))
	for _, f := range d.Files {
		file := ProtoToFile(f)
		files = append(files, file)
		// The names of files in source directories and tree artifacts are not available at analysis time.
		// The names of runfiles technically are (runfiles can be flattened), but rules usually don't inspect them.
		fileAnalysisHash := file.AnalysisDigest()
		executionHash.Write(fileAnalysisHash[:])
		fileExecutionHash := file.ExecutionDigest()
		executionHash.Write(fileExecutionHash[:])
	}

	return &Directory{
		Files:           files,
		path:            d.Path,
		analysisDigest:  HashString(d.Path),
		executionDigest: Digest(executionHash.Sum(nil)),
	}
}

type InputSet struct {
	Files          []*File
	Directories    []*Directory
	TransitiveSets []*InputSet

	shallowAnalysisDigest  Digest
	shallowExecutionDigest Digest
	deepAnalysisDigest     *Digest
	deepExecutionDigest    *Digest
}

var emptyInputSet = &InputSet{}

func (s *InputSet) Path() string            { panic(fmt.Sprintf("InputSet %s doesn't have a path", s.String())) }
func (s *InputSet) AnalysisDigest() Digest  { return s.shallowAnalysisDigest }
func (s *InputSet) ExecutionDigest() Digest { return s.shallowExecutionDigest }
func (s *InputSet) DeepAnalysisDigest() Digest {
	s.computeDeepDigests()
	return *s.deepAnalysisDigest
}
func (s *InputSet) DeepExecutionDigest() Digest {
	s.computeDeepDigests()
	return *s.deepExecutionDigest
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
			inputsSet[directory] = struct{}{}
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

func (s *InputSet) computeDeepDigests() {
	if s.deepAnalysisDigest != nil {
		return
	}

	sortedInputs := s.Flatten()

	analysisDigest := sha256.New()
	executionDigest := sha256.New()
	for _, input := range sortedInputs {
		inputAnalysisDigest := input.AnalysisDigest()
		analysisDigest.Write(inputAnalysisDigest[:])
		inputExecutionDigest := input.ExecutionDigest()
		executionDigest.Write(inputExecutionDigest[:])
	}

	s.deepAnalysisDigest = new(Digest)
	copy(s.deepAnalysisDigest[:], analysisDigest.Sum(nil))
	s.deepExecutionDigest = new(Digest)
	copy(s.deepExecutionDigest[:], executionDigest.Sum(nil))
}

func (s *InputSet) String() string {
	return fmt.Sprintf("set:(files=%v,dirs=%v,transitiveSets=%v)", s.Files, s.Directories, s.TransitiveSets)
}

func ProtoToInputSet(s *spawn.ExecLogEntry_InputSet, previousInputs map[int32]Input) *InputSet {
	analysisDigest := sha256.New()
	executionDigest := sha256.New()

	analysisDigest.Write([]byte{0})
	executionDigest.Write([]byte{0})
	files := make([]*File, 0, len(s.FileIds))
	for _, fid := range s.FileIds {
		file := previousInputs[fid].(*File)
		files = append(files, file)
		fileAnalysisDigest := file.AnalysisDigest()
		analysisDigest.Write(fileAnalysisDigest[:])
		fileExecutionDigest := file.ExecutionDigest()
		executionDigest.Write(fileExecutionDigest[:])
	}

	analysisDigest.Write([]byte{1})
	executionDigest.Write([]byte{1})
	directories := make([]*Directory, 0, len(s.DirectoryIds))
	for _, did := range s.DirectoryIds {
		directory := previousInputs[did].(*Directory)
		directories = append(directories, directory)
		directoryAnalysisDigest := directory.AnalysisDigest()
		analysisDigest.Write(directoryAnalysisDigest[:])
		directoryExecutionDigest := directory.ExecutionDigest()
		executionDigest.Write(directoryExecutionDigest[:])
	}

	analysisDigest.Write([]byte{2})
	executionDigest.Write([]byte{2})
	transitiveSets := make([]*InputSet, 0, len(s.TransitiveSetIds))
	for _, tsid := range s.TransitiveSetIds {
		transitiveSet := previousInputs[tsid].(*InputSet)
		transitiveSets = append(transitiveSets, transitiveSet)
		transitiveSetAnalysisDigest := transitiveSet.AnalysisDigest()
		analysisDigest.Write(transitiveSetAnalysisDigest[:])
		transitiveSetExecutionDigest := transitiveSet.ExecutionDigest()
		executionDigest.Write(transitiveSetExecutionDigest[:])
	}

	return &InputSet{
		Files:          files,
		Directories:    directories,
		TransitiveSets: transitiveSets,

		shallowAnalysisDigest:  Digest(analysisDigest.Sum(nil)),
		shallowExecutionDigest: Digest(executionDigest.Sum(nil)),
	}
}

func isSourcePath(path string) bool {
	return !strings.HasPrefix(path, "bazel-out/")
}

func isRunfilesDirPath(path string) bool {
	return !isSourcePath(path) && strings.HasSuffix(path, ".runfiles")
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
				// The first one has a non-zero exit code even if it fails to compile the sources, but will not have
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
	return fmt.Sprintf("%s %s (%s)", s.Mnemonic, s.Label, s.PrimaryOutputPath())
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
