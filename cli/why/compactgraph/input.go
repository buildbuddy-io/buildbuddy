package compactgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"golang.org/x/exp/maps"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

type Digest = [sha256.Size]byte

func HashString(s string) Digest {
	return Digest(sha256.New().Sum([]byte(s)))
}

type Input interface {
	AnalysisDigest() Digest
	ExecutionDigest() Digest
}

type File struct {
	Path   string
	Digest Digest

	analysisDigest Digest
}

func (f *File) AnalysisDigest() Digest  { return f.analysisDigest }
func (f *File) ExecutionDigest() Digest { return f.Digest }

func (f *File) String() string { return "file:" + f.Path }

func (f *File) IsSourceFile() bool { return isSourcePath(f.Path) }

func ProtoToFile(f *spawn.ExecLogEntry_File) File {
	digest, err := hex.DecodeString(f.Digest.GetHash())
	if err != nil {
		panic(fmt.Sprint("invalid digest: ", f.Digest.GetHash()))
	}
	return File{
		Path:           f.Path,
		Digest:         Digest(digest),
		analysisDigest: HashString(f.Path),
	}
}

type Directory struct {
	Path  string
	Files []*File

	analysisDigest  Digest
	executionDigest Digest
}

func (d *Directory) AnalysisDigest() Digest  { return d.analysisDigest }
func (d *Directory) ExecutionDigest() Digest { return d.executionDigest }

func (d *Directory) IsSourceDirectory() bool { return isSourcePath(d.Path) }

func ProtoToDirectory(d *spawn.ExecLogEntry_Directory) *Directory {
	executionHash := sha256.New()

	files := make([]*File, 0, len(d.Files))
	for _, f := range d.Files {
		file := ProtoToFile(f)
		files = append(files, &file)
		// The names of files in source directories and tree artifacts are not available at analysis time.
		// The names of runfiles technically are (runfiles can be flattened), but rules usually don't inspect them.
		fileAnalysisHash := file.AnalysisDigest()
		executionHash.Write(fileAnalysisHash[:])
		fileExecutionHash := file.ExecutionDigest()
		executionHash.Write(fileExecutionHash[:])
	}

	return &Directory{
		Path:            d.Path,
		Files:           files,
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
func (s *InputSet) computeDeepDigests() {
	if s.deepAnalysisDigest != nil {
		return
	}

	allInputs := make(map[string]Input)
	setsToVisit := []*InputSet{s}
	visitedSets := make(map[*InputSet]struct{})
	for len(setsToVisit) > 0 {
		set := setsToVisit[0]
		setsToVisit = setsToVisit[1:]
		visitedSets[set] = struct{}{}
		for _, file := range set.Files {
			allInputs[file.Path] = file
		}
		for _, directory := range set.Directories {
			allInputs[directory.Path] = directory
		}
		for _, transitiveSet := range set.TransitiveSets {
			if _, visited := visitedSets[transitiveSet]; !visited {
				setsToVisit = append(setsToVisit, transitiveSet)
			}
		}
	}

	sortedInputs := maps.Keys(allInputs)
	slices.Sort(sortedInputs)

	analysisDigest := sha256.New()
	executionDigest := sha256.New()
	for _, path := range sortedInputs {
		input := allInputs[path]
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

func ProtoToInputSet(s *spawn.ExecLogEntry_InputSet, previousInputs []Input) *InputSet {
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
