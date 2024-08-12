package compactgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"strings"
)

type Digest = [sha256.Size]byte

func HashString(s string) Digest {
	return Digest(sha256.New().Sum([]byte(s)))
}

type Input interface {
	ShallowAnalysisDigest() Digest
	ShallowExecutionDigest() Digest
	DeepAnalysisDigest() Digest
	DeepExecutionDigest() Digest
}

type File struct {
	Path   string
	Digest Digest

	pathDigest Digest
}

func (f *File) ShallowAnalysisDigest() Digest  { return f.pathDigest }
func (f *File) ShallowExecutionDigest() Digest { return f.Digest }
func (f *File) DeepAnalysisDigest() Digest     { return f.pathDigest }
func (f *File) DeepExecutionDigest() Digest    { return f.Digest }

func (f *File) String() string { return "file:" + f.Path }

func (f *File) IsSourceFile() bool { return isSourcePath(f.Path) }

func ProtoToFile(f *spawn.ExecLogEntry_File) File {
	digest, err := hex.DecodeString(f.Digest.GetHash())
	if err != nil {
		panic(fmt.Sprint("invalid digest: ", f.Digest.GetHash()))
	}
	return File{
		Path:       f.Path,
		Digest:     Digest(digest),
		pathDigest: HashString(f.Path),
	}
}

type Directory struct {
	Path  string
	Files []*File

	analysisDigest  Digest
	executionDigest Digest
}

func (d *Directory) ShallowAnalysisDigest() Digest  { return d.analysisDigest }
func (d *Directory) ShallowExecutionDigest() Digest { return d.executionDigest }
func (d *Directory) DeepAnalysisDigest() Digest     { return d.analysisDigest }
func (d *Directory) DeepExecutionDigest() Digest    { return d.executionDigest }

func (d *Directory) IsSourceDirectory() bool { return isSourcePath(d.Path) }

func ProtoToDirectory(d *spawn.ExecLogEntry_Directory) *Directory {
	analysisHash := sha256.New()
	executionHash := sha256.New()

	files := make([]*File, 0, len(d.Files))
	for _, f := range d.Files {
		file := ProtoToFile(f)
		files = append(files, &file)
		fileAnalysisHash := file.ShallowAnalysisDigest()
		analysisHash.Write(fileAnalysisHash[:])
		fileExecutionHash := file.ShallowExecutionDigest()
		executionHash.Write(fileExecutionHash[:])
	}

	return &Directory{
		Path:            d.Path,
		Files:           files,
		analysisDigest:  Digest(analysisHash.Sum(nil)),
		executionDigest: Digest(executionHash.Sum(nil)),
	}
}

type InputSet struct {
	Files          []*File
	Directories    []*Directory
	TransitiveSets []*InputSet

	analysisDigest  Digest
	executionDigest Digest
}

func (s *InputSet) ShallowAnalysisDigest() Digest  { return d.analysisDigest }
func (s *InputSet) ShallowExecutionDigest() Digest { return d.executionDigest }
func (s *InputSet) DeepAnalysisDigest() Digest     { return d.analysisDigest }
func (s *InputSet) DeepExecutionDigest() Digest    { return d.executionDigest }

func ProtoToInputSet(s *spawn.ExecLogEntry_InputSet, previousInputs []Input) *InputSet {
	analysisDigest := sha256.New()
	executionDigest := sha256.New()

	analysisDigest.Write([]byte{0})
	executionDigest.Write([]byte{0})
	files := make([]*File, 0, len(s.FileIds))
	for _, fid := range s.FileIds {
		file := previousInputs[fid].(*File)
		files = append(files, file)
		fileAnalysisDigest := file.ShallowAnalysisDigest()
		analysisDigest.Write(fileAnalysisDigest[:])
		fileExecutionDigest := file.ShallowExecutionDigest()
		executionDigest.Write(fileExecutionDigest[:])
	}

	analysisDigest.Write([]byte{1})
	executionDigest.Write([]byte{1})
	directories := make([]*Directory, 0, len(s.DirectoryIds))
	for _, did := range s.DirectoryIds {
		directory := previousInputs[did].(*Directory)
		directories = append(directories, directory)
		directoryAnalysisDigest := directory.ShallowAnalysisDigest()
		analysisDigest.Write(directoryAnalysisDigest[:])
		directoryExecutionDigest := directory.ShallowExecutionDigest()
		executionDigest.Write(directoryExecutionDigest[:])
	}

	analysisDigest.Write([]byte{2})
	executionDigest.Write([]byte{2})
	transitiveSets := make([]*InputSet, 0, len(s.TransitiveSetIds))
	for _, tsid := range s.TransitiveSetIds {
		transitiveSet := previousInputs[tsid].(*InputSet)
		transitiveSets = append(transitiveSets, transitiveSet)
		transitiveSetAnalysisDigest := transitiveSet.ShallowAnalysisDigest()
		analysisDigest.Write(transitiveSetAnalysisDigest[:])
		transitiveSetExecutionDigest := transitiveSet.ShallowExecutionDigest()
		executionDigest.Write(transitiveSetExecutionDigest[:])
	}

	return &InputSet{
		Files:          files,
		Directories:    directories,
		TransitiveSets: transitiveSets,

		analysisDigest:  Digest(analysisDigest.Sum(nil)),
		executionDigest: Digest(executionDigest.Sum(nil)),
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
