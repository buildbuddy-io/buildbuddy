package compactgraph

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn"
	gocmp "github.com/google/go-cmp/cmp"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protodelim"
)

type CompactGraph map[string]*Spawn

func ReadCompactLog(in io.Reader) (CompactGraph, error) {
	d, err := zstd.NewReader(in)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	r := bufio.NewReader(d)

	var entry spawn.ExecLogEntry
	cg := make(CompactGraph)
	previousInputs := make(map[int32]Input)
	previousInputs[0] = emptyInputSet
	for {
		err := protodelim.UnmarshalFrom(r, &entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch entry.Type.(type) {
		case *spawn.ExecLogEntry_Invocation_:
			hashFunction := entry.GetInvocation().HashFunctionName
			if hashFunction != "SHA-256" {
				return nil, fmt.Errorf("unsupported hash function: %q", hashFunction)
			}
		case *spawn.ExecLogEntry_File_:
			file := ProtoToFile(entry.GetFile())
			previousInputs[entry.Id] = file
		case *spawn.ExecLogEntry_Directory_:
			dir := ProtoToDirectory(entry.GetDirectory())
			previousInputs[entry.Id] = dir
		case *spawn.ExecLogEntry_InputSet_:
			inputSet := ProtoToInputSet(entry.GetInputSet(), previousInputs)
			previousInputs[entry.Id] = inputSet
		case *spawn.ExecLogEntry_Spawn_:
			s, outputPaths := ProtoToSpawn(entry.GetSpawn(), previousInputs)
			if s != nil {
				for _, path := range outputPaths {
					cg[path] = s
				}
			}
		case *spawn.ExecLogEntry_UnresolvedSymlink_:
			panic(fmt.Sprintf("unresolved symlinks are unsupported, got %s --> %s", entry.GetUnresolvedSymlink().Path, entry.GetUnresolvedSymlink().TargetPath))
		}
	}
	return cg, nil
}

func Compare(a, b CompactGraph) (diags []string) {
	aPrimaryOutputs := a.primaryOutputs()
	bPrimaryOutputs := b.primaryOutputs()

	aExtraOutputs := a.ReduceToRoots(setDifference(aPrimaryOutputs, bPrimaryOutputs))
	for _, path := range aExtraOutputs {
		diags = append(diags, fmt.Sprintf("A: extra top-level spawn %s", a[path]))
	}
	bExtraOutputs := b.ReduceToRoots(setDifference(bPrimaryOutputs, aPrimaryOutputs))
	for _, path := range bExtraOutputs {
		diags = append(diags, fmt.Sprintf("B: extra top-level spawn %s", b[path]))
	}

	commonOutputs := setIntersection(aPrimaryOutputs, bPrimaryOutputs)
	for _, output := range commonOutputs {
		aSpawn := a[output]
		bSpawn := b[output]
		extraDiags, _ := diffSpawns(aSpawn, bSpawn)
		diags = append(diags, extraDiags...)
	}
	//_ = b.Walk(commonOutputs, func(node interface{}) error {
	//	bSpawn, ok := node.(*Spawn)
	//	if !ok {
	//		return nil
	//	}
	//	aSpawn, ok := a[bSpawn.PrimaryOutputPath()]
	//	if !ok {
	//		diags = append(diags, fmt.Sprintf("B: extra spawn %s", bSpawn))
	//		return SkipSubgraph
	//	}
	//	extraDiags, skipSubgraph := diffSpawns(aSpawn, bSpawn)
	//	diags = append(diags, extraDiags...)
	//	if skipSubgraph {
	//		return SkipSubgraph
	//	} else {
	//		return nil
	//	}
	//})

	return
}

type WalkFunc func(node interface{}) error

var SkipSubgraph = errors.New("skip subgraph")

func (cg *CompactGraph) Walk(roots []string, walkFunc WalkFunc) error {
	var toVisit []interface{}
	visited := make(map[interface{}]struct{})
	markForVisit := func(n interface{}) {
		if _, seen := visited[n]; !seen {
			toVisit = append(toVisit, n)
		}
	}
	for _, root := range roots {
		markForVisit((*cg)[root])
	}
	for len(toVisit) > 0 {
		var next interface{}
		next, toVisit = toVisit[0], toVisit[1:]
		visited[next] = struct{}{}
		err := walkFunc(next)
		if err == SkipSubgraph {
			continue
		} else if err != nil {
			return err
		}
		switch n := next.(type) {
		case *InputSet:
			for _, file := range n.Files {
				markForVisit(file)
			}
			for _, dir := range n.Directories {
				markForVisit(dir)
			}
			for _, inputSet := range n.TransitiveSets {
				markForVisit(inputSet)
			}
		case *Spawn:
			markForVisit(n.Inputs)
			markForVisit(n.Tools)
		}
	}
	return nil
}

func (cg *CompactGraph) ReduceToRoots(outputs []string) []string {
	notSeenAsInputs := make(map[string]struct{})
	for _, output := range outputs {
		notSeenAsInputs[output] = struct{}{}
	}

	_ = cg.Walk(outputs, func(node interface{}) error {
		switch n := node.(type) {
		case *File:
			// A top-level java_binary target will request all runtime jars corresponding to compilation jars, but there
			// is no path in the graph linking the target to the runtime jars with header compilation enabled. We should
			// not consider the individual runtime jars as roots.
			// TODO: Make this kind of logic configurable. Symlink actions are similarly not represented, so we may want
			//  a postprocessing step that adds synthetic edges between nodes.
			if strings.HasSuffix(n.Path(), "-hjar.jar") {
				delete(notSeenAsInputs, strings.TrimSuffix(n.Path(), "-hjar.jar")+".jar")
			}
			delete(notSeenAsInputs, n.Path())
		case *Directory:
			delete(notSeenAsInputs, n.Path())
		}
		return nil
	})

	var roots []string
	for root, _ := range notSeenAsInputs {
		roots = append(roots, root)
	}
	slices.Sort(roots)
	return roots
}

func (cg *CompactGraph) primaryOutputs() []string {
	var primaryOutputs []string
	for p, s := range *cg {
		if s.PrimaryOutputPath() == p {
			primaryOutputs = append(primaryOutputs, p)
		}
	}
	slices.Sort(primaryOutputs)
	return primaryOutputs
}

func diffSpawns(aSpawn *Spawn, bSpawn *Spawn) (diags []string, skipSubgraph bool) {
	envDiff := gocmp.Diff(aSpawn.Env, bSpawn.Env)
	if envDiff != "" {
		diags = append(diags, fmt.Sprintf("%s: environment changed: %s", bSpawn, envDiff))
	}
	argsDiff := gocmp.Diff(aSpawn.Args, bSpawn.Args)
	if argsDiff != "" {
		diags = append(diags, fmt.Sprintf("%s: arguments changed: %s", bSpawn, argsDiff))
	}
	paramFilesDiff := diffInputSets(aSpawn.ParamFiles, bSpawn.ParamFiles)
	if paramFilesDiff != "" {
		diags = append(diags, fmt.Sprintf("%s: param files changed: %s", bSpawn, paramFilesDiff))
	}
	toolsDiff := diffInputSets(aSpawn.Tools, bSpawn.Tools)
	if toolsDiff != "" {
		diags = append(diags, fmt.Sprintf("%s: tools changed: %s", bSpawn, toolsDiff))
	}
	inputsDiff := diffInputSets(aSpawn.Inputs, bSpawn.Inputs)
	if inputsDiff != "" {
		diags = append(diags, fmt.Sprintf("%s: inputs changed: %s", bSpawn, inputsDiff))
	}

	return
}

func diffInputSets(a, b *InputSet) string {
	pathsCertainlyUnchanged := a.AnalysisDigest() == b.AnalysisDigest()
	contentsCertainlyUnchanged := a.ExecutionDigest() == b.ExecutionDigest()

	if pathsCertainlyUnchanged && contentsCertainlyUnchanged {
		return ""
	}

	aInputs := a.Flatten()
	bInputs := b.Flatten()

	if !pathsCertainlyUnchanged {
		pathsDiff := gocmp.Diff(aInputs, bInputs, gocmp.Transformer("path", func(i Input) string { return i.Path() }))
		if pathsDiff != "" {
			return "paths changed: " + pathsDiff
		}
	}

	if !contentsCertainlyUnchanged {
		var contentsDiff []string
		for i, aInput := range aInputs {
			bInput := bInputs[i]
			if aInput.ExecutionDigest() != bInput.ExecutionDigest() {
				contentsDiff = append(contentsDiff, aInput.Path())
			}
		}
		if len(contentsDiff) > 0 {
			return "contents changed: " + strings.Join(contentsDiff, ", ")
		}
	}
	return ""
}

// setDifference computes the sorted slice a \ b for sorted slices a and b.
func setDifference(a, b []string) []string {
	var difference []string
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			difference = append(difference, a[i])
			i++
		case a[i] > b[j]:
			j++
		default:
			i++
			j++
		}
	}
	difference = append(difference, a[i:]...)
	return difference
}

// setIntersection computes the sorted slice a ∩ b for sorted slices a and b.
func setIntersection(a, b []string) []string {
	var intersection []string
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			intersection = append(intersection, a[i])
			i++
			j++
		}
	}
	return intersection
}
