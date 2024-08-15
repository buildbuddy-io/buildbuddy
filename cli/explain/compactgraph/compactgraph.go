package compactgraph

import (
	"bufio"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"

	spawnproto "github.com/buildbuddy-io/buildbuddy/proto/spawn"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protodelim"

	gocmp "github.com/google/go-cmp/cmp"
)

type CompactGraph map[string]*Spawn

func ReadCompactLog(in io.Reader) (CompactGraph, error) {
	d, err := zstd.NewReader(in)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	r := bufio.NewReader(d)

	var entry spawnproto.ExecLogEntry
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
		case *spawnproto.ExecLogEntry_Invocation_:
			hashFunction := entry.GetInvocation().HashFunctionName
			if hashFunction != "SHA-256" {
				return nil, fmt.Errorf("unsupported hash function: %q", hashFunction)
			}
		case *spawnproto.ExecLogEntry_File_:
			file := ProtoToFile(entry.GetFile())
			previousInputs[entry.Id] = file
		case *spawnproto.ExecLogEntry_Directory_:
			dir := ProtoToDirectory(entry.GetDirectory())
			previousInputs[entry.Id] = dir
		case *spawnproto.ExecLogEntry_InputSet_:
			inputSet := ProtoToInputSet(entry.GetInputSet(), previousInputs)
			previousInputs[entry.Id] = inputSet
		case *spawnproto.ExecLogEntry_Spawn_:
			spawn, outputPaths := ProtoToSpawn(entry.GetSpawn(), previousInputs)
			if spawn != nil {
				for _, path := range outputPaths {
					cg[path] = spawn
				}
			}
		case *spawnproto.ExecLogEntry_UnresolvedSymlink_:
			panic(fmt.Sprintf("unresolved symlinks are unsupported, got %s --> %s", entry.GetUnresolvedSymlink().Path, entry.GetUnresolvedSymlink().TargetPath))
		}
	}
	return cg, nil
}

func Compare(a, b CompactGraph) (diags []string) {
	aPrimaryOutputs := a.primaryOutputs()
	bPrimaryOutputs := b.primaryOutputs()

	bExtraOutputs := b.ReduceToRoots(setDifference(bPrimaryOutputs, aPrimaryOutputs))
	for _, path := range bExtraOutputs {
		spawn := b[path]
		diags = append(diags, fmt.Sprintf("%s  \n  primary output: %s\n  new top-level spawn", spawn, spawn.PrimaryOutputPath()))
	}

	commonOutputs := setIntersection(aPrimaryOutputs, bPrimaryOutputs)
	type diffResult struct {
		diffs            []string
		localChange      bool
		affectedBy       []string
		mnemonicAndCount map[string]uint
	}
	diffResults := sync.Map{}
	diffEG := errgroup.Group{}
	for _, output := range commonOutputs {
		diffEG.Go(func() error {
			aSpawn := a[output]
			bSpawn := b[output]
			diff, localChange, affectedBy := diffSpawns(aSpawn, bSpawn)
			diffResults.Store(output, &diffResult{
				diffs:       diff,
				localChange: localChange,
				affectedBy:  affectedBy,
			})
			return nil
		})
	}
	_ = diffEG.Wait()

	// Sort the common outputs topologically according to the graph structure of b.
	bOutputsSorted := b.SortTopologically()
	commonOutputsSet := make(map[string]struct{})
	for _, output := range commonOutputs {
		commonOutputsSet[output] = struct{}{}
	}
	n := 0
	for _, x := range bOutputsSorted {
		if _, ok := commonOutputsSet[x]; ok {
			bOutputsSorted[n] = x
			n++
		}
	}
	commonOutputsSorted := bOutputsSorted[:n]

	// Visit spawns in topological order and attribute their diffs to transitive dependencies if possible.
	for _, output := range commonOutputsSorted {
		resultEntry, _ := diffResults.Load(output)
		result := resultEntry.(*diffResult)
		spawn := b[output]
		foundTransitiveCause := false
		for _, affectedBy := range result.affectedBy {
			if otherResultEntry, ok := diffResults.Load(affectedBy); ok {
				foundTransitiveCause = true
				otherResult := otherResultEntry.(*diffResult)
				if otherResult.mnemonicAndCount == nil {
					otherResult.mnemonicAndCount = make(map[string]uint)
				}
				for k, v := range result.mnemonicAndCount {
					otherResult.mnemonicAndCount[k] += v
				}
				otherResult.mnemonicAndCount[spawn.Mnemonic]++
			}
		}
		if len(result.diffs) > 0 && (result.localChange || !foundTransitiveCause) {
			transitiveEffectSuffix := formatTransitiveEffectSuffix(result.mnemonicAndCount)
			diags = append(diags, fmt.Sprintf("%s%s", spawn, transitiveEffectSuffix))
			diags = append(diags, fmt.Sprintf("  primary output: %s", output))
			for _, diag := range result.diffs {
				diags = append(diags, fmt.Sprintf("  %s", diag))
			}
			diags = append(diags, "")
		}
	}

	return
}

func formatTransitiveEffectSuffix(mnemonicAndCount map[string]uint) string {
	if len(mnemonicAndCount) == 0 {
		return ""
	}

	type kv struct {
		Mnemonic string
		Count    uint
	}
	var sorted []kv
	for k, v := range mnemonicAndCount {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		this, that := sorted[i], sorted[j]
		if this.Count != that.Count {
			return this.Count > that.Count
		}
		return this.Mnemonic < that.Mnemonic
	})

	var parts []string
	for _, mc := range sorted {
		parts = append(parts, fmt.Sprintf("%d %s", mc.Count, mc.Mnemonic))
	}
	return fmt.Sprintf(" (transitively invalidated: %s)", strings.Join(parts, ", "))
}

type WalkFunc func(node interface{})

func (cg *CompactGraph) Walk(roots []string, walkFunc WalkFunc) {
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
		walkFunc(next)
		cg.visit(next, func(input interface{}) {
			markForVisit(input)
		})
	}
}

func (cg *CompactGraph) ReduceToRoots(outputs []string) []string {
	notSeenAsInputs := make(map[string]struct{})
	for _, output := range outputs {
		notSeenAsInputs[output] = struct{}{}
	}

	cg.Walk(outputs, func(node interface{}) {
		switch n := node.(type) {
		case *File:
		case *Directory:
			delete(notSeenAsInputs, n.Path())
		}
	})

	var roots []string
	for root, _ := range notSeenAsInputs {
		roots = append(roots, root)
	}
	slices.Sort(roots)
	return roots
}

func (cg *CompactGraph) SortTopologically() []string {
	var toVisit []interface{}
	for _, spawn := range *cg {
		toVisit = append(toVisit, spawn)
	}
	sort.Slice(toVisit, func(i, j int) bool {
		return toVisit[i].(*Spawn).PrimaryOutputPath() < toVisit[j].(*Spawn).PrimaryOutputPath()
	})

	ordered := make([]string, 0, len(*cg))
	state := make(map[interface{}]bool)
	for len(toVisit) > 0 {
		n := toVisit[len(toVisit)-1]
		toVisit = toVisit[:len(toVisit)-1]
		if done, seen := state[n]; seen {
			if !done {
				state[n] = true
				if spawn, ok := n.(*Spawn); ok {
					ordered = append(ordered, spawn.PrimaryOutputPath())
				}
			}
			continue
		}
		state[n] = false
		toVisit = append(toVisit, n)
		cg.visit(n, func(input interface{}) {
			toVisit = append(toVisit, input)
		})
	}
	slices.Reverse(ordered)
	return ordered
}

func (cg *CompactGraph) visit(node interface{}, visitor func(input interface{})) {
	switch n := node.(type) {
	case *File:
	case *Directory:
		if !isSourcePath(n.Path()) {
			spawn, ok := (*cg)[n.Path()]
			if ok {
				visitor(spawn)
			}
		}
	case *InputSet:
		for _, file := range n.Files {
			visitor(file)
		}
		for _, dir := range n.Directories {
			visitor(dir)
		}
		for _, inputSet := range n.TransitiveSets {
			visitor(inputSet)
		}
	case *Spawn:
		visitor(n.Tools)
		visitor(n.Inputs)
	}
}

func (cg *CompactGraph) primaryOutputs() []string {
	var primaryOutputs []string
	for path, spawn := range *cg {
		if spawn.PrimaryOutputPath() == path {
			primaryOutputs = append(primaryOutputs, path)
		}
	}
	slices.Sort(primaryOutputs)
	return primaryOutputs
}

func diffSpawns(a, b *Spawn) (diffs []string, localChange bool, affectedBy []string) {
	envDiff := gocmp.Diff(a.Env, b.Env)
	if envDiff != "" {
		localChange = true
		diffs = append(diffs, fmt.Sprintf("environment changed: %s", envDiff))
	}
	toolsPathDiff, toolsContentChanged := diffInputSets(a.Tools, b.Tools)
	if toolsPathDiff != "" {
		diffs = append(diffs, fmt.Sprintf("set of tools changed: %s", toolsPathDiff))
	}
	if len(toolsContentChanged) > 0 {
		diffs = append(diffs, fmt.Sprintf("tools changed: %s", strings.Join(toolsContentChanged, ", ")))
	}
	inputsPathDiff, inputsContentChanged := diffInputSets(a.Inputs, b.Inputs)
	if inputsPathDiff != "" {
		diffs = append(diffs, fmt.Sprintf("set of inputs changed: %s", inputsPathDiff))
	}
	if len(inputsContentChanged) > 0 {
		diffs = append(diffs, fmt.Sprintf("inputs changed: %s", strings.Join(inputsContentChanged, ", ")))
	}
	argsDiff := gocmp.Diff(a.Args, b.Args)
	if argsDiff != "" {
		diffs = append(diffs, fmt.Sprintf("arguments changed: %s", argsDiff))
	}
	paramFilesDiff, _ := diffInputSets(a.ParamFiles, b.ParamFiles)
	if paramFilesDiff != "" {
		diffs = append(diffs, fmt.Sprintf("param files changed: %s", paramFilesDiff))
	}

	// We assume that changes in the spawn's arguments are caused by changes to the spawn's input or tool paths if any.
	// This may not always be correct (e.g. adding a copt or adding a dep), but we still show the diff in this case
	// unless a transitive target is changed.
	if (argsDiff != "" || paramFilesDiff != "") && inputsPathDiff == "" && toolsPathDiff == "" {
		localChange = true
	}
	for _, input := range append(toolsContentChanged, inputsContentChanged...) {
		if isSourcePath(input) {
			localChange = true
		} else {
			affectedBy = append(affectedBy, input)
		}
	}
	// TODO: Report changes in the set of inputs if neither the contents nor the arguments changed.

	var aOutputNames []string
	for _, output := range a.Outputs {
		aOutputNames = append(aOutputNames, output.Path())
	}
	var bOutputNames []string
	for _, output := range b.Outputs {
		bOutputNames = append(bOutputNames, output.Path())
	}
	outputNamesDiff := gocmp.Diff(aOutputNames, bOutputNames)
	if outputNamesDiff != "" {
		localChange = true
		diffs = append(diffs, fmt.Sprintf("set of outputs changed: %s", outputNamesDiff))
	}

	if len(diffs) > 0 {
		return
	}

	var contentsDiff []string
	for i, aOutput := range a.Outputs {
		bOutput := b.Outputs[i]
		if aOutput.ShallowContentDigest() != bOutput.ShallowContentDigest() {
			contentsDiff = append(contentsDiff, aOutput.Path())
		}
	}
	if len(contentsDiff) > 0 {
		diffs = append(diffs, fmt.Sprintf("contents of outputs changed non-hermetically: %s", strings.Join(contentsDiff, ", ")))
	}

	return
}

func diffInputSets(a, b *InputSet) (pathsDiff string, changedFiles []string) {
	pathsCertainlyUnchanged := a.ShallowPathDigest() == b.ShallowPathDigest()
	contentsCertainlyUnchanged := a.ShallowContentDigest() == b.ShallowContentDigest()
	if pathsCertainlyUnchanged && contentsCertainlyUnchanged {
		return
	}

	aInputs := a.Flatten()
	bInputs := b.Flatten()

	if !pathsCertainlyUnchanged {
		pathsDiff = gocmp.Diff(aInputs, bInputs, gocmp.Transformer("path", func(i Input) string { return i.Path() }))
	}

	if !contentsCertainlyUnchanged {
		for i, aInput := range aInputs {
			bInput := bInputs[i]
			if aInput.ShallowContentDigest() != bInput.ShallowContentDigest() {
				changedFiles = append(changedFiles, aInput.Path())
			}
		}
	}
	return
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
