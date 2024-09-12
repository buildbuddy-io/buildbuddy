package compactgraph

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protodelim"

	spawnproto "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

type CompactGraph map[string]*Spawn

func ReadCompactLog(in io.Reader) (CompactGraph, string, error) {
	d, err := zstd.NewReader(in)
	if err != nil {
		return nil, "", err
	}
	defer d.Close()
	r := bufio.NewReader(d)

	var hashFunction string
	var entry spawnproto.ExecLogEntry
	cg := make(CompactGraph)
	previousInputs := make(map[int32]Input)
	previousInputs[0] = emptyInputSet
	for {
		err = protodelim.UnmarshalFrom(r, &entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", err
		}
		switch entry.Type.(type) {
		case *spawnproto.ExecLogEntry_Invocation_:
			hashFunction = entry.GetInvocation().HashFunctionName
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
	return cg, hashFunction, nil
}

func Compare(old, new CompactGraph) (spawnDiffs []*spawn_diff.SpawnDiff) {
	oldPrimaryOutputs := old.primaryOutputs()
	newPrimaryOutputs := new.primaryOutputs()

	oldOnlyOutputs := old.ReduceToRoots(setDifference(oldPrimaryOutputs, newPrimaryOutputs))
	for _, path := range oldOnlyOutputs {
		spawn := old[path]
		spawnDiffs = append(spawnDiffs, &spawn_diff.SpawnDiff{
			PrimaryOutput: spawn.PrimaryOutputPath(),
			Target:        spawn.Label,
			Mnemonic:      spawn.Mnemonic,
			DiffType:      spawn_diff.SpawnDiff_OLD_ONLY,
		})
	}

	newOnlyOutputs := new.ReduceToRoots(setDifference(newPrimaryOutputs, oldPrimaryOutputs))
	for _, path := range newOnlyOutputs {
		spawn := new[path]
		spawnDiffs = append(spawnDiffs, &spawn_diff.SpawnDiff{
			PrimaryOutput: spawn.PrimaryOutputPath(),
			Target:        spawn.Label,
			Mnemonic:      spawn.Mnemonic,
			DiffType:      spawn_diff.SpawnDiff_NEW_ONLY,
		})
	}

	commonOutputs := setIntersection(oldPrimaryOutputs, newPrimaryOutputs)
	type diffResult struct {
		diff        *spawn_diff.SpawnDiff
		localChange bool
		affectedBy  []string
	}
	diffResults := sync.Map{}
	diffEG := errgroup.Group{}
	for _, output := range commonOutputs {
		diffEG.Go(func() error {
			aSpawn := old[output]
			bSpawn := new[output]
			diff, localChange, affectedBy := diffSpawns(aSpawn, bSpawn)
			diffResults.Store(output, &diffResult{
				diff:        diff,
				localChange: localChange,
				affectedBy:  affectedBy,
			})
			return nil
		})
	}
	_ = diffEG.Wait()

	// Sort the common outputs topologically according to the graph structure of b.
	bOutputsSorted := new.SortTopologically()
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
		spawn := new[output]
		foundTransitiveCause := false
		for _, affectedBy := range result.affectedBy {
			if otherResultEntry, ok := diffResults.Load(affectedBy); ok {
				foundTransitiveCause = true
				otherDiff := otherResultEntry.(*diffResult).diff
				if otherDiff.TransitivelyInvalidated == nil {
					otherDiff.TransitivelyInvalidated = make(map[string]uint32)
				}
				for k, v := range result.diff.TransitivelyInvalidated {
					otherDiff.TransitivelyInvalidated[k] += v
				}
				otherDiff.TransitivelyInvalidated[spawn.Mnemonic]++
			}
		}
		if len(result.diff.Diffs) > 0 && (result.localChange || !foundTransitiveCause) {
			spawnDiffs = append(spawnDiffs, result.diff)
			//transitiveEffectSuffix := formatTransitiveEffectSuffix(result.mnemonicAndCount)
			//diffs = append(diffs, fmt.Sprintf("%s%s", spawn, transitiveEffectSuffix))
			//diffs = append(diffs, fmt.Sprintf("  primary output: %s", output))
			//for _, diag := range result.diff {
			//	diffs = append(diffs, fmt.Sprintf("  %s", diag))
			//}
			//diffs = append(diffs, "")
		}
	}

	return
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
		cg.visitSuccessors(next, func(input interface{}) {
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
		cg.visitSuccessors(n, func(input interface{}) {
			toVisit = append(toVisit, input)
		})
	}
	slices.Reverse(ordered)
	return ordered
}

func (cg *CompactGraph) visitSuccessors(node interface{}, visitor func(input interface{})) {
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

func diffSpawns(old, new *Spawn) (diff *spawn_diff.SpawnDiff, localChange bool, affectedBy []string) {
	diff = &spawn_diff.SpawnDiff{
		PrimaryOutput: old.PrimaryOutputPath(),
		Target:        old.Label,
		Mnemonic:      old.Mnemonic,
	}

	if !maps.Equal(old.Env, new.Env) {
		localChange = true
		envDiff := &spawn_diff.DictDiff{
			OldChanged: make(map[string]string),
			NewChanged: make(map[string]string),
		}
		for key, value := range old.Env {
			if newValue, ok := new.Env[key]; !ok || value != newValue {
				envDiff.OldChanged[key] = value
			}
		}
		for key, value := range new.Env {
			if oldValue, ok := old.Env[key]; !ok || value != oldValue {
				envDiff.NewChanged[key] = value
			}
		}
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_Env{Env: envDiff}})
	}
	toolPathsDiff, toolContentsDiff := diffInputSets(old.Tools, new.Tools)
	if toolPathsDiff != nil {
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ToolPaths{ToolPaths: toolPathsDiff}})
	}
	if toolContentsDiff != nil {
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ToolContents{ToolContents: toolContentsDiff}})
	}
	inputPathsDiff, inputContentsDiff := diffInputSets(old.Inputs, new.Inputs)
	if inputPathsDiff != nil {
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_InputPaths{InputPaths: inputPathsDiff}})
	}
	if inputContentsDiff != nil {
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_InputContents{InputContents: inputContentsDiff}})
	}
	argsChanged := !slices.Equal(old.Args, new.Args)
	if argsChanged {
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_Args{
			Args: &spawn_diff.ListDiff{
				Old: old.Args,
				New: new.Args,
			},
		}})
	}
	paramFilePathsDiff, paramFileContentsDiff := diffInputSets(old.ParamFiles, new.ParamFiles)
	if paramFilePathsDiff != nil {
		localChange = true
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ParamFilePaths{ParamFilePaths: paramFilePathsDiff}})
	}
	if paramFileContentsDiff != nil {
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ParamFileContents{ParamFileContents: paramFileContentsDiff}})
	}

	// We assume that changes in the spawn's arguments are caused by changes to the spawn's input or tool paths if any.
	// This may not always be correct (e.g. adding a copt or adding a dep), but we still show the diff in this case
	// unless a transitive target is changed.
	if (argsChanged || paramFileContentsDiff != nil) && inputPathsDiff == nil && toolPathsDiff == nil {
		localChange = true
	}
	for _, fileDiff := range append(toolContentsDiff.FileDiffs, inputContentsDiff.FileDiffs...) {
		if isSourcePath(fileDiff.Path) {
			localChange = true
		} else {
			affectedBy = append(affectedBy, fileDiff.Path)
		}
	}
	// TODO: Report changes in the set of inputs if neither the contents nor the arguments changed.

	var oldOutputPaths []string
	for _, output := range old.Outputs {
		oldOutputPaths = append(oldOutputPaths, output.Path())
	}
	slices.Sort(oldOutputPaths)
	var newOutputPaths []string
	for _, output := range new.Outputs {
		newOutputPaths = append(newOutputPaths, output.Path())
	}
	slices.Sort(newOutputPaths)
	if !slices.Equal(oldOutputPaths, newOutputPaths) {
		localChange = true
		diff.Diffs = append(diff.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_OutputPaths{
			OutputPaths: &spawn_diff.StringSetDiff{
				OldOnly: setDifference(oldOutputPaths, newOutputPaths),
				NewOnly: setDifference(newOutputPaths, oldOutputPaths),
			},
		}})
	}

	if len(diff.Diffs) > 0 {
		return
	}

	var outputContentsDiffs []*spawn_diff.FileDiff
	for i, oldOutput := range old.Outputs {
		newOutput := new.Outputs[i]
		if !slices.Equal(oldOutput.ShallowContentDigest(), newOutput.ShallowContentDigest()) {
			outputContentsDiffs = append(outputContentsDiffs, &spawn_diff.FileDiff{
				Path: oldOutput.Path(),
				Old: &spawn_diff.FileDiff_OldDigest{
					OldDigest: hex.EncodeToString(oldOutput.ShallowContentDigest()),
				},
				New: &spawn_diff.FileDiff_NewDigest{
					NewDigest: hex.EncodeToString(newOutput.ShallowContentDigest()),
				},
			})
		}
	}

	return
}

func diffInputSets(old, new *InputSet) (pathsDiff *spawn_diff.StringSetDiff, contentsDiff *spawn_diff.FileSetDiff) {
	pathsCertainlyUnchanged := slices.Equal(old.ShallowPathDigest(), new.ShallowPathDigest())
	contentsCertainlyUnchanged := slices.Equal(old.ShallowContentDigest(), new.ShallowContentDigest())
	if pathsCertainlyUnchanged && contentsCertainlyUnchanged {
		return nil, nil
	}

	oldInputs := old.Flatten()
	newInputs := new.Flatten()

	if !pathsCertainlyUnchanged && !slices.EqualFunc(oldInputs, newInputs, func(a, b Input) bool { return a.Path() == b.Path() }) {
		oldPaths := make([]string, len(oldInputs))
		for i, input := range oldInputs {
			oldPaths[i] = input.Path()
		}
		newPaths := make([]string, len(newInputs))
		for i, input := range newInputs {
			newPaths[i] = input.Path()
		}
		return &spawn_diff.StringSetDiff{
			OldOnly: setDifference(oldPaths, newPaths),
			NewOnly: setDifference(newPaths, oldPaths),
		}, nil
	}

	if !contentsCertainlyUnchanged {
		var fileDiffs []*spawn_diff.FileDiff
		for i, oldInput := range oldInputs {
			newInput := newInputs[i]
			if !slices.Equal(oldInput.ShallowContentDigest(), newInput.ShallowContentDigest()) {
				fileDiffs = append(fileDiffs, &spawn_diff.FileDiff{
					Path: oldInput.Path(),
					Old: &spawn_diff.FileDiff_OldDigest{
						OldDigest: hex.EncodeToString(oldInput.ShallowContentDigest()),
					},
					New: &spawn_diff.FileDiff_NewDigest{
						NewDigest: hex.EncodeToString(newInput.ShallowContentDigest()),
					},
				})
			}
		}
		if len(fileDiffs) > 0 {
			return nil, &spawn_diff.FileSetDiff{FileDiffs: fileDiffs}
		}
	}
	return nil, nil
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
