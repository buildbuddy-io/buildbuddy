package compactgraph

import (
	"bufio"
	"cmp"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protodelim"

	spawnproto "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

// CompactGraph represents the compact execution log as a directed acyclic graph of spawns.
type CompactGraph struct {
	// The keys are the output paths of all spawns in the graph and each spawn contains references to other execution
	// log entries, such as input files, directories, and input sets. The edges between spawns are tracked implicitly by
	// matching the output paths of the spawns to the input paths of the referenced entries.
	spawns map[string]*Spawn
	// symlinkResolutions maps the paths of artifacts that are known to be symlinks to their target paths (through arbitrary
	// levels of indirection).
	symlinkResolutions map[string]string
	settings           globalSettings
}

type globalSettings struct {
	hashFunction               string
	workspaceRunfilesDirectory string
	legacyExternalRunfiles     bool
	hasEmptyFiles              bool
}

// ReadCompactLog reads a compact execution log from the given reader and returns the graph of spawns, the hash function
// used to compute the file digests, and an error if any.
func ReadCompactLog(in io.Reader) (*CompactGraph, error) {
	diffEG := errgroup.Group{}

	// A size > 1 shows noticeable performance improvements in benchmarks. Larger sizes don't show relevant further
	// improvements.
	entries := make(chan *spawnproto.ExecLogEntry, 100)
	diffEG.Go(func() error {
		d, err := zstd.NewReader(in)
		if err != nil {
			return err
		}
		defer d.Close()
		r := bufio.NewReader(d)

		unmarshalOpts := protodelim.UnmarshalOptions{MaxSize: -1}
		for {
			var entry spawnproto.ExecLogEntry
			err = unmarshalOpts.UnmarshalFrom(r, &entry)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			entries <- &entry
		}
		close(entries)
		return nil
	})

	cg := &CompactGraph{}
	diffEG.Go(func() error {
		cg.spawns = make(map[string]*Spawn)
		previousInputs := make(map[uint32]Input)
		previousInputs[0] = emptyInputSet
		for entry := range entries {
			switch entry.Type.(type) {
			case *spawnproto.ExecLogEntry_Invocation_:
				if entry.GetInvocation().GetSiblingRepositoryLayout() {
					return errors.New("--experimental_sibling_repository_layout is not supported")
				}
				cg.settings.hashFunction = entry.GetInvocation().HashFunctionName
				cg.settings.workspaceRunfilesDirectory = entry.GetInvocation().WorkspaceRunfilesDirectory
			case *spawnproto.ExecLogEntry_File_:
				file := protoToFile(entry.GetFile(), cg.settings.hashFunction)
				previousInputs[entry.Id] = file
			case *spawnproto.ExecLogEntry_UnresolvedSymlink_:
				symlink := protoToSymlink(entry.GetUnresolvedSymlink())
				previousInputs[entry.Id] = symlink
			case *spawnproto.ExecLogEntry_Directory_:
				dir := protoToDirectory(entry.GetDirectory(), cg.settings.hashFunction)
				previousInputs[entry.Id] = dir
			case *spawnproto.ExecLogEntry_InputSet_:
				inputSet := protoToInputSet(entry.GetInputSet(), previousInputs)
				previousInputs[entry.Id] = inputSet
			case *spawnproto.ExecLogEntry_Spawn_:
				spawn, outputPaths := protoToSpawn(entry.GetSpawn(), previousInputs)
				if spawn != nil {
					for _, p := range outputPaths {
						cg.spawns[p] = spawn
					}
				}
			case *spawnproto.ExecLogEntry_SymlinkAction_:
				symlinkAction := entry.GetSymlinkAction()
				target := symlinkAction.InputPath
				if resolvedTarget, ok := cg.symlinkResolutions[target]; ok {
					target = resolvedTarget
				}
				if cg.symlinkResolutions == nil {
					cg.symlinkResolutions = make(map[string]string)
				}
				cg.symlinkResolutions[symlinkAction.OutputPath] = target
			case *spawnproto.ExecLogEntry_SymlinkEntrySet_:
				symlinkEntrySet := protoToSymlinkEntrySet(entry.GetSymlinkEntrySet(), previousInputs)
				previousInputs[entry.Id] = symlinkEntrySet
			case *spawnproto.ExecLogEntry_RunfilesTree_:
				runfilesTreeProto := entry.GetRunfilesTree()
				cg.settings.legacyExternalRunfiles = cg.settings.legacyExternalRunfiles || runfilesTreeProto.LegacyExternalRunfiles
				cg.settings.hasEmptyFiles = cg.settings.hasEmptyFiles || len(runfilesTreeProto.EmptyFiles) > 0
				runfilesTree := protoToRunfilesTree(runfilesTreeProto, previousInputs, cg.settings.hashFunction)
				previousInputs[entry.Id] = addRunfilesTreeSpawn(cg, runfilesTree)
			default:
				log.Fatalf("unexpected entry type: %T", entry.Type)
			}
		}
		return nil
	})

	err := diffEG.Wait()
	if err != nil {
		return nil, err
	}
	return cg, nil
}

// This synthetic mnemonic contains a space to ensure it doesn't conflict with any real mnemonic.
const runfilesTreeSpawnMnemonic = "Runfiles directory"

// addRunfilesTreeSpawn adds a synthetic spawn creating the given runfiles tree and returns its output, which is an
// opaque directory that represents the runfiles tree. This is used to structurally "intern" the runfiles tree and diff
// it only once, even if it is used as a tool in multiple spawns or is an input to a test with multiple attempts.
func addRunfilesTreeSpawn(cg *CompactGraph, tree *RunfilesTree) Input {
	output := &OpaqueRunfilesDirectory{tree}
	s := &Spawn{
		Mnemonic: runfilesTreeSpawnMnemonic,
		Inputs: &InputSet{
			DirectEntries:      []Input{tree},
			shallowPathHash:    tree.ShallowPathHash(),
			shallowContentHash: tree.ShallowContentHash(),
		},
		Tools:      emptyInputSet,
		ParamFiles: emptyInputSet,
		Outputs:    []Input{output},
	}
	// The spawn producing the executable corresponding to this runfiles tree may not have been run in the current
	// build, but if it has, we can attach a label to the runfiles tree.
	runfilesOwner := cg.resolveSymlinksFunc()(strings.TrimSuffix(tree.Path(), ".runfiles"))
	if owner, ok := cg.spawns[runfilesOwner]; ok {
		s.TargetLabel = owner.TargetLabel
	}
	cg.spawns[tree.Path()] = s
	return output
}

func Diff(old, new *CompactGraph) ([]*spawn_diff.SpawnDiff, error) {
	if old.settings != new.settings {
		settingDiffs := diffSettings(&old.settings, &new.settings)
		return nil, fmt.Errorf("global settings changed:\n%s", strings.Join(settingDiffs, "\n"))
	}

	var spawnDiffs []*spawn_diff.SpawnDiff

	oldPrimaryOutputs := old.primaryOutputs()
	newPrimaryOutputs := new.primaryOutputs()

	oldOnlyPrimaryOutputs := setDifference(oldPrimaryOutputs, newPrimaryOutputs)
	oldOnlyTopLevelOutputs := old.findRootSet(oldOnlyPrimaryOutputs)
	for _, p := range oldOnlyPrimaryOutputs {
		sd := newDiff(old.spawns[p])
		_, topLevel := oldOnlyTopLevelOutputs[p]
		sd.Diff = &spawn_diff.SpawnDiff_OldOnly{OldOnly: &spawn_diff.OldOnly{
			TopLevel: topLevel,
		}}
		spawnDiffs = append(spawnDiffs, sd)
	}
	newOnlyPrimaryOutputs := setDifference(newPrimaryOutputs, oldPrimaryOutputs)
	newOnlyTopLevelOutputs := new.findRootSet(newOnlyPrimaryOutputs)
	for _, p := range newOnlyPrimaryOutputs {
		sd := newDiff(new.spawns[p])
		_, topLevel := newOnlyTopLevelOutputs[p]
		sd.Diff = &spawn_diff.SpawnDiff_NewOnly{NewOnly: &spawn_diff.NewOnly{
			TopLevel: topLevel,
		}}
		spawnDiffs = append(spawnDiffs, sd)
	}

	commonOutputs := setIntersection(oldPrimaryOutputs, newPrimaryOutputs)
	var commonSpawnOutputs, commonRunfilesTrees []string
	for _, output := range commonOutputs {
		oldIsRunfilesTree := old.spawns[output].Mnemonic == runfilesTreeSpawnMnemonic
		newIsRunfilesTree := new.spawns[output].Mnemonic == runfilesTreeSpawnMnemonic
		if oldIsRunfilesTree || newIsRunfilesTree {
			if !oldIsRunfilesTree || !newIsRunfilesTree {
				// This can only happen in pathological cases where an executable in one build no longer exists in
				// another build, but an output at <executable>.runfiles does.
				log.Fatalf("inconsistent runfiles trees %s: %v vs. %v", output, old.spawns[output], new.spawns[output])
			}
			commonRunfilesTrees = append(commonRunfilesTrees, output)
		} else {
			commonSpawnOutputs = append(commonSpawnOutputs, output)
		}
	}

	oldResolveSymlinks := old.resolveSymlinksFunc()
	newResolveSymlinks := new.resolveSymlinksFunc()
	type diffResult struct {
		spawnDiff     *spawn_diff.SpawnDiff
		localChange   bool
		invalidatedBy []string
		invalidates   []any
	}
	diffResults := sync.Map{}
	diffWG := sync.WaitGroup{}
	// Diff runfiles tree spawns first to compute exact content hashes that are used when diffing other spawns.
	for _, output := range commonRunfilesTrees {
		diffWG.Add(1)
		go func() {
			defer diffWG.Done()
			spawnDiff, localChange, invalidatedBy := diffRunfilesTrees(old.spawns[output], new.spawns[output], oldResolveSymlinks, newResolveSymlinks)
			diffResults.Store(output, &diffResult{
				spawnDiff:     spawnDiff,
				localChange:   localChange,
				invalidatedBy: invalidatedBy,
			})
		}()
	}
	diffWG.Wait()
	for _, output := range commonSpawnOutputs {
		diffWG.Add(1)
		go func() {
			defer diffWG.Done()
			spawnDiff, localChange, invalidatedBy := diffSpawns(old.spawns[output], new.spawns[output], oldResolveSymlinks, newResolveSymlinks)
			diffResults.Store(output, &diffResult{
				spawnDiff:     spawnDiff,
				localChange:   localChange,
				invalidatedBy: invalidatedBy,
			})
		}()
	}
	diffWG.Wait()

	// Visit spawns in topological order and attribute their diffs to transitive dependencies if possible.
	commonOutputsSet := make(map[string]struct{})
	for _, output := range commonOutputs {
		commonOutputsSet[output] = struct{}{}
	}
	for _, output := range new.sortedPrimaryOutputs() {
		if _, ok := commonOutputsSet[output]; !ok {
			continue
		}
		resultEntry, _ := diffResults.Load(output)
		result := resultEntry.(*diffResult)
		spawn := new.spawns[output]
		foundTransitiveCause := false
		// Get the deduplicated primary outputs for those spawns referenced via invalidatedBy.
		invalidatedByPrimaryOutput := make(map[string]struct{})
		for _, invalidatedBy := range result.invalidatedBy {
			if s, ok := new.spawns[invalidatedBy]; ok {
				invalidatedByPrimaryOutput[s.PrimaryOutputPath()] = struct{}{}
			}
		}
		for invalidatedBy, _ := range invalidatedByPrimaryOutput {
			if invalidatingResultEntry, ok := diffResults.Load(invalidatedBy); ok {
				invalidatingResult := invalidatingResultEntry.(*diffResult)
				foundTransitiveCause = true
				// Intentionally not flattening the slice here to avoid quadratic complexity when there are many
				// transitively invalidated target, but few transitive causes. Quadratic complexity can't be avoided in
				// the general case.
				invalidatingResult.invalidates = append(invalidatingResult.invalidates, result.invalidates, spawn)
			}
		}
		if len(result.spawnDiff.GetModified().Diffs) > 0 && (result.localChange || !foundTransitiveCause) {
			if len(result.invalidates) > 0 {
				// result.invalidates isn't modified after this point as the spawns are visited in topological order.
				diffWG.Add(1)
				go func() {
					defer diffWG.Done()
					result.spawnDiff.GetModified().TransitivelyInvalidated = flattenInvalidates(result.invalidates)
				}()
			}
			spawnDiffs = append(spawnDiffs, result.spawnDiff)
		}
	}
	diffWG.Wait()

	return spawnDiffs, nil
}

// flattenInvalidates flattens a tree of Spawn nodes into a deduplicated map of mnemonic to count of transitively
// invalidated spawns.
func flattenInvalidates(invalidates []any) map[string]uint32 {
	transitivelyInvalidated := make(map[string]uint32)
	spawnsSeen := make(map[*Spawn]struct{})
	toVisit := invalidates
	for len(toVisit) > 0 {
		var n any
		n, toVisit = toVisit[0], toVisit[1:]
		switch n := n.(type) {
		case *Spawn:
			if _, seen := spawnsSeen[n]; !seen {
				spawnsSeen[n] = struct{}{}
				transitivelyInvalidated[n.Mnemonic]++
			}
		default:
			// If n is not a Spawn, it must be a slice of Spawns or slices.
			toVisit = append(toVisit, n.([]any)...)
		}
	}
	return transitivelyInvalidated
}

func diffSettings(old, new *globalSettings) []string {
	var settingDiffs []string
	if old.hashFunction != new.hashFunction {
		settingDiffs = append(settingDiffs, fmt.Sprintf("  --digest_function: %s -> %s\n", old.hashFunction, new.hashFunction))
	}
	if old.workspaceRunfilesDirectory != new.workspaceRunfilesDirectory {
		oldUsesLegacyExeclog := old.workspaceRunfilesDirectory == ""
		newUsesLegacyExeclog := new.workspaceRunfilesDirectory == ""
		oldUsesBzlmod := old.workspaceRunfilesDirectory == "_main"
		newUsesBzlmod := new.workspaceRunfilesDirectory == "_main"
		if oldUsesLegacyExeclog != newUsesLegacyExeclog {
			settingDiffs = append(settingDiffs, fmt.Sprintf("  Bazel 7.4.0 or higher: %t -> %t", !oldUsesLegacyExeclog, !newUsesLegacyExeclog))
		} else if oldUsesBzlmod != newUsesBzlmod {
			settingDiffs = append(settingDiffs, fmt.Sprintf("  --enable_bzlmod: %t -> %t", oldUsesBzlmod, newUsesBzlmod))
		} else {
			settingDiffs = append(settingDiffs, fmt.Sprintf("  WORKSPACE name: %s -> %s", old.workspaceRunfilesDirectory, new.workspaceRunfilesDirectory))
		}
	}
	if old.legacyExternalRunfiles != new.legacyExternalRunfiles {
		settingDiffs = append(settingDiffs, fmt.Sprintf("  --legacy_external_runfiles: %t -> %t", old.legacyExternalRunfiles, new.legacyExternalRunfiles))
	}
	if old.hasEmptyFiles != new.hasEmptyFiles {
		settingDiffs = append(settingDiffs, fmt.Sprintf("  --incompatible_default_to_explicit_init_py: %t -> %t", !old.hasEmptyFiles, !new.hasEmptyFiles))
	}
	return settingDiffs
}

// findRootSet returns the subset of outputs that are not inputs to any other spawn in the subgraph corresponding to
// the given outputs.
func (cg *CompactGraph) findRootSet(outputs []string) map[string]struct{} {
	rootsSet := make(map[string]struct{}, len(outputs))
	for _, output := range outputs {
		rootsSet[output] = struct{}{}
	}

	// Visit all nodes in the graph and remove those with incoming edges from the set of roots.
	var toVisit []any
	visited := make(map[any]struct{})
	markForVisit := func(n any) {
		if _, seen := visited[n]; !seen {
			toVisit = append(toVisit, n)
			visited[n] = struct{}{}
		}
	}
	for _, output := range outputs {
		markForVisit(cg.spawns[output])
	}
	for len(toVisit) > 0 {
		var node any
		node, toVisit = toVisit[0], toVisit[1:]
		switch n := node.(type) {
		case *File:
			delete(rootsSet, n.Path())
		case *Directory:
			delete(rootsSet, n.Path())
		}
		cg.visitSuccessors(node, markForVisit)
	}
	return rootsSet
}

// sortedPrimaryOutputs returns the primary output paths of the spawns in topological order.
func (cg *CompactGraph) sortedPrimaryOutputs() []string {
	toVisit := make([]any, 0, len(cg.spawns))
	for output, spawn := range cg.spawns {
		if spawn.PrimaryOutputPath() == output {
			toVisit = append(toVisit, spawn)
		}
	}
	sort.Slice(toVisit, func(i, j int) bool {
		return toVisit[i].(*Spawn).PrimaryOutputPath() < toVisit[j].(*Spawn).PrimaryOutputPath()
	})

	ordered := make([]string, 0, len(cg.spawns))
	// An entry is present in the map if it has been visited and its state is true if it either isn't a spawn or its
	// been visited again (necessarily after all its successors have been visited).
	state := make(map[any]bool)
	for len(toVisit) > 0 {
		n := toVisit[len(toVisit)-1]
		toVisit = toVisit[:len(toVisit)-1]
		if done, seen := state[n]; seen {
			if !done {
				state[n] = true
				ordered = append(ordered, n.(*Spawn).PrimaryOutputPath())
			}
			continue
		}
		// Only spawns need to be revisited.
		if _, isSpawn := n.(*Spawn); isSpawn {
			state[n] = false
			toVisit = append(toVisit, n)
		} else {
			state[n] = true
		}
		cg.visitSuccessors(n, func(input any) {
			toVisit = append(toVisit, input)
		})
	}
	slices.Reverse(ordered)
	return ordered
}

func (cg *CompactGraph) visitSuccessors(node any, visitor func(input any)) {
	switch n := node.(type) {
	case *File:
		if spawn, ok := cg.spawns[n.Path()]; ok {
			visitor(spawn)
		}
	case *Directory:
		if spawn, ok := cg.spawns[n.Path()]; ok {
			visitor(spawn)
		}
	case *InputSet:
		for _, input := range n.DirectEntries {
			visitor(input)
		}
		for _, transitiveSet := range n.TransitiveSets {
			visitor(transitiveSet)
		}
	case *SymlinkEntrySet:
		targets := maps.Values(n.directEntries)
		slices.SortFunc(targets, func(a, b Input) int {
			return cmp.Compare(a.Path(), b.Path())
		})
		for _, target := range targets {
			visitor(target)
		}
		for _, transitiveSet := range n.transitiveSets {
			visitor(transitiveSet)
		}
	case *RunfilesTree:
		visitor(n.Artifacts)
		visitor(n.Symlinks)
		visitor(n.RootSymlinks)
		// The repo mapping manifest is a synthetic File not produced by any Spawn, so we don't need to visit it.
	case *OpaqueRunfilesDirectory:
		visitor(cg.spawns[n.Path()])
	case *Spawn:
		visitor(n.Inputs)
		// Tools are a subset of all inputs, so we don't need to visit them separately.
	}
}

func (cg *CompactGraph) primaryOutputs() []string {
	var primaryOutputs []string
	for p, spawn := range cg.spawns {
		if spawn.PrimaryOutputPath() == p {
			primaryOutputs = append(primaryOutputs, p)
		}
	}
	slices.Sort(primaryOutputs)
	return primaryOutputs
}

func (cg *CompactGraph) resolveSymlinksFunc() func(string) string {
	return func(p string) string {
		// Symlinks are resolved deeply before being stored in the map, so a single lookup is sufficient.
		if resolved, ok := cg.symlinkResolutions[p]; ok {
			return resolved
		}
		return p
	}
}

func diffSpawns(old, new *Spawn, oldResolveSymlinks, newResolveSymlinks func(string) string) (diff *spawn_diff.SpawnDiff, localChange bool, invalidatedBy []string) {
	diff = newDiff(new)
	m := &spawn_diff.Modified{}
	diff.Diff = &spawn_diff.SpawnDiff_Modified{Modified: m}

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
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_Env{Env: envDiff}})
	}
	inputPathsDiff, inputContentsDiff := diffInputSets(old.Inputs, new.Inputs, oldResolveSymlinks, newResolveSymlinks)
	if inputPathsDiff != nil {
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_InputPaths{InputPaths: inputPathsDiff}})
	}
	if inputContentsDiff != nil {
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_InputContents{InputContents: inputContentsDiff}})
	}
	// Tools are a subset of all inputs, so we only need to compare the paths, not the contents.
	toolPathsDiff := diffInputSetsIgnoringContents(old.Tools, new.Tools, oldResolveSymlinks, newResolveSymlinks)
	if toolPathsDiff != nil {
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ToolPaths{ToolPaths: toolPathsDiff}})
	}
	argsChanged := !slices.Equal(old.Args, new.Args)
	if argsChanged {
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_Args{
			Args: &spawn_diff.ListDiff{
				Old: old.Args,
				New: new.Args,
			},
		}})
	}
	paramFilePathsDiff, paramFileContentsDiff := diffInputSets(old.ParamFiles, new.ParamFiles, oldResolveSymlinks, newResolveSymlinks)
	if paramFilePathsDiff != nil {
		localChange = true
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ParamFilePaths{ParamFilePaths: paramFilePathsDiff}})
	}
	if paramFileContentsDiff != nil {
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ParamFileContents{ParamFileContents: paramFileContentsDiff}})
	}

	// We assume that changes in the spawn's arguments are caused by changes to the spawn's input or tool paths if any
	// and thus don't show the spawn's diff if the path and argument changes are the only differences and the spawn that
	// caused the path changes is contained in the log (and thus shown as the transitive cause).
	// This heuristic can be wrong if the spawn's arguments also changed in other ways (e.g. adding a copt and a new dep
	// at the same time), but we would still show at least one (transitive) cause for invalidating the spawn.
	if (argsChanged || paramFilePathsDiff != nil || paramFileContentsDiff != nil) && inputPathsDiff == nil && toolPathsDiff == nil && !mayExplainArgsChange(inputContentsDiff) {
		// Split XML generation receives the test duration as an argument, which is clearly non-hermetic and should not
		// be considered at all if the test action reran.
		if new.Mnemonic == testRunnerXmlGeneration {
			m.Expected = true
		} else {
			localChange = true
		}
	}
	for _, fileDiff := range inputContentsDiff.GetFileDiffs() {
		var p string
		switch fd := fileDiff.New.(type) {
		case *spawn_diff.FileDiff_NewFile:
			p = fd.NewFile.Path
		case *spawn_diff.FileDiff_NewSymlink:
			p = fd.NewSymlink.Path
		case *spawn_diff.FileDiff_NewDirectory:
			p = fd.NewDirectory.Path
		}
		if isSourcePath(p) {
			localChange = true
		} else {
			invalidatedBy = append(invalidatedBy, p)
		}
	}
	if new.Mnemonic == testRunnerXmlGeneration && argsChanged && len(invalidatedBy) == 0 {
		// The arguments for the split XML generation contain the duration of the test, which is non-hermetic. We
		// attribute it to the main test action, which has the test log as primary output.
		testLog := path.Dir(new.PrimaryOutputPath()) + "/test.log"
		invalidatedBy = append(invalidatedBy, testLog)
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
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_OutputPaths{
			OutputPaths: &spawn_diff.StringSetDiff{
				OldOnly: setDifference(oldOutputPaths, newOutputPaths),
				NewOnly: setDifference(newOutputPaths, oldOutputPaths),
			},
		}})
	}

	// Do not report changes in the outputs of a spawn whose inputs changed.
	if len(m.Diffs) > 0 {
		return
	}

	if new.ExitCode != old.ExitCode {
		// This action is flaky, always report it.
		localChange = true
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_ExitCode{
			ExitCode: &spawn_diff.IntDiff{
				Old: old.ExitCode,
				New: new.ExitCode,
			},
		}})
		// Don't report changes in the outputs of a flaky action.
		return
	}

	var outputContentsDiffs []*spawn_diff.FileDiff
	for i, oldOutput := range old.Outputs {
		// oldOutput.Path() == new.Outputs[i].Path() by construction, so we can pass either as the unresolved path.
		fileDiff := diffContents(oldOutput, new.Outputs[i], oldOutput.Path(), oldResolveSymlinks, newResolveSymlinks)
		if fileDiff != nil {
			outputContentsDiffs = append(outputContentsDiffs, fileDiff)
		}
	}
	if len(outputContentsDiffs) > 0 {
		// This action is non-hermetic, always report it.
		localChange = true
		if new.Mnemonic == "TestRunner" {
			// Test action outputs are usually non-reproducible due to timestamps in the test.log and test.xml files.
			m.Expected = true
		}
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_OutputContents{
			OutputContents: &spawn_diff.FileSetDiff{FileDiffs: outputContentsDiffs},
		}})
	}

	return
}

// mayExplainArgsChange returns true if the given diff of input contents may explain a change in the spawn's arguments.
func mayExplainArgsChange(diff *spawn_diff.FileSetDiff) bool {
	for _, fileDiff := range diff.GetFileDiffs() {
		// Contents of non-directories and source directories are not evaluated before spawn execution and thus don't
		// influence the spawn's arguments. Runfiles directories can be flattened in rules logic, but this is only
		// commonly done when the runfiles are staged as inputs (e.g. in packaging actions), not as runfiles. In the
		// former case, they wouldn't show up as directories at this point.
		if fileDiff.GetOldDirectory() == nil || fileDiff.GetNewDirectory() == nil || !IsTreeArtifactPath(fileDiff.LogicalPath) {
			continue
		}
		if len(fileDiff.GetOldDirectory().Files) != len(fileDiff.GetNewDirectory().Files) {
			return true
		}
		oldPaths := make(map[string]struct{})
		for _, oldFile := range fileDiff.GetOldDirectory().Files {
			oldPaths[oldFile.Path] = struct{}{}
		}
		for _, newFile := range fileDiff.GetNewDirectory().Files {
			if _, ok := oldPaths[newFile.Path]; !ok {
				return true
			}
		}
	}
	return false
}

func diffInputSets(old, new *InputSet, oldResolveSymlinks, newResolveSymlinks func(string) string) (pathsDiff *spawn_diff.StringSetDiff, contentsDiff *spawn_diff.FileSetDiff) {
	return diffInputSetsInternal(old, new, oldResolveSymlinks, newResolveSymlinks, false)
}

func diffInputSetsIgnoringContents(old, new *InputSet, oldResolveSymlinks, newResolveSymlinks func(string) string) *spawn_diff.StringSetDiff {
	pathsDiff, _ := diffInputSetsInternal(old, new, oldResolveSymlinks, newResolveSymlinks, true)
	return pathsDiff
}

func diffInputSetsInternal(old, new *InputSet, oldResolveSymlinks, newResolveSymlinks func(string) string, ignoreContents bool) (pathsDiff *spawn_diff.StringSetDiff, contentsDiff *spawn_diff.FileSetDiff) {
	pathsCertainlyUnchanged := slices.Equal(old.ShallowPathHash(), new.ShallowPathHash())
	contentsCertainlyUnchanged := ignoreContents || slices.Equal(old.ShallowContentHash(), new.ShallowContentHash())
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
			// oldInput.Path() == newInputs[i].Path() by construction, so we can pass either as the unresolved path.
			fileDiff := diffContents(oldInput, newInputs[i], oldInput.Path(), oldResolveSymlinks, newResolveSymlinks)
			if fileDiff != nil {
				fileDiffs = append(fileDiffs, fileDiff)
			}
		}
		if len(fileDiffs) > 0 {
			return nil, &spawn_diff.FileSetDiff{FileDiffs: fileDiffs}
		}
	}
	return nil, nil
}

// diffRunfilesTrees returns a diff of the runfiles trees if the paths or contents of the inputs differ, or nil if they
// are equal.
func diffRunfilesTrees(old, new *Spawn, oldResolveSymlinks, newResolveSymlinks func(string) string) (diff *spawn_diff.SpawnDiff, localChange bool, invalidatedBy []string) {
	oldTree := old.Inputs.DirectEntries[0].(*RunfilesTree)
	newTree := new.Inputs.DirectEntries[0].(*RunfilesTree)

	diff = newDiff(new)
	m := &spawn_diff.Modified{}
	diff.Diff = &spawn_diff.SpawnDiff_Modified{Modified: m}

	contentsCertainlyUnchanged := slices.Equal(oldTree.ShallowContentHash(), newTree.ShallowContentHash())
	if contentsCertainlyUnchanged {
		return
	}

	oldMapping := oldTree.ComputeMapping()
	newMapping := newTree.ComputeMapping()

	var oldOnly, newOnly []string
	for p, _ := range oldMapping {
		if _, ok := newMapping[p]; !ok {
			oldOnly = append(oldOnly, p)
		}
	}
	for p, _ := range newMapping {
		if _, ok := oldMapping[p]; !ok {
			newOnly = append(newOnly, p)
		}
	}
	if len(oldOnly) > 0 || len(newOnly) > 0 {
		slices.Sort(oldOnly)
		slices.Sort(newOnly)
		localChange = true
		m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_InputPaths{
			InputPaths: &spawn_diff.StringSetDiff{
				OldOnly: oldOnly,
				NewOnly: newOnly,
			}}})
		return
	}

	if !contentsCertainlyUnchanged {
		var fileDiffs []*spawn_diff.FileDiff
		for p, oldInput := range oldMapping {
			newInput := newMapping[p]
			fileDiff := diffContents(oldInput, newInput, p, oldResolveSymlinks, newResolveSymlinks)
			if fileDiff != nil {
				fileDiffs = append(fileDiffs, fileDiff)
				invalidatedBy = append(invalidatedBy, newInput.Path())
			}
		}
		if len(fileDiffs) > 0 {
			slices.SortFunc(fileDiffs, func(a, b *spawn_diff.FileDiff) int {
				return cmp.Compare(a.LogicalPath, b.LogicalPath)
			})
			m.Diffs = append(m.Diffs, &spawn_diff.Diff{Diff: &spawn_diff.Diff_InputContents{
				InputContents: &spawn_diff.FileSetDiff{
					FileDiffs: fileDiffs,
				}}})
		}
	}

	return
}

// diffContents returns a file diff if the contents of the old and new inputs differ, or nil if they are equal.
func diffContents(old, new Input, logicalPath string, oldResolveSymlinks, newResolveSymlinks func(string) string) *spawn_diff.FileDiff {
	if slices.Equal(old.ShallowContentHash(), new.ShallowContentHash()) {
		return nil
	}
	fileDiff := &spawn_diff.FileDiff{
		LogicalPath: logicalPath,
	}
	switch oldProto := old.Proto().(type) {
	case *spawnproto.ExecLogEntry_File:
		oldProto.Path = oldResolveSymlinks(oldProto.Path)
		fileDiff.Old = &spawn_diff.FileDiff_OldFile{OldFile: oldProto}
	case *spawnproto.ExecLogEntry_UnresolvedSymlink:
		oldProto.Path = oldResolveSymlinks(oldProto.Path)
		fileDiff.Old = &spawn_diff.FileDiff_OldSymlink{OldSymlink: oldProto}
	case *spawnproto.ExecLogEntry_Directory:
		oldProto.Path = oldResolveSymlinks(oldProto.Path)
		fileDiff.Old = &spawn_diff.FileDiff_OldDirectory{OldDirectory: oldProto}
	case string:
		fileDiff.Old = &spawn_diff.FileDiff_OldInvalidOutput{OldInvalidOutput: oldProto}
	}
	switch newProto := new.Proto().(type) {
	case *spawnproto.ExecLogEntry_File:
		newProto.Path = newResolveSymlinks(newProto.Path)
		fileDiff.New = &spawn_diff.FileDiff_NewFile{NewFile: newProto}
	case *spawnproto.ExecLogEntry_UnresolvedSymlink:
		newProto.Path = newResolveSymlinks(newProto.Path)
		fileDiff.New = &spawn_diff.FileDiff_NewSymlink{NewSymlink: newProto}
	case *spawnproto.ExecLogEntry_Directory:
		newProto.Path = newResolveSymlinks(newProto.Path)
		fileDiff.New = &spawn_diff.FileDiff_NewDirectory{NewDirectory: newProto}
	case string:
		fileDiff.New = &spawn_diff.FileDiff_NewInvalidOutput{NewInvalidOutput: newProto}
	}
	return fileDiff
}

func newDiff(s *Spawn) *spawn_diff.SpawnDiff {
	return &spawn_diff.SpawnDiff{
		PrimaryOutput: s.PrimaryOutputPath(),
		TargetLabel:   s.TargetLabel,
		Mnemonic:      s.Mnemonic,
	}
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
