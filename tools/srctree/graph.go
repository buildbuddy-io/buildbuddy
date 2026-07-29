package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protowire"

	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

// ExecLogEntry oneof field numbers for the parts a merge leaves encoded, or
// reads only the labels of, but the info views need in full.
const (
	fieldInputSet = 6
	fieldSpawn    = 7
)

// spawnGraph is the half of a log that merging throws away: which spawns ran,
// what they consumed and what they produced. It's built for one log at a time,
// on demand, because keeping it for every merged log would dwarf the tree.
type spawnGraph struct {
	// paths names the file, directory or symlink an entry ID refers to. A
	// directory's contents are matched by prefix rather than listed.
	paths map[uint32]string
	dirs  map[uint32]bool

	// sets holds the input sets, and setParents the reverse edges: which sets
	// contain a given set. Membership questions are answered by walking up
	// those edges, which is far cheaper than expanding every spawn's inputs.
	sets       map[uint32]*spb.ExecLogEntry_InputSet
	setParents map[uint32][]uint32

	spawns []graphSpawn
}

// graphSpawn is one executed spawn, reduced to what the info view asks about.
type graphSpawn struct {
	target   string
	mnemonic string
	inputSet uint32
	outputs  []uint32
}

// targetMnemonic identifies a build step the way the info view reports it.
type targetMnemonic struct {
	target   string
	mnemonic string
}

func (t targetMnemonic) String() string {
	if t.target == "" {
		return t.mnemonic
	}
	return t.target + "  " + t.mnemonic
}

// loadSpawnGraph reads a log and builds its spawn graph. Unlike a merge, this
// unmarshals the spawns and input sets, so it's the expensive way to read a
// log - hence one log, only when asked.
func loadSpawnGraph(logPath string) (*spawnGraph, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	magic, err := r.Peek(len(zstdMagic))
	if err != nil && err != io.EOF {
		return nil, err
	}
	var in io.Reader = r
	if bytes.Equal(magic, zstdMagic) {
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("open zstd stream: %s", err)
		}
		defer zr.Close()
		in = zr
	}
	return readSpawnGraph(in)
}

func readSpawnGraph(in io.Reader) (*spawnGraph, error) {
	g := &spawnGraph{
		paths:      map[uint32]string{},
		dirs:       map[uint32]bool{},
		sets:       map[uint32]*spb.ExecLogEntry_InputSet{},
		setParents: map[uint32][]uint32{},
	}
	r := bufio.NewReaderSize(in, 1<<20)
	var buf []byte
	for {
		size, err := binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read execution log entry: %s", err)
		}
		if size > maxEntrySize {
			return nil, fmt.Errorf("read execution log entry: entry of %d bytes is implausible", size)
		}
		if uint64(cap(buf)) < size {
			buf = make([]byte, size)
		}
		buf = buf[:size]
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read execution log entry: %s", err)
		}
		if err := g.addEntry(buf); err != nil {
			return nil, fmt.Errorf("read execution log entry: %s", err)
		}
	}
	// The reverse edges can only be built once every set has been seen.
	for id, set := range g.sets {
		for _, child := range set.GetTransitiveSetIds() {
			g.setParents[child] = append(g.setParents[child], id)
		}
	}
	return g, nil
}

// addEntry decodes one encoded ExecLogEntry. Unlike the merge, this one needs
// the entry's ID: the whole graph is expressed in terms of them.
func (g *spawnGraph) addEntry(b []byte) error {
	var (
		id      uint32
		field   protowire.Number
		payload []byte
	)
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		if typ == protowire.BytesType {
			p, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			field, payload = num, p
			b = b[n:]
			continue
		}
		if num == 1 && typ == protowire.VarintType {
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			id = uint32(v)
			b = b[n:]
			continue
		}
		if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
	}
	if payload == nil {
		return nil
	}
	return g.addPayload(id, field, payload)
}

func (g *spawnGraph) addPayload(id uint32, field protowire.Number, payload []byte) error {
	switch field {
	case fieldFile:
		f := &spb.ExecLogEntry_File{}
		if err := f.UnmarshalVT(payload); err != nil {
			return err
		}
		g.paths[id] = f.GetPath()
	case fieldDirectory:
		d := &spb.ExecLogEntry_Directory{}
		if err := d.UnmarshalVT(payload); err != nil {
			return err
		}
		g.paths[id] = d.GetPath()
		g.dirs[id] = true
	case fieldUnresolvedSymlink:
		s := &spb.ExecLogEntry_UnresolvedSymlink{}
		if err := s.UnmarshalVT(payload); err != nil {
			return err
		}
		g.paths[id] = s.GetPath()
	case fieldInputSet:
		set := &spb.ExecLogEntry_InputSet{}
		if err := set.UnmarshalVT(payload); err != nil {
			return err
		}
		g.sets[id] = set
	case fieldSpawn:
		s := &spb.ExecLogEntry_Spawn{}
		if err := s.UnmarshalVT(payload); err != nil {
			return err
		}
		spawn := graphSpawn{
			target:   s.GetTargetLabel(),
			mnemonic: s.GetMnemonic(),
			inputSet: s.GetInputSetId(),
		}
		for _, o := range s.GetOutputs() {
			if outputID := outputEntryID(o); outputID != 0 {
				spawn.outputs = append(spawn.outputs, outputID)
			}
		}
		g.spawns = append(g.spawns, spawn)
	default:
		// The other entry kinds say nothing about what consumed what.
	}
	return nil
}

// outputEntryID pulls the entry ID out of a spawn output, whichever of the
// oneof's forms it took.
func outputEntryID(o *spb.ExecLogEntry_Output) uint32 {
	switch t := o.GetType().(type) {
	case *spb.ExecLogEntry_Output_OutputId:
		return t.OutputId
	case *spb.ExecLogEntry_Output_FileId:
		return t.FileId
	case *spb.ExecLogEntry_Output_DirectoryId:
		return t.DirectoryId
	case *spb.ExecLogEntry_Output_UnresolvedSymlinkId:
		return t.UnresolvedSymlinkId
	default:
		// An invalid output path, which names no entry.
		return 0
	}
}

// entryIDsFor returns the entries that name the given path, including a
// directory the path sits inside.
func (g *spawnGraph) entryIDsFor(filePath string) map[uint32]bool {
	ids := map[uint32]bool{}
	for id, p := range g.paths {
		if p == filePath || (g.dirs[id] && strings.HasPrefix(filePath, p+"/")) {
			ids[id] = true
		}
	}
	return ids
}

// setsContaining returns every input set that holds one of the entries, found
// by walking up from the sets that hold them directly.
func (g *spawnGraph) setsContaining(ids map[uint32]bool) map[uint32]bool {
	found := map[uint32]bool{}
	var queue []uint32
	for setID, set := range g.sets {
		for _, input := range setInputs(set) {
			if ids[input] {
				found[setID] = true
				queue = append(queue, setID)
				break
			}
		}
	}
	for len(queue) > 0 {
		setID := queue[0]
		queue = queue[1:]
		for _, parent := range g.setParents[setID] {
			if !found[parent] {
				found[parent] = true
				queue = append(queue, parent)
			}
		}
	}
	return found
}

// setInputs returns a set's directly held entries, including the deprecated
// per-kind fields older Bazels wrote.
func setInputs(set *spb.ExecLogEntry_InputSet) []uint32 {
	inputs := set.GetInputIds()
	if len(inputs) > 0 {
		return inputs
	}
	return slices.Concat(set.GetFileIds(), set.GetDirectoryIds(), set.GetUnresolvedSymlinkIds())
}

// spawnsProducing returns the spawns that produced any of the entries.
func (g *spawnGraph) spawnsProducing(ids map[uint32]bool) []graphSpawn {
	if len(ids) == 0 {
		return nil
	}
	var producing []graphSpawn
	for _, s := range g.spawns {
		for _, id := range s.outputs {
			if ids[id] {
				producing = append(producing, s)
				break
			}
		}
	}
	return producing
}

// spawnsConsuming returns the spawns that took any of the entries as inputs.
func (g *spawnGraph) spawnsConsuming(ids map[uint32]bool) []graphSpawn {
	if len(ids) == 0 {
		return nil
	}
	sets := g.setsContaining(ids)
	var consuming []graphSpawn
	for _, s := range g.spawns {
		if sets[s.inputSet] {
			consuming = append(consuming, s)
		}
	}
	return consuming
}

// targetFiles is everything the target info view asks a log about one target:
// what each of its steps touched, and which other steps are on the far end of
// those files.
type targetFiles struct {
	// steps is what each of the target's steps touched, by mnemonic.
	steps map[string]*stepFiles
	// dependencies are the steps that produced what this target consumed, and
	// dependents the steps that consumed what it produced.
	dependencies []graphEdge
	dependents   []graphEdge
}

// step returns what one of the target's steps touched, and whether the log ran
// that step at all. It's nil-safe: a log we couldn't read answers nothing.
func (f *targetFiles) step(mnemonic string) (*stepFiles, bool) {
	if f == nil {
		return nil, false
	}
	files, ok := f.steps[mnemonic]
	return files, ok
}

// stepFiles are the files one target's steps touched in a single log.
type stepFiles struct {
	inputs  []string
	outputs []string
}

// graphEdge is one build step on the far end of a target's inputs or outputs,
// and the files that connect the two.
type graphEdge struct {
	step  targetMnemonic
	files []string
}

// stepEntries are the entries one step touched, before they're named.
type stepEntries struct {
	inputs  map[uint32]bool
	outputs map[uint32]bool
	// expanded are the input sets already walked for this step. A target's
	// spawns share most of their inputs, so this is what keeps pooling them from
	// costing a full walk each time.
	expanded map[uint32]bool
}

// describe answers everything the target info view asks about a target, in one
// walk of the log's spawns. Several spawns can share a target and mnemonic -
// test shards, or a target with more than one action of a kind - so their files
// are pooled.
func (g *spawnGraph) describe(label string) *targetFiles {
	byMnemonic := map[string]*stepEntries{}
	for _, s := range g.spawns {
		if s.target != label {
			continue
		}
		step := byMnemonic[s.mnemonic]
		if step == nil {
			step = &stepEntries{
				inputs:   map[uint32]bool{},
				outputs:  map[uint32]bool{},
				expanded: map[uint32]bool{},
			}
			byMnemonic[s.mnemonic] = step
		}
		g.collectSet(s.inputSet, step.inputs, step.expanded)
		for _, id := range s.outputs {
			step.outputs[id] = true
		}
	}

	files := &targetFiles{steps: make(map[string]*stepFiles, len(byMnemonic))}
	// The steps are reported separately, but which targets are on the far end is
	// a question about the target as a whole.
	allInputs, allOutputs := map[uint32]bool{}, map[uint32]bool{}
	for mnemonic, step := range byMnemonic {
		files.steps[mnemonic] = &stepFiles{
			inputs:  g.pathsOf(step.inputs),
			outputs: g.pathsOf(step.outputs),
		}
		maps.Copy(allInputs, step.inputs)
		maps.Copy(allOutputs, step.outputs)
	}
	files.dependencies = g.producersOf(label, allInputs)
	files.dependents = g.consumersOf(label, allOutputs)
	return files
}

// producersOf returns the steps that produced any of the given entries, with
// the ones each of them produced. The target's own steps are left out: what its
// spawns hand to each other is already covered by their inputs and outputs.
func (g *spawnGraph) producersOf(label string, ids map[uint32]bool) []graphEdge {
	if len(ids) == 0 {
		return nil
	}
	byStep := map[targetMnemonic]map[uint32]bool{}
	for _, s := range g.spawns {
		if s.target == label {
			continue
		}
		for _, id := range s.outputs {
			if ids[id] {
				addEntry(byStep, targetMnemonic{target: s.target, mnemonic: s.mnemonic}, id)
			}
		}
	}
	return g.edgesOf(byStep)
}

// consumersOf returns the steps that took any of the given entries as inputs,
// with the ones each of them took.
func (g *spawnGraph) consumersOf(label string, ids map[uint32]bool) []graphEdge {
	if len(ids) == 0 {
		return nil
	}
	byStep := map[targetMnemonic]map[uint32]bool{}
	for i, taken := range g.takenFrom(slices.Sorted(maps.Keys(ids))) {
		s := g.spawns[i]
		if s.target == label {
			continue
		}
		step := targetMnemonic{target: s.target, mnemonic: s.mnemonic}
		for _, id := range taken {
			addEntry(byStep, step, id)
		}
	}
	return g.edgesOf(byStep)
}

// takenFrom reports which of the given entries each spawn takes as an input,
// keyed by the spawn's position in the graph.
//
// It attributes them by pushing membership up the reverse edges rather than by
// flattening each spawn's inputs: a library's input set is shared by every
// spawn that links it, and flattening it once per spawn is the difference
// between a pass over the sets and a pass over the sets per consumer. Entries
// are handled 64 at a time so that each edge costs a couple of bit operations.
func (g *spawnGraph) takenFrom(ids []uint32) map[int][]uint32 {
	taken := map[int][]uint32{}
	for block := range slices.Chunk(ids, 64) {
		index := make(map[uint32]uint64, len(block))
		for i, id := range block {
			index[id] |= 1 << uint(i)
		}
		// holds[setID] is which of the block a set holds, directly or through
		// the sets nested in it.
		holds := map[uint32]uint64{}
		var queue []uint32
		for setID, set := range g.sets {
			var mask uint64
			for _, input := range setInputs(set) {
				mask |= index[input]
			}
			if mask != 0 {
				holds[setID] = mask
				queue = append(queue, setID)
			}
		}
		// Only growth is pushed, so this settles even if a corrupt log leaves a
		// cycle among the sets.
		for len(queue) > 0 {
			setID := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			mask := holds[setID]
			for _, parent := range g.setParents[setID] {
				if holds[parent]|mask != holds[parent] {
					holds[parent] |= mask
					queue = append(queue, parent)
				}
			}
		}
		for i, s := range g.spawns {
			mask := holds[s.inputSet]
			if mask == 0 {
				continue
			}
			for j, id := range block {
				if mask&(1<<uint(j)) != 0 {
					taken[i] = append(taken[i], id)
				}
			}
		}
	}
	return taken
}

// addEntry records an entry against the step on the far end of it.
func addEntry(byStep map[targetMnemonic]map[uint32]bool, step targetMnemonic, id uint32) {
	if byStep[step] == nil {
		byStep[step] = map[uint32]bool{}
	}
	byStep[step][id] = true
}

// edgesOf names the files of each edge, in target order.
func (g *spawnGraph) edgesOf(byStep map[targetMnemonic]map[uint32]bool) []graphEdge {
	edges := make([]graphEdge, 0, len(byStep))
	for step, ids := range byStep {
		edges = append(edges, graphEdge{step: step, files: g.pathsOf(ids)})
	}
	slices.SortFunc(edges, func(a, b graphEdge) int { return compareSteps(a.step, b.step) })
	return edges
}

// collectSet adds an input set's entries to ids, following the sets nested
// inside it. It walks iteratively because nested sets go deep in a large build,
// and skips the sets in visited, which are shared between spawns.
func (g *spawnGraph) collectSet(setID uint32, ids, visited map[uint32]bool) {
	queue := []uint32{setID}
	for len(queue) > 0 {
		id := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		// An unset input set ID means an empty set.
		if id == 0 || visited[id] {
			continue
		}
		visited[id] = true
		set, ok := g.sets[id]
		if !ok {
			continue
		}
		for _, input := range setInputs(set) {
			ids[input] = true
		}
		queue = append(queue, set.GetTransitiveSetIds()...)
	}
}

// pathsOf names a set of entries, in path order. Entries with no path of their
// own - a runfiles tree, say - are dropped: the files inside them are named by
// entries of their own.
func (g *spawnGraph) pathsOf(ids map[uint32]bool) []string {
	paths := make([]string, 0, len(ids))
	for id := range ids {
		if p := g.paths[id]; p != "" {
			paths = append(paths, p)
		}
	}
	slices.Sort(paths)
	return slices.Compact(paths)
}

// stepsFor answers what the file info view asks of one log: which build steps
// wrote the file, and which took it as an input. A file no step wrote is a
// source file, which is how you tell one.
//
// It deliberately stops there. Following the steps' own files another hop out -
// what a consumer's outputs go on to feed - means a walk of the input sets per
// step rather than one for the file, and the target each step belongs to has a
// page of its own that answers it properly.
func (g *spawnGraph) stepsFor(filePath string) (producers, consumers []targetMnemonic) {
	ids := g.entryIDsFor(filePath)
	if len(ids) == 0 {
		return nil, nil
	}
	return stepsOf(g.spawnsProducing(ids)), stepsOf(g.spawnsConsuming(ids))
}

// stepsOf names the distinct target and mnemonic combinations a set of spawns
// belongs to.
func stepsOf(spawns []graphSpawn) []targetMnemonic {
	seen := map[targetMnemonic]bool{}
	for _, s := range spawns {
		seen[targetMnemonic{target: s.target, mnemonic: s.mnemonic}] = true
	}
	return sortedSteps(seen)
}

// sortedSteps flattens a set of steps into the order they're listed in.
func sortedSteps(steps map[targetMnemonic]bool) []targetMnemonic {
	sorted := slices.Collect(maps.Keys(steps))
	slices.SortFunc(sorted, compareSteps)
	return sorted
}

func compareSteps(a, b targetMnemonic) int {
	if n := strings.Compare(a.target, b.target); n != 0 {
		return n
	}
	return strings.Compare(a.mnemonic, b.mnemonic)
}

// maxConcurrentGraphs bounds how many logs are read at once when summarizing a
// file. Each graph is tens of megabytes and is dropped as soon as it has been
// asked its question, so this is really a cap on how much of them is in memory
// at the same time.
const maxConcurrentGraphs = 4

// fileSummary is what every build that touched a file had to say about it.
// Which steps use a file changes as a repo does - a source moves between
// packages, a dependency comes and goes - so this pools what all of them said
// rather than trusting any one build.
type fileSummary struct {
	producers []targetMnemonic
	consumers []targetMnemonic
	// read is how many logs were read, and failed how many of those couldn't be.
	read   int
	failed int
}

// logSource is one build's log to read, and its graph if we happen to have it
// already.
type logSource struct {
	path  string
	graph *spawnGraph
}

// summarizeFile reads every given log and pools what they say about a file. The
// logs are read concurrently and each graph is dropped as soon as it has
// answered, since they're far too big to keep all of.
//
// More than one path means one file built in several configurations, which the
// logs know separately and the view shows as one.
func summarizeFile(filePaths []string, sources []logSource) *fileSummary {
	type answer struct {
		producers []targetMnemonic
		consumers []targetMnemonic
		err       error
	}
	answers := make([]answer, len(sources))
	eg := &errgroup.Group{}
	eg.SetLimit(maxConcurrentGraphs)
	for i, src := range sources {
		eg.Go(func() error {
			g := src.graph
			if g == nil {
				loaded, err := loadSpawnGraph(src.path)
				if err != nil {
					answers[i].err = err
					return nil
				}
				g = loaded
			}
			for _, filePath := range filePaths {
				producers, consumers := g.stepsFor(filePath)
				answers[i].producers = append(answers[i].producers, producers...)
				answers[i].consumers = append(answers[i].consumers, consumers...)
			}
			return nil
		})
	}
	// The callbacks never return an error; failures travel in the answers.
	eg.Wait()

	summary := &fileSummary{read: len(sources)}
	producers, consumers := map[targetMnemonic]bool{}, map[targetMnemonic]bool{}
	for _, a := range answers {
		if a.err != nil {
			summary.failed++
			summary.read--
			continue
		}
		for _, step := range a.producers {
			producers[step] = true
		}
		for _, step := range a.consumers {
			consumers[step] = true
		}
	}
	summary.producers = sortedSteps(producers)
	summary.consumers = sortedSteps(consumers)
	return summary
}
