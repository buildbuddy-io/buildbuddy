package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protodelim"

	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

// graphLog builds a log out of whole ExecLogEntry protos, so the graph tests
// can describe spawns and input sets directly.
func graphLog(t *testing.T, entries ...*spb.ExecLogEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	for _, e := range entries {
		_, err := protodelim.MarshalTo(zw, e)
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func fileEntry(id uint32, path string) *spb.ExecLogEntry {
	return &spb.ExecLogEntry{
		Id:   id,
		Type: &spb.ExecLogEntry_File_{File: &spb.ExecLogEntry_File{Path: path}},
	}
}

func setEntry(id uint32, inputs []uint32, transitive []uint32) *spb.ExecLogEntry {
	return &spb.ExecLogEntry{
		Id: id,
		Type: &spb.ExecLogEntry_InputSet_{InputSet: &spb.ExecLogEntry_InputSet{
			InputIds:         inputs,
			TransitiveSetIds: transitive,
		}},
	}
}

func spawnEntry(target, mnemonic string, inputSet uint32, outputs ...uint32) *spb.ExecLogEntry {
	s := &spb.ExecLogEntry_Spawn{
		TargetLabel: target,
		Mnemonic:    mnemonic,
		InputSetId:  inputSet,
	}
	for _, id := range outputs {
		s.Outputs = append(s.Outputs, &spb.ExecLogEntry_Output{
			Type: &spb.ExecLogEntry_Output_OutputId{OutputId: id},
		})
	}
	return &spb.ExecLogEntry{Type: &spb.ExecLogEntry_Spawn_{Spawn: s}}
}

// buildGraph is a small dependency chain: rexec.go compiles into rexec.a, which
// links into two binaries. unrelated.go is off to the side.
func buildGraph(t *testing.T) *spawnGraph {
	t.Helper()
	log := graphLog(t,
		fileEntry(1, "server/util/rexec/rexec.go"),
		fileEntry(2, "server/util/rexec/rexec.a"),
		fileEntry(3, "server/main.go"),
		fileEntry(4, "server/server"),
		fileEntry(5, "cli/cli"),
		fileEntry(6, "unrelated.go"),
		setEntry(10, []uint32{1}, nil),
		// The compile output reaches the linkers through a transitive set, the
		// way Bazel actually nests them.
		setEntry(11, []uint32{2}, nil),
		setEntry(12, []uint32{3}, []uint32{11}),
		setEntry(13, nil, []uint32{11}),
		setEntry(14, []uint32{6}, nil),
		spawnEntry("//server/util/rexec:rexec", "GoCompilePkg", 10, 2),
		spawnEntry("//server:server", "GoLink", 12, 4),
		spawnEntry("//cli:cli", "GoLink", 13, 5),
		spawnEntry("//other:other", "GoCompilePkg", 14),
	)
	g, err := readSpawnGraph(bytes.NewReader(mustExpand(t, log)))
	require.NoError(t, err)
	return g
}

// mustExpand decompresses a log, since readSpawnGraph takes the expanded form.
func mustExpand(t *testing.T, compressed []byte) []byte {
	t.Helper()
	zr, err := zstd.NewReader(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer zr.Close()
	expanded, err := zr.DecodeAll(compressed, nil)
	require.NoError(t, err)
	return expanded
}

func TestSpawnGraphReadsEntries(t *testing.T) {
	g := buildGraph(t)
	assert.Equal(t, "server/util/rexec/rexec.go", g.paths[1])
	assert.Len(t, g.sets, 5)
	require.Len(t, g.spawns, 4)
	assert.Equal(t, "//server/util/rexec:rexec", g.spawns[0].target)
	assert.Equal(t, "GoCompilePkg", g.spawns[0].mnemonic)
	assert.Equal(t, []uint32{2}, g.spawns[0].outputs)
	// Reverse edges let membership be answered without expanding every set.
	assert.ElementsMatch(t, []uint32{12, 13}, g.setParents[11])
}

// The headline question: which steps are at either end of this file.
func TestSpawnGraphStepsFor(t *testing.T) {
	g := buildGraph(t)

	// A source is written by nothing and compiled by one step.
	producers, consumers := g.stepsFor("server/util/rexec/rexec.go")
	assert.Empty(t, producers)
	assert.Equal(t, []targetMnemonic{
		{target: "//server/util/rexec:rexec", mnemonic: "GoCompilePkg"},
	}, consumers)

	// The compile's output is written by it and taken by both links, one of
	// them through a set that only holds it transitively.
	producers, consumers = g.stepsFor("server/util/rexec/rexec.a")
	assert.Equal(t, []targetMnemonic{
		{target: "//server/util/rexec:rexec", mnemonic: "GoCompilePkg"},
	}, producers)
	assert.Equal(t, []targetMnemonic{
		{target: "//cli:cli", mnemonic: "GoLink"},
		{target: "//server:server", mnemonic: "GoLink"},
	}, consumers)

	// A binary nothing consumes.
	producers, consumers = g.stepsFor("server/server")
	assert.Equal(t, []targetMnemonic{{target: "//server:server", mnemonic: "GoLink"}}, producers)
	assert.Empty(t, consumers)
}

func TestSpawnGraphStepsForUnknownFile(t *testing.T) {
	producers, consumers := buildGraph(t).stepsFor("nothing/here.go")
	assert.Empty(t, producers)
	assert.Empty(t, consumers)
}

// A spawn that takes a directory as an input consumes the files inside it.
func TestSpawnGraphMatchesFilesInsideDirectories(t *testing.T) {
	log := graphLog(t,
		&spb.ExecLogEntry{
			Id: 1,
			Type: &spb.ExecLogEntry_Directory_{Directory: &spb.ExecLogEntry_Directory{
				Path:  "some/data",
				Files: []*spb.ExecLogEntry_File{{Path: "inner.txt"}},
			}},
		},
		setEntry(10, []uint32{1}, nil),
		spawnEntry("//some:data", "Genrule", 10),
	)
	g, err := readSpawnGraph(bytes.NewReader(mustExpand(t, log)))
	require.NoError(t, err)

	_, consumers := g.stepsFor("some/data/inner.txt")
	require.Len(t, consumers, 1)
	assert.Equal(t, "//some:data", consumers[0].target)
	// A path that merely shares a prefix isn't inside the directory.
	_, consumers = g.stepsFor("some/database.txt")
	assert.Empty(t, consumers)
}

// Older Bazels wrote the per-kind fields instead of input_ids.
func TestSpawnGraphReadsDeprecatedInputFields(t *testing.T) {
	log := graphLog(t,
		fileEntry(1, "old/style.go"),
		&spb.ExecLogEntry{
			Id: 10,
			Type: &spb.ExecLogEntry_InputSet_{InputSet: &spb.ExecLogEntry_InputSet{
				FileIds: []uint32{1},
			}},
		},
		spawnEntry("//old:style", "GoCompilePkg", 10),
	)
	g, err := readSpawnGraph(bytes.NewReader(mustExpand(t, log)))
	require.NoError(t, err)

	_, consumers := g.stepsFor("old/style.go")
	require.Len(t, consumers, 1)
	assert.Equal(t, "//old:style", consumers[0].target)
}

// What a target's steps consumed and produced, which is the other question the
// graph answers.
func TestSpawnGraphDescribeFiles(t *testing.T) {
	g := buildGraph(t)

	steps := g.describe("//server:server").steps
	require.Contains(t, steps, "GoLink")
	// The link takes main.go directly and the compile's output through a nested
	// set.
	assert.Equal(t, []string{"server/main.go", "server/util/rexec/rexec.a"}, steps["GoLink"].inputs)
	assert.Equal(t, []string{"server/server"}, steps["GoLink"].outputs)

	// A step whose outputs nobody takes still reports what it consumed.
	other := g.describe("//other:other").steps
	require.Contains(t, other, "GoCompilePkg")
	assert.Equal(t, []string{"unrelated.go"}, other["GoCompilePkg"].inputs)
	assert.Empty(t, other["GoCompilePkg"].outputs)

	assert.Empty(t, g.describe("//nothing:here").steps)
}

// Which targets are on the far end of a target's files.
func TestSpawnGraphDescribeEdges(t *testing.T) {
	g := buildGraph(t)

	// The compile's output reaches both linkers, one of them through a set that
	// only holds it transitively.
	compile := g.describe("//server/util/rexec:rexec")
	assert.Empty(t, compile.dependencies, "nothing generates its source")
	require.Len(t, compile.dependents, 2)
	assert.Equal(t, targetMnemonic{target: "//cli:cli", mnemonic: "GoLink"}, compile.dependents[0].step)
	assert.Equal(t, []string{"server/util/rexec/rexec.a"}, compile.dependents[0].files)
	assert.Equal(t, targetMnemonic{target: "//server:server", mnemonic: "GoLink"}, compile.dependents[1].step)
	assert.Equal(t, []string{"server/util/rexec/rexec.a"}, compile.dependents[1].files)

	// And from the other end.
	link := g.describe("//server:server")
	require.Len(t, link.dependencies, 1)
	assert.Equal(t, targetMnemonic{target: "//server/util/rexec:rexec", mnemonic: "GoCompilePkg"},
		link.dependencies[0].step)
	assert.Equal(t, []string{"server/util/rexec/rexec.a"}, link.dependencies[0].files)
	assert.Empty(t, link.dependents)
}

// A target's own steps handing files to each other aren't dependencies of it:
// its inputs and outputs already cover that.
func TestSpawnGraphDescribeSkipsItself(t *testing.T) {
	log := graphLog(t,
		fileEntry(1, "lib.go"),
		fileEntry(2, "lib.a"),
		fileEntry(3, "lib.so"),
		setEntry(10, []uint32{1}, nil),
		setEntry(11, []uint32{2}, nil),
		spawnEntry("//lib:lib", "GoCompilePkg", 10, 2),
		spawnEntry("//lib:lib", "GoLink", 11, 3),
	)
	g, err := readSpawnGraph(bytes.NewReader(mustExpand(t, log)))
	require.NoError(t, err)

	files := g.describe("//lib:lib")
	assert.Empty(t, files.dependencies)
	assert.Empty(t, files.dependents)
	// The step lists still show the handoff.
	assert.Equal(t, []string{"lib.a"}, files.steps["GoLink"].inputs)
}

// Attribution is per entry: two dependents that take different files of ours
// are told apart.
func TestSpawnGraphDescribeAttributesFiles(t *testing.T) {
	log := graphLog(t,
		fileEntry(1, "src.go"),
		fileEntry(2, "out.a"),
		fileEntry(3, "out.h"),
		setEntry(10, []uint32{1}, nil),
		setEntry(11, []uint32{2}, nil),
		setEntry(12, []uint32{3}, nil),
		setEntry(13, nil, []uint32{11, 12}),
		spawnEntry("//lib:lib", "GoCompilePkg", 10, 2, 3),
		spawnEntry("//a:a", "CppLink", 11),
		spawnEntry("//b:b", "CppCompile", 12),
		spawnEntry("//c:c", "Genrule", 13),
	)
	g, err := readSpawnGraph(bytes.NewReader(mustExpand(t, log)))
	require.NoError(t, err)

	dependents := g.describe("//lib:lib").dependents
	require.Len(t, dependents, 3)
	assert.Equal(t, "//a:a", dependents[0].step.target)
	assert.Equal(t, []string{"out.a"}, dependents[0].files)
	assert.Equal(t, "//b:b", dependents[1].step.target)
	assert.Equal(t, []string{"out.h"}, dependents[1].files)
	// One that takes both, through a set holding them only transitively.
	assert.Equal(t, "//c:c", dependents[2].step.target)
	assert.Equal(t, []string{"out.a", "out.h"}, dependents[2].files)
}

// More than 64 entries means more than one block of attribution, and they all
// have to end up on the same edge.
func TestSpawnGraphDescribeManyOutputs(t *testing.T) {
	entries := []*spb.ExecLogEntry{}
	outputs := make([]uint32, 0, 100)
	inputs := make([]uint32, 0, 100)
	for i := range uint32(100) {
		entries = append(entries, fileEntry(i+1, fmt.Sprintf("out%03d", i)))
		outputs = append(outputs, i+1)
		inputs = append(inputs, i+1)
	}
	entries = append(entries,
		setEntry(200, nil, nil),
		setEntry(201, inputs, nil),
		spawnEntry("//lib:lib", "Genrule", 200, outputs...),
		spawnEntry("//user:user", "Genrule", 201),
	)
	g, err := readSpawnGraph(bytes.NewReader(mustExpand(t, graphLog(t, entries...))))
	require.NoError(t, err)

	dependents := g.describe("//lib:lib").dependents
	require.Len(t, dependents, 1)
	assert.Len(t, dependents[0].files, 100)
	assert.Equal(t, "out000", dependents[0].files[0])
	assert.Equal(t, "out099", dependents[0].files[99])
}

// Several spawns can share a target and mnemonic - test shards, say - and their
// files are pooled.
func TestSpawnGraphFilesForPoolsSpawns(t *testing.T) {
	log := graphLog(t,
		fileEntry(1, "shard1.txt"),
		fileEntry(2, "shard2.txt"),
		fileEntry(3, "shared.txt"),
		fileEntry(4, "log1"),
		fileEntry(5, "log2"),
		setEntry(10, []uint32{3}, nil),
		setEntry(11, []uint32{1}, []uint32{10}),
		setEntry(12, []uint32{2}, []uint32{10}),
		spawnEntry("//t:t", "TestRunner", 11, 4),
		spawnEntry("//t:t", "TestRunner", 12, 5),
	)
	g, err := readSpawnGraph(bytes.NewReader(mustExpand(t, log)))
	require.NoError(t, err)

	files := g.describe("//t:t").steps
	require.Contains(t, files, "TestRunner")
	// The set they share is listed once, not twice.
	assert.Equal(t, []string{"shard1.txt", "shard2.txt", "shared.txt"}, files["TestRunner"].inputs)
	assert.Equal(t, []string{"log1", "log2"}, files["TestRunner"].outputs)
}

// A cycle among input sets can't happen in a real log, but a corrupt one
// mustn't hang the UI.
func TestSpawnGraphFilesForSurvivesCycles(t *testing.T) {
	log := graphLog(t,
		fileEntry(1, "a.txt"),
		setEntry(10, []uint32{1}, []uint32{11}),
		setEntry(11, nil, []uint32{10}),
		spawnEntry("//t:t", "Genrule", 10),
	)
	g, err := readSpawnGraph(bytes.NewReader(mustExpand(t, log)))
	require.NoError(t, err)

	assert.Equal(t, []string{"a.txt"}, g.describe("//t:t").steps["Genrule"].inputs)
}

// Pooling what several builds say about one file, which is what the file info
// view asks.
func TestSummarizeFile(t *testing.T) {
	dir := t.TempDir()
	// The same source, compiled by a different target in each build, and each
	// build's compile feeding a different link.
	write := func(name, target, output string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, graphLog(t,
			fileEntry(1, "shared.go"),
			fileEntry(2, output),
			fileEntry(3, "binary"),
			setEntry(10, []uint32{1}, nil),
			setEntry(11, []uint32{2}, nil),
			spawnEntry(target, "GoCompilePkg", 10, 2),
			spawnEntry(target+"_bin", "GoLink", 11, 3),
		), 0644))
		return path
	}
	sources := []logSource{
		{path: write("new.binpb.zst", "//new:new", "new.a")},
		{path: write("old.binpb.zst", "//old:old", "old.a")},
	}

	summary := summarizeFile([]string{"shared.go"}, sources)
	assert.Equal(t, 2, summary.read)
	assert.Zero(t, summary.failed)
	// Both builds' compiles took it.
	assert.Equal(t, []targetMnemonic{
		{target: "//new:new", mnemonic: "GoCompilePkg"},
		{target: "//old:old", mnemonic: "GoCompilePkg"},
	}, summary.consumers)
	// Nothing wrote it, which is what makes it a source.
	assert.Empty(t, summary.producers)

	// The generated file only one of the builds wrote.
	summary = summarizeFile([]string{"old.a"}, sources)
	require.Len(t, summary.producers, 1)
	assert.Equal(t, "//old:old", summary.producers[0].target)
}

// A log that can't be read is counted rather than reported as an empty answer.
func TestSummarizeFileCountsFailures(t *testing.T) {
	summary := summarizeFile([]string{"shared.go"}, []logSource{{path: "/nonexistent/log.binpb.zst"}})
	assert.Zero(t, summary.read)
	assert.Equal(t, 1, summary.failed)
	assert.Empty(t, summary.producers)
	assert.Empty(t, summary.consumers)
}

// A graph we already have is used as it is, rather than read again.
func TestSummarizeFileUsesLoadedGraphs(t *testing.T) {
	summary := summarizeFile([]string{"server/util/rexec/rexec.a"}, []logSource{{graph: buildGraph(t)}})
	assert.Equal(t, 1, summary.read)
	require.Len(t, summary.producers, 1)
	assert.Equal(t, "//server/util/rexec:rexec", summary.producers[0].target)
	require.Len(t, summary.consumers, 2)
	assert.Equal(t, "//cli:cli", summary.consumers[0].target)
	assert.Equal(t, "//server:server", summary.consumers[1].target)
}

// The graph reads the same logs the merge does, compressed or not.
func TestLoadSpawnGraphFromFile(t *testing.T) {
	g, err := loadSpawnGraph(log1)
	require.NoError(t, err)
	assert.NotEmpty(t, g.spawns)
	assert.NotEmpty(t, g.sets)

	_, consumers := g.stepsFor("cli/printlog/compact/compact.go")
	require.NotEmpty(t, consumers, "a source file in the log should have been compiled by something")
	assert.NotEmpty(t, consumers[0].mnemonic)
}
