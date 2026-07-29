package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

const (
	log1 = "example-cel.binpb.zst"
	log2 = "example-cel-2.binpb.zst"
)

func newDigest(hash string, size int64) *version {
	return &version{digest: &spb.Digest{Hash: hash, SizeBytes: size}}
}

func fromLog(v *version, logIdx int) *version {
	v.firstLog = logIdx
	return v
}

func TestParseSingleLog(t *testing.T) {
	tr := newTree()
	require.NoError(t, tr.parse(log1))
	assert.Empty(t, tr.conflicts)
	assert.Equal(t, "BLAKE3", tr.hashFunction)
	assert.Equal(t, []string{"cc026130-0563-4c28-8ea1-422d5b82e32e"}, tr.invocationIDs())
	assert.Equal(t, 6023, tr.numFiles)

	// Before filtering, generated outputs are part of the tree.
	assert.Contains(t, tr.root.children, "bazel-out")

	// External repo sources are filtered out along with generated files.
	assert.Equal(t, 45, tr.shownFiles)

	filter(tr.root, filterOptions{}, 0)

	assert.NotContains(t, tr.root.children, "bazel-out")
	assert.NotContains(t, tr.root.children, "external")
	s := stats(tr.root)
	assert.Equal(t, 45, s.files)
	// A single log has nothing to compare against.
	assert.Equal(t, 0, s.comparable)
	assert.Equal(t, 0, s.modified)

	var buf bytes.Buffer
	printFlat(&buf, tr, printOptions{})
	assert.Contains(t, buf.String(),
		"cli/printlog/compact/compact.go\ta5f1b4415cd1c182be84036b903bd11ad1be6fcfaa5fb36f997fbf6962e50554  13415\n")

	// Every printed path should be a source path.
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		assert.False(t, strings.HasPrefix(line, "bazel-"), "generated path in filtered output: %s", line)
		assert.NotContains(t, line, modifiedMarker)
	}
}

func TestMergeLogs(t *testing.T) {
	tr := newTree()
	require.NoError(t, tr.parse(log1))
	require.NoError(t, tr.parse(log2))
	assert.Empty(t, tr.conflicts)
	assert.Len(t, tr.invocationIDs(), 2)

	filter(tr.root, filterOptions{}, 0)
	s := stats(tr.root)

	// The merged tree covers more files than either log alone.
	assert.Greater(t, s.files, 45)
	assert.Greater(t, s.comparable, 0)
	assert.GreaterOrEqual(t, s.comparable, s.modified)

	// Modified files, and only modified files, are marked.
	var buf bytes.Buffer
	printFlat(&buf, tr, printOptions{})
	marked := 0
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if strings.HasSuffix(line, modifiedMarker) {
			marked++
		}
	}
	assert.Equal(t, s.modified, marked)

	// Merging is order independent, apart from the order versions are listed
	// in.
	reversed := newTree()
	require.NoError(t, reversed.parse(log2))
	require.NoError(t, reversed.parse(log1))
	filter(reversed.root, filterOptions{}, 0)
	assert.Equal(t, s, stats(reversed.root))
}

func TestIncludeExternal(t *testing.T) {
	tr := newTree()
	tr.filter = filterOptions{includeExternal: true}
	require.NoError(t, tr.parse(log1))
	assert.Equal(t, 66, tr.shownFiles)

	filter(tr.root, tr.filter, 0)

	assert.Contains(t, tr.root.children, "external")
	assert.Equal(t, 66, stats(tr.root).files)
}

// The running tally kept while logs are merged has to agree with the tree that
// actually gets printed, for every combination of filter options.
func TestShownCountsMatchFilteredTree(t *testing.T) {
	for _, opts := range []filterOptions{
		{},
		{includeExternal: true},
		{includeGenerated: true},
		{includeGenerated: true, includeExternal: true},
	} {
		t.Run(fmt.Sprintf("generated=%v/external=%v", opts.includeGenerated, opts.includeExternal), func(t *testing.T) {
			tr := newTree()
			tr.filter = opts
			require.NoError(t, tr.parse(log1))
			require.NoError(t, tr.parse(log2))
			shownFiles, shownDirs := tr.shownFiles, tr.shownDirs

			filter(tr.root, opts, 0)

			assert.Equal(t, stats(tr.root).files, shownFiles)
			assert.Equal(t, countDirs(tr.root), shownDirs)
		})
	}
}

// countDirs counts the directories below a node, excluding the node itself.
func countDirs(n *node) int {
	count := 0
	for _, c := range n.children {
		if c.isDir() {
			count += 1 + countDirs(c)
		}
	}
	return count
}

func TestShownCountsSkipDuplicatePaths(t *testing.T) {
	tr := newTree()
	tr.add("a/b/c.go", newDigest("aaaa", 1))
	assert.Equal(t, 1, tr.shownFiles)
	assert.Equal(t, 2, tr.shownDirs)

	// Re-adding the same path, even with different contents, doesn't grow the
	// tree: this is what tells the walk that history has stopped adding files.
	tr.add("a/b/c.go", fromLog(newDigest("bbbb", 1), 1))
	assert.Equal(t, 1, tr.shownFiles)
	assert.Equal(t, 2, tr.shownDirs)

	// Filtered-out paths don't count either.
	tr.add("bazel-out/k8-fastbuild/bin/gen.go", newDigest("cccc", 1))
	tr.add("external/repo/dep.go", newDigest("dddd", 1))
	assert.Equal(t, 1, tr.shownFiles)
	assert.Equal(t, 2, tr.shownDirs)
}

func TestFilterOptionsRemoveExternalByDefault(t *testing.T) {
	tr := newTree()
	require.NoError(t, tr.parse(log1))
	filter(tr.root, filterOptions{}, 0)

	assert.NotContains(t, tr.root.children, "external")
	assert.Equal(t, 45, stats(tr.root).files)
}

func TestAddAndPrintTree(t *testing.T) {
	tr := newTree()
	tr.logs = []logInfo{{name: "first.binpb.zst"}, {name: "second.binpb.zst"}}
	tr.add("a/b/c.go", newDigest("aaaa", 1))
	tr.add("a/d.go", newDigest("bbbb", 2))
	tr.add("bazel-out/k8-fastbuild/bin/gen.go", newDigest("cccc", 3))
	tr.add("link", &version{symlinkTarget: "a/d.go"})
	// Empty files have no digest.
	tr.add("empty", &version{})
	assert.Empty(t, tr.conflicts)
	assert.Equal(t, 5, tr.numFiles)

	// Re-adding the same contents from a second log is a no-op...
	tr.add("a/d.go", fromLog(newDigest("bbbb", 2), 1))
	assert.Empty(t, tr.conflicts)
	// ...but different contents in a second log is a modification.
	tr.add("a/b/c.go", fromLog(newDigest("dddd", 4), 1))
	assert.Empty(t, tr.conflicts)
	assert.Equal(t, 5, tr.numFiles)

	filter(tr.root, filterOptions{}, 0)

	var buf bytes.Buffer
	printTree(&buf, tr, printOptions{hashLength: 4, dirHashes: false})
	// The modified file marks every directory above it.
	assert.Equal(t, `. (!)
├── a/ (!)
│   ├── b/ (!)
│   │   └── c.go  aaaa  1, dddd  4 (!)
│   └── d.go  bbbb  2
├── empty  <empty>  0
└── link  -> a/d.go
`, buf.String())

	s := stats(tr.root)
	assert.Equal(t, 4, s.files)
	// a/b/c.go and a/d.go were both in two logs; only a/b/c.go changed.
	assert.Equal(t, 2, s.comparable)
	assert.Equal(t, 1, s.modified)
}

func TestSameLogConflict(t *testing.T) {
	tr := newTree()
	tr.logs = []logInfo{{name: "first.binpb.zst"}, {name: "second.binpb.zst"}}
	tr.add("a.go", newDigest("aaaa", 1))
	// Two digests for the same path within one log is a real conflict, not a
	// modification...
	tr.add("a.go", newDigest("bbbb", 1))
	assert.Len(t, tr.conflicts, 1)
	assert.Contains(t, tr.conflicts[0], "first.binpb.zst")

	// ...including when the version it contradicts was first seen in an
	// earlier log.
	tr.add("b.go", newDigest("cccc", 1))
	tr.add("b.go", fromLog(newDigest("cccc", 1), 1))
	assert.Len(t, tr.conflicts, 1)
	tr.add("b.go", fromLog(newDigest("dddd", 1), 1))
	require.Len(t, tr.conflicts, 2)
	assert.Contains(t, tr.conflicts[1], "second.binpb.zst")
}

func TestDirHashesIgnoreUnrelatedSubtrees(t *testing.T) {
	build := func(extra string) *tree {
		tr := newTree()
		tr.add("a/b.go", newDigest("aaaa", 1))
		tr.add(extra, newDigest("bbbb", 2))
		return tr
	}
	first := build("c/d.go")
	second := build("e/f.go")

	assert.Equal(t, first.root.children["a"].hash(), second.root.children["a"].hash())
	assert.NotEqual(t, first.root.hash(), second.root.hash())

	// A modified file changes the hashes of the directories above it.
	modified := build("c/d.go")
	modified.add("a/b.go", fromLog(newDigest("ffff", 3), 1))
	assert.NotEqual(t, first.root.children["a"].hash(), modified.root.children["a"].hash())

	// Pruning a subtree must invalidate the memoized hashes above it.
	rootHash := second.root.hash()
	delete(second.root.children, "e")
	filter(second.root, filterOptions{}, 0)
	assert.NotEqual(t, rootHash, second.root.hash())
}
