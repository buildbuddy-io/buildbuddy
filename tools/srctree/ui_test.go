package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// keyPress builds the message the UI sees for a keystroke.
func keyPress(s string) tea.KeyPressMsg {
	if s == "enter" {
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	}
	return tea.KeyPressMsg{Code: []rune(s)[0], Text: s}
}

// uiTestTree builds a small tree with one modified file, buried deep enough
// that it starts out hidden.
func uiTestTree(t *testing.T) *tree {
	t.Helper()
	tr := newTree()
	tr.logs = []logInfo{{name: "first.binpb.zst"}, {name: "second.binpb.zst"}}
	tr.add("cli/parser.go", newDigest("aaaa", 1))
	tr.add("server/util/rexec/rexec.go", newDigest("bbbb", 2))
	tr.add("server/util/rexec/rexec_test.go", newDigest("cccc", 3))
	tr.add("server/main.go", newDigest("dddd", 4))
	tr.add("bazel-out/k8-fastbuild/bin/gen.go", newDigest("eeee", 5))
	// Modified in the second log.
	tr.add("server/util/rexec/rexec.go", fromLog(newDigest("ffff", 6), 1))
	require.Empty(t, tr.conflicts)
	return tr
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;?]*[a-zA-Z]`)

// stripANSI removes the styling escape sequences from rendered output, leaving
// the text that actually shows up on screen.
func stripANSI(s string) string {
	return ansiPattern.ReplaceAllString(s, "")
}

// paths returns the path of each visible row.
func paths(m *treeModel) []string {
	out := make([]string, 0, len(m.treeView.rows))
	for _, r := range m.treeView.rows {
		out = append(out, r.path)
	}
	return out
}

// The key helper has to produce what the UI actually matches on, or every test
// that uses it would silently exercise nothing.
func TestKeyPressHelper(t *testing.T) {
	for _, key := range []string{"t", "T", "i", "j", "g", "G", "b", "m", "c", "/", "n", "enter"} {
		assert.Equal(t, key, keyPress(key).String())
	}
}

func TestAnnotateCounts(t *testing.T) {
	tr := uiTestTree(t)
	filter(tr.root, filterOptions{}, 0)
	files, modified := annotateCounts(tr.root)
	assert.Equal(t, 4, files)
	assert.Equal(t, 1, modified)
	assert.Equal(t, 1, tr.root.children["server"].modifiedCount)
	assert.Equal(t, 3, tr.root.children["server"].fileCount)
	assert.Equal(t, 0, tr.root.children["cli"].modifiedCount)
}

func TestPrintTreeMarksDirectories(t *testing.T) {
	tr := uiTestTree(t)
	filter(tr.root, filterOptions{}, 0)
	var buf strings.Builder
	printTree(&buf, tr, printOptions{hashLength: 4, dirHashes: false})
	assert.Equal(t, `. (!)
├── cli/
│   └── parser.go  aaaa  1
└── server/ (!)
    ├── main.go  dddd  4
    └── util/ (!)
        └── rexec/ (!)
            ├── rexec.go  bbbb  2, ffff  6 (!)
            └── rexec_test.go  cccc  3
`, buf.String())
}

func TestUIExpandCollapse(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)

	// Everything starts collapsed: only the top level is showing.
	assert.Equal(t, []string{"cli", "server"}, paths(m))

	m.treeView.cursor = 1 // server
	m.expand()
	assert.Equal(t, []string{
		"cli",
		"server",
		"server/main.go",
		"server/util",
	}, paths(m))

	m.treeView.cursor = 3 // server/util
	m.expand()
	assert.Contains(t, paths(m), "server/util/rexec")
	assert.NotContains(t, paths(m), "server/util/rexec/rexec.go")

	m.treeView.cursor = 4 // server/util/rexec
	m.expand()
	assert.Contains(t, paths(m), "server/util/rexec/rexec.go")

	m.collapse()
	assert.NotContains(t, paths(m), "server/util/rexec/rexec.go")
	assert.Equal(t, "server/util/rexec", m.current().path)

	// Collapsing an already-collapsed node closes its parent and moves there.
	m.collapse()
	require.NotNil(t, m.current())
	assert.Equal(t, "server/util", m.current().path)
	assert.NotContains(t, paths(m), "server/util/rexec")
}

func TestUIJumpToModified(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)
	require.Len(t, m.modified, 1)

	m.jumpToModified(1)
	require.NotNil(t, m.current())
	assert.Equal(t, "server/util/rexec/rexec.go", m.current().path)
	assert.Empty(t, m.statusMsg)

	// The cursor must be on screen after the jump.
	assert.GreaterOrEqual(t, m.treeView.cursor, m.treeView.offset)
	assert.Less(t, m.treeView.cursor, m.treeView.offset+m.rowsHeight())
}

// uiModifiedTree has modified files spread through the tree, in tree order:
// a/1.go, b/2.go, c/3.go.
func uiModifiedTree(t *testing.T) *tree {
	t.Helper()
	tr := newTree()
	tr.logs = []logInfo{{name: "one"}, {name: "two"}}
	for _, p := range []string{"a/1.go", "a/quiet.go", "b/2.go", "c/3.go"} {
		tr.add(p, newDigest("aaaa", 1))
	}
	for _, p := range []string{"a/1.go", "b/2.go", "c/3.go"} {
		tr.add(p, fromLog(newDigest("bbbb", 2), 1))
	}
	require.Empty(t, tr.conflicts)
	return tr
}

// Jumping starts from wherever the cursor is, not from wherever the last jump
// left off.
func TestUIJumpToModifiedStartsFromCursor(t *testing.T) {
	m := newTreeModel(uiModifiedTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 24
	require.Len(t, m.modified, 3)

	m.jumpToModified(1)
	assert.Equal(t, "a/1.go", m.current().path)
	m.jumpToModified(1)
	assert.Equal(t, "b/2.go", m.current().path)

	// Move the cursor by hand, past c/. The next jump picks up from there
	// rather than continuing after b/2.go.
	m.treeView.cursor = len(m.treeView.rows) - 1
	require.Equal(t, "c", m.current().path)
	m.jumpToModified(1)
	assert.Equal(t, "c/3.go", m.current().path)

	// Backwards, likewise.
	m.jumpToModified(-1)
	assert.Equal(t, "b/2.go", m.current().path)
	m.jumpToModified(-1)
	assert.Equal(t, "a/1.go", m.current().path)
}

// A directory anchors before its own contents, so a jump from it finds what's
// inside before moving on.
func TestUIJumpToModifiedFromDirectory(t *testing.T) {
	m := newTreeModel(uiModifiedTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 24

	m.treeView.cursor = 1 // b/
	require.Equal(t, "b", m.current().path)
	m.jumpToModified(1)
	assert.Equal(t, "b/2.go", m.current().path)
}

func TestUIJumpToModifiedWraps(t *testing.T) {
	m := newTreeModel(uiModifiedTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 24

	// Past the last modified file, forwards wraps to the first.
	m.treeView.cursor = len(m.treeView.rows) - 1
	m.jumpToModified(1) // c/3.go
	m.jumpToModified(1)
	assert.Equal(t, "a/1.go", m.current().path)
	assert.Contains(t, m.statusMsg, "wrapped around to the first")

	// And backwards from the first wraps to the last.
	m.jumpToModified(-1)
	assert.Equal(t, "c/3.go", m.current().path)
	assert.Contains(t, m.statusMsg, "wrapped around to the last")
}

func TestUIJumpToModifiedWithNoneReports(t *testing.T) {
	tr := newTree()
	tr.add("a.go", newDigest("aaaa", 1))
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)

	m.jumpToModified(1)
	assert.Equal(t, "no modified files", m.statusMsg)
}

func TestUIFilter(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)

	m.query = "rexec_test"
	m.applyQuery()
	// Matching paths are revealed even though nothing was expanded by hand.
	assert.Equal(t, []string{
		"server",
		"server/util",
		"server/util/rexec",
		"server/util/rexec/rexec_test.go",
	}, paths(m))

	// Filtering is case insensitive.
	m.query = "REXEC_TEST"
	m.applyQuery()
	assert.Contains(t, paths(m), "server/util/rexec/rexec_test.go")

	m.query = "nothing matches this"
	m.applyQuery()
	assert.Empty(t, paths(m))
	assert.Nil(t, m.current())

	m.query = ""
	m.applyQuery()
	assert.Contains(t, paths(m), "cli")
}

func TestUIViewFillsTerminal(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20
	m.treeView.clamp(m.rowsHeight())

	lines := strings.Split(m.View().Content, "\n")
	assert.Len(t, lines, m.height)
	for _, line := range lines {
		assert.LessOrEqual(t, lipgloss.Width(line), m.width, "line wider than the terminal: %q", line)
	}

	// The modified file's marker and provenance show up once it's selected.
	m.jumpToModified(1)
	rendered := m.View().Content
	assert.Contains(t, rendered, modifiedMarker)
	assert.Contains(t, rendered, "second.binpb.zst")
}

func TestUICursorMarker(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20
	m.treeView.cursor = 2
	m.treeView.clamp(m.rowsHeight())

	for i := range m.treeView.rows {
		// Styling wraps the row in escape sequences, so compare the text.
		row := stripANSI(m.rowView(&m.treeView, i))
		if i == m.treeView.cursor {
			assert.True(t, strings.HasPrefix(row, uiCursorGutter), "cursor row %d: %q", i, row)
		} else {
			assert.True(t, strings.HasPrefix(row, uiGutter), "row %d: %q", i, row)
		}
	}
	// The gutters have to be the same width, or the tree won't line up.
	assert.Equal(t, len(uiGutter), len(uiCursorGutter))
}

// uiWideTree builds a tree with more top-level directories than fit on screen,
// none of them modified so that nothing starts out expanded.
func uiWideTree(t *testing.T, dirs int) *tree {
	t.Helper()
	tr := newTree()
	tr.logs = []logInfo{{name: "one"}}
	for i := range dirs {
		tr.add(fmt.Sprintf("dir%02d/a.go", i), newDigest(fmt.Sprintf("%04d", i), 1))
		tr.add(fmt.Sprintf("dir%02d/b.go", i), newDigest(fmt.Sprintf("%04x", i), 2))
	}
	return tr
}

// Expanding a node low on the screen scrolls it up, so the children it just
// revealed are visible rather than off the bottom.
func TestUIExpandScrollsToMidpoint(t *testing.T) {
	m := newTreeModel(uiWideTree(t, 30), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20
	height := m.rowsHeight()
	require.Equal(t, 12, height)
	mid := height / 2

	// A node near the bottom of the viewport moves up to the midpoint.
	m.treeView.cursor = 10
	m.treeView.clamp(height)
	require.Equal(t, 0, m.treeView.offset)
	m.expand()
	assert.Equal(t, 10-mid, m.treeView.offset)
	assert.Equal(t, mid, m.treeView.cursor-m.treeView.offset)
	// Its children are on screen.
	assert.Equal(t, "dir10/a.go", m.treeView.rows[11].path)
	assert.Less(t, 11-m.treeView.offset, height)
}

// A node already above the midpoint stays put: expanding shouldn't shuffle the
// screen around for no reason.
func TestUIExpandLeavesHighRowsAlone(t *testing.T) {
	m := newTreeModel(uiWideTree(t, 30), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20

	m.treeView.cursor = 2
	m.treeView.clamp(m.rowsHeight())
	m.expand()

	assert.Equal(t, 0, m.treeView.offset)
	assert.Equal(t, 2, m.treeView.cursor)
}

// Near the end of the list there aren't enough rows below to scroll a full
// half-screen, so the cursor lands as high as it can without scrolling past the
// last row.
func TestUIExpandNearTheEndStaysInBounds(t *testing.T) {
	m := newTreeModel(uiWideTree(t, 15), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20
	height := m.rowsHeight()

	m.treeView.cursor = len(m.treeView.rows) - 1
	m.treeView.clamp(height)
	m.expand()

	assert.Equal(t, len(m.treeView.rows)-height, m.treeView.offset)
	assert.GreaterOrEqual(t, m.treeView.cursor, m.treeView.offset)
	assert.Less(t, m.treeView.cursor-m.treeView.offset, height)
}

// uiVersionTree builds a tree whose logs carry invocation metadata, as they do
// when fetched from a server.
func uiVersionTree(t *testing.T) *tree {
	t.Helper()
	tr := newTree()
	tr.logs = []logInfo{
		{name: "inv-one@abc1234", invocationID: "inv-one", branch: "main"},
		{name: "inv-two@def5678", invocationID: "inv-two", branch: "fix-branch"},
	}
	tr.add("a/metacache.go", newDigest("9911eefb", 30106))
	tr.add("a/metacache.go", fromLog(newDigest("5efa467f", 11171), 1))
	tr.add("a/stable.go", newDigest("aaaabbbb", 42))
	require.Empty(t, tr.conflicts)
	return tr
}

// rows returns the rendered text of every tree row, without styling.
func renderedRows(m *treeModel) []string {
	out := make([]string, 0, len(m.treeView.rows))
	for i := range m.treeView.rows {
		out = append(out, strings.TrimRight(stripANSI(m.rowView(&m.treeView, i)), " "))
	}
	return out
}

func TestUIExpandFileShowsVersions(t *testing.T) {
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8, dirHashes: false}, filterOptions{}, nil)
	m.width, m.height = 120, 24

	// Open the directory, then the file.
	m.expand()
	m.treeView.cursor = 1 // a/metacache.go
	m.expand()

	// The cursor is on metacache.go, hence its marker in the gutter.
	assert.Equal(t, []string{
		"  └── a/ (!)",
		"o     ├── metacache.go  9911eefb  30106  +1 more (!)",
		"      │   ├── 9911eefb  inv-one  main  30106",
		"      │   └── 5efa467f  inv-two  fix-branch  11171",
		"      └── stable.go  aaaabbbb  42",
	}, renderedRows(m))

	// The versions belong to the file: collapsing it takes them away again.
	m.collapse()
	assert.Len(t, m.treeView.rows, 3)
	assert.Equal(t, "a/metacache.go", m.current().path)
}

// A version can't be collapsed, so collapsing on one closes the file it belongs
// to and highlights that.
func TestUICollapseFromVersionRowClosesFile(t *testing.T) {
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	versionCursor(t, m, 1)
	rows := len(m.treeView.rows)

	m.collapse()
	assert.Equal(t, "a/metacache.go", m.current().path)
	assert.Nil(t, m.current().version)
	assert.Len(t, m.treeView.rows, rows-2, "the versions should be gone")

	// And again closes the directory above it.
	m.collapse()
	assert.Equal(t, "a", m.current().path)
	assert.Equal(t, []string{"a"}, paths(m))
}

// Moving to a parent that has scrolled off the top brings it into view.
func TestUICollapseScrollsParentIntoView(t *testing.T) {
	m := newTreeModel(uiWideTree(t, 30), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20
	height := m.rowsHeight()

	// Open a directory and walk down past its children, so the directory
	// itself is above the top of the viewport.
	m.treeView.cursor = 3
	m.expand()
	m.treeView.cursor = len(m.treeView.rows) - 1
	m.treeView.clamp(height)
	require.Greater(t, m.treeView.offset, 3)

	// From a closed directory at the bottom, collapse moves to its parent...
	m.collapse()
	// ...which here is the root, so nothing moves: top-level rows have no
	// parent row to select.
	assert.Equal(t, len(m.treeView.rows)-1, m.treeView.cursor)

	// From one of the open directory's children, it closes the directory and
	// scrolls back up to it.
	m.treeView.cursor = 4
	m.treeView.clamp(height)
	m.collapse()
	assert.Equal(t, 3, m.treeView.cursor)
	assert.Equal(t, "dir03", m.current().path)
	assert.NotContains(t, paths(m), "dir03/a.go")
	assert.LessOrEqual(t, m.treeView.offset, 3)
	assert.Less(t, 3-m.treeView.offset, height)
}

// fakeChecker answers cache lookups from a set of digests it considers gone.
type fakeChecker struct {
	gone      map[string]bool
	err       error
	instances []string
	calls     int
}

func (f *fakeChecker) missingBlobs(ctx context.Context, instanceName string, df repb.DigestFunction_Value, digests []*repb.Digest) ([]*repb.Digest, error) {
	f.calls++
	f.instances = append(f.instances, instanceName)
	if f.err != nil {
		return nil, f.err
	}
	var missing []*repb.Digest
	for _, d := range digests {
		if f.gone[d.GetHash()] {
			missing = append(missing, d)
		}
	}
	return missing, nil
}

// cachedTree is uiVersionTree with the cache metadata a fetched log carries.
func cachedTree(t *testing.T) *tree {
	t.Helper()
	tr := uiVersionTree(t)
	tr.hashFunction = "SHA256"
	tr.repo = "https://github.com/buildbuddy-io/buildbuddy"
	for i := range tr.logs {
		tr.logs[i].cacheHost = "remote.buildbuddy.io"
		tr.logs[i].instanceName = "ci"
	}
	return tr
}

// Expanding a file asks the cache which of its versions can still be fetched,
// and each one is marked with the answer.
func TestUIExpandChecksBlobs(t *testing.T) {
	checker := &fakeChecker{gone: map[string]bool{"9911eefb": true}}
	m := newTreeModel(cachedTree(t), printOptions{hashLength: 8}, filterOptions{}, checker)
	m.width, m.height = 120, 24
	m.expand() // the directory: no blobs to check
	assert.Equal(t, 0, checker.calls)

	m.treeView.cursor = 1 // a/metacache.go
	cmd := m.expand()
	require.NotNil(t, cmd, "expanding a file should kick off a cache check")

	// Until the answer arrives the versions say so.
	assert.Equal(t, blobChecking, m.blobs[m.treeView.rows[2].version])
	assert.Contains(t, stripANSI(m.rowView(&m.treeView, 2)), "checking cache")

	msg := cmd()
	m.Update(msg)
	assert.Equal(t, 1, checker.calls)
	assert.Equal(t, []string{"ci"}, checker.instances)
	assert.Equal(t, blobEvicted, m.blobs[m.treeView.rows[2].version])
	assert.Equal(t, blobCached, m.blobs[m.treeView.rows[3].version])
	assert.Contains(t, stripANSI(m.rowView(&m.treeView, 2)), "evicted")
	assert.Contains(t, stripANSI(m.rowView(&m.treeView, 3)), "cached")
}

// A failed lookup leaves the versions unmarked rather than claiming they're
// gone.
func TestUIBlobCheckFailureLeavesVersionsUnknown(t *testing.T) {
	checker := &fakeChecker{err: fmt.Errorf("unavailable")}
	m := newTreeModel(cachedTree(t), printOptions{hashLength: 8}, filterOptions{}, checker)
	m.width, m.height = 120, 24
	m.expand()
	m.treeView.cursor = 1
	cmd := m.expand()
	require.NotNil(t, cmd)

	m.Update(cmd())
	for _, row := range m.treeView.rows {
		if row.version != nil {
			assert.Equal(t, blobUnknown, m.blobs[row.version])
			assert.NotContains(t, stripANSI(m.rowView(&m.treeView, 2)), "evicted")
		}
	}
	assert.Contains(t, m.statusMsg, "could not check the cache")
}

// Without a server there's nobody to ask, so expanding just works.
func TestUIExpandWithoutCheckerDoesNothing(t *testing.T) {
	m := newTreeModel(cachedTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	m.width, m.height = 120, 24
	m.expand()
	m.treeView.cursor = 1
	assert.Nil(t, m.expand())
	row := stripANSI(m.rowView(&m.treeView, 2))
	for _, marker := range []string{"cached", "evicted", "checking"} {
		assert.NotContains(t, row, marker)
	}
}

// Two versions of the same file go to the code viewer's diff, which needs the
// blob URIs and the repo.
func TestUICompareSameFileOpensDiffViewer(t *testing.T) {
	opened := watchBrowser(t)
	m := newTreeModel(cachedTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	versionCursor(t, m, 0)

	m.updateKey(keyPress("m"))
	m.treeView.cursor = 3
	m.updateKey(keyPress("c"))

	require.Len(t, *opened, 1)
	got, err := url.Parse((*opened)[0])
	require.NoError(t, err)
	assert.Equal(t, "/code/buildbuddy-io/buildbuddy/", got.Path)
	assert.Equal(t, "diff", got.Fragment)
	q := got.Query()
	assert.Equal(t, "bytestream://remote.buildbuddy.io/ci/blobs/9911eefb/30106", q.Get("bytestream_url"))
	assert.Equal(t, "inv-one", q.Get("invocation_id"))
	assert.Equal(t, "a/metacache.go", q.Get("filename"))
	assert.Equal(t, "bytestream://remote.buildbuddy.io/ci/blobs/5efa467f/11171", q.Get("compare_bytestream_url"))
	assert.Equal(t, "inv-two", q.Get("compare_invocation_id"))
	assert.Equal(t, "a/metacache.go", q.Get("compare_filename"))
}

// Comparing versions of two different files still compares the builds.
func TestUICompareDifferentFilesOpensInvocationCompare(t *testing.T) {
	opened := watchBrowser(t)
	tr := cachedTree(t)
	tr.add("a/stable.go", fromLog(newDigest("cccc1234", 43), 1))
	m := newTreeModel(tr, printOptions{hashLength: 8}, filterOptions{}, nil)
	versionCursor(t, m, 0)

	m.updateKey(keyPress("m"))
	// Open the other file and mark one of its versions.
	m.treeView.cursor = 4 // a/stable.go
	require.Equal(t, "a/stable.go", m.current().path)
	m.expand()
	m.treeView.cursor = 6
	require.NotNil(t, m.current().version)
	m.updateKey(keyPress("c"))

	require.Len(t, *opened, 1)
	assert.Equal(t, "https://app.example.com/compare/inv-one...inv-two", (*opened)[0])
}

// Without the cache metadata there's no way to address the blobs, so the
// comparison falls back to the builds.
func TestUICompareSameFileWithoutCacheInfo(t *testing.T) {
	opened := watchBrowser(t)
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	versionCursor(t, m, 0)

	m.updateKey(keyPress("m"))
	m.treeView.cursor = 3
	m.updateKey(keyPress("c"))

	require.Len(t, *opened, 1)
	assert.Equal(t, "https://app.example.com/compare/inv-one...inv-two", (*opened)[0])
}

func TestRepoPath(t *testing.T) {
	for repo, want := range map[string]string{
		"https://github.com/buildbuddy-io/buildbuddy":     "buildbuddy-io/buildbuddy",
		"https://github.com/buildbuddy-io/buildbuddy.git": "buildbuddy-io/buildbuddy",
		"git@github.com:buildbuddy-io/buildbuddy.git":     "buildbuddy-io/buildbuddy",
		"ssh://git@github.com/org/repo":                   "org/repo",
		"":                                                "",
		"not-a-url":                                       "",
	} {
		assert.Equal(t, want, repoPath(repo), "repoPath(%q)", repo)
	}
}

// A version row's own detail says which build it came from and where enter
// leads.
func TestUIVersionRowDetail(t *testing.T) {
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	m.width, m.height = 120, 24
	m.expand()
	m.treeView.cursor = 1
	m.expand()
	m.treeView.cursor = 3 // the second version

	detail := stripANSI(m.detailView())
	assert.Contains(t, detail, "a/metacache.go")
	assert.Contains(t, detail, "11171 bytes")
	assert.Contains(t, detail, "inv-two@def5678")
	assert.Contains(t, detail, "fix-branch")
	assert.Contains(t, detail, "/invocation/inv-two")
}

func TestUIEnterOpensInvocation(t *testing.T) {
	var opened []string
	old := openInBrowser
	openInBrowser = func(url string) error {
		opened = append(opened, url)
		return nil
	}
	oldURL := *appURL
	*appURL = "https://app.example.com/"
	t.Cleanup(func() { openInBrowser, *appURL = old, oldURL })

	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	m.width, m.height = 120, 24
	m.expand()
	m.treeView.cursor = 1

	// Enter on the file expands it rather than opening anything.
	m.updateKey(keyPress("enter"))
	assert.Empty(t, opened)

	m.treeView.cursor = 2 // the first version
	m.updateKey(keyPress("enter"))
	require.Equal(t, []string{"https://app.example.com/invocation/inv-one"}, opened)
	assert.Contains(t, m.statusMsg, "opened")
}

func TestUIEnterReportsBrowserFailures(t *testing.T) {
	old := openInBrowser
	openInBrowser = func(url string) error { return fmt.Errorf("no opener") }
	t.Cleanup(func() { openInBrowser = old })

	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	m.expand()
	m.treeView.cursor = 1
	m.expand()
	m.treeView.cursor = 2
	m.updateKey(keyPress("enter"))

	assert.Contains(t, m.statusMsg, "could not open")
	assert.Contains(t, m.statusMsg, "no opener")
}

// Logs read from local files have no invocation to open.
func TestUIEnterWithoutInvocation(t *testing.T) {
	old := openInBrowser
	opened := false
	openInBrowser = func(url string) error { opened = true; return nil }
	t.Cleanup(func() { openInBrowser = old })

	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)
	m.jumpToModified(1)
	m.expand()
	m.treeView.move(1, m.rowsHeight())
	require.NotNil(t, m.current().version)

	m.updateKey(keyPress("enter"))
	assert.False(t, opened)
	assert.Contains(t, m.statusMsg, "no invocation to open")
}

// versionCursor opens the file at a/metacache.go and puts the cursor on one of
// its version rows.
func versionCursor(t *testing.T, m *treeModel, which int) {
	t.Helper()
	m.width, m.height = 120, 24
	m.expand()            // a/
	m.treeView.cursor = 1 // a/metacache.go
	m.expand()
	m.treeView.cursor = 2 + which
	require.NotNil(t, m.current().version, "row %d isn't a version row", m.treeView.cursor)
}

// watchBrowser captures the URLs the UI would open.
func watchBrowser(t *testing.T) *[]string {
	t.Helper()
	var opened []string
	old := openInBrowser
	openInBrowser = func(url string) error {
		opened = append(opened, url)
		return nil
	}
	oldURL := *appURL
	*appURL = "https://app.example.com"
	t.Cleanup(func() { openInBrowser, *appURL = old, oldURL })
	return &opened
}

func TestUICompareBaseAndCompare(t *testing.T) {
	opened := watchBrowser(t)
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	versionCursor(t, m, 0)

	m.updateKey(keyPress("m"))
	require.NotNil(t, m.base)
	assert.Equal(t, "a/metacache.go", m.base.path)
	assert.Contains(t, m.statusMsg, "compare base")
	// The base is marked in the tree and in the title.
	assert.Contains(t, stripANSI(m.rowView(&m.treeView, 2)), "(base)")
	assert.Contains(t, stripANSI(m.titleView()), "base 9911eefb")

	// Comparing from the base itself is refused: it's the same build.
	m.updateKey(keyPress("c"))
	assert.Empty(t, *opened)
	assert.Contains(t, m.statusMsg, "same build")

	// From the other version, the base comes first in the URL.
	m.treeView.cursor = 3
	m.updateKey(keyPress("c"))
	assert.Equal(t, []string{"https://app.example.com/compare/inv-one...inv-two"}, *opened)
}

func TestUICompareBaseTogglesOff(t *testing.T) {
	watchBrowser(t)
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	versionCursor(t, m, 0)

	m.updateKey(keyPress("m"))
	require.NotNil(t, m.base)
	m.updateKey(keyPress("m"))
	assert.Nil(t, m.base)
	assert.Equal(t, "compare base cleared", m.statusMsg)
	assert.NotContains(t, stripANSI(m.rowView(&m.treeView, 2)), "(base)")
}

func TestUICompareWithoutBase(t *testing.T) {
	opened := watchBrowser(t)
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	versionCursor(t, m, 0)

	m.updateKey(keyPress("c"))
	assert.Empty(t, *opened)
	assert.Contains(t, m.statusMsg, "no compare base yet")
}

// c only means something on a file version.
func TestUICompareFromNonVersionRow(t *testing.T) {
	opened := watchBrowser(t)
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	versionCursor(t, m, 0)
	m.updateKey(keyPress("m"))

	m.treeView.cursor = 0 // the directory
	m.updateKey(keyPress("c"))
	assert.Empty(t, *opened)
	assert.Contains(t, m.statusMsg, "select a file version")
}

// m only means anything on a file version, and says so elsewhere rather than
// doing something else.
func TestUIBaseKeyOnlyAppliesToVersionRows(t *testing.T) {
	watchBrowser(t)
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	m.width, m.height = 120, 24

	m.updateKey(keyPress("m"))
	assert.Nil(t, m.base)
	assert.False(t, m.show.includeGenerated)
	assert.Contains(t, m.statusMsg, "select a file version")
}

// The most-changed view expands files into the same version rows as the tree,
// with the same comparison keys.
func TestUITopViewExpandsVersions(t *testing.T) {
	opened := watchBrowser(t)
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	m.width, m.height = 120, 24
	m.updateKey(keyPress("t"))
	require.Equal(t, []string{"2 a/metacache.go", "1 a/stable.go"}, topPaths(m))

	m.updateKey(keyPress("l"))
	rows := m.topView.rows
	require.Len(t, rows, 4)
	assert.Nil(t, rows[0].version)
	require.NotNil(t, rows[1].version)
	require.NotNil(t, rows[2].version)
	assert.Nil(t, rows[3].version)
	// Versions line up under the path (2-column gutter + 6-column count
	// column), rather than being drawn as a tree.
	assert.Equal(t, uiGutter+uiTopIndent+"├── 9911eefb  inv-one  main  30106",
		strings.TrimRight(stripANSI(m.rowView(&m.topView, 1)), " "))
	assert.Equal(t, len(uiGutter+uiTopIndent),
		strings.Index(stripANSI(m.rowView(&m.topView, 0)), "a/metacache.go"))

	// Set a base here and compare against the other version, without ever
	// leaving this view.
	m.topView.cursor = 1
	m.updateKey(keyPress("m"))
	require.NotNil(t, m.base)
	m.topView.cursor = 2
	m.updateKey(keyPress("c"))
	assert.Equal(t, []string{"https://app.example.com/compare/inv-one...inv-two"}, *opened)

	// Enter opens the build rather than jumping to the tree.
	m.updateKey(keyPress("enter"))
	assert.Equal(t, "https://app.example.com/invocation/inv-two", (*opened)[len(*opened)-1])
	assert.Equal(t, viewTop, m.view)

	// Collapsing from a version closes the file, as in the tree.
	m.updateKey(keyPress("h"))
	assert.Len(t, m.topView.rows, 2)
	assert.Equal(t, "a/metacache.go", m.current().path)
}

// Expansion is shared: a file opened in one view is open in the other.
func TestUITopViewExpansionIsSharedWithTree(t *testing.T) {
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	m.width, m.height = 120, 24
	m.view = viewTop

	m.updateKey(keyPress("l"))
	require.Len(t, m.topView.rows, 4)

	// The tree has the versions too, once the directory above them is open.
	m.updateKey(keyPress("t"))
	m.expand()
	assert.Equal(t, []string{
		"a", "a/metacache.go", "a/metacache.go", "a/metacache.go", "a/stable.go",
	}, paths(m))
}

// Enter on a file in the most-changed view still jumps to it in the tree.
func TestUITopViewEnterOnFileRevealsInTree(t *testing.T) {
	watchBrowser(t)
	m := newTreeModel(uiVersionTree(t), printOptions{hashLength: 8}, filterOptions{}, nil)
	m.width, m.height = 120, 24
	m.view = viewTop

	m.updateKey(keyPress("enter"))
	assert.Equal(t, viewTree, m.view)
	assert.Equal(t, "a/metacache.go", m.current().path)
}

// infoModel builds a model over a real log, positioned on a file that something
// in that build compiled.
func infoModel(t *testing.T) (*treeModel, string) {
	t.Helper()
	// Point the log cache somewhere empty, so logFile finds the log by the name
	// it was parsed under rather than a stray entry from a real run.
	useTempLogCache(t)
	tr := newTree()
	require.NoError(t, tr.parse(log1))
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 30

	const path = "cli/printlog/compact/compact.go"
	var target *uiRow
	for i := range m.files {
		if m.files[i].path == path {
			target = &m.files[i]
		}
	}
	require.NotNil(t, target, "the example log should contain %s", path)
	m.reveal(target)
	require.Equal(t, path, m.current().path)
	return m, path
}

// infoText is the info view's rows as plain lines.
func infoText(m *treeModel) string {
	lines := make([]string, 0, len(m.infoView.rows))
	for _, r := range m.infoView.rows {
		lines = append(lines, stripANSI(r.text))
	}
	return strings.Join(lines, "\n")
}

func TestUIInfoView(t *testing.T) {
	m, path := infoModel(t)

	cmd := m.updateKey(keyPress("i"))
	require.NotNil(t, cmd, "i should start reading the logs")
	assert.Equal(t, viewInfo, m.view)
	assert.Contains(t, infoText(m), "reading", "the view says what it's waiting on")
	assert.Contains(t, stripANSI(m.titleView()), "info · "+path)

	m.Update(cmd())

	text := infoText(m)
	assert.Contains(t, text, path)
	assert.Contains(t, text, "from 1 build")
	// It's a source file, so nothing in any build wrote it.
	assert.Contains(t, text, "no spawn found that outputs this file.")
	// The steps that used it are counted, and closed until asked for.
	assert.Contains(t, text, "used as an input by 1 target · 2 steps")
	assert.NotContains(t, text, "GoCompilePkg")
	// And every version we merged.
	assert.Contains(t, text, "versions seen (1)")
	// In the info view the cursor is on a line of prose; the file it describes
	// is the subject it was opened from.
	require.NotNil(t, m.infoSubject)
	assert.Contains(t, text, m.opts.formatHash(m.infoSubject.node.entry.versions[0].key()))

	// Opening the list shows the steps themselves, and nothing under them.
	m.infoView.cursor = groupRow(t, m, groupConsumers, &m.infoView)
	m.updateKey(keyPress("l"))
	text = infoText(m)
	assert.Contains(t, text, "//cli/printlog/compact:compact  GoCompilePkg")
	assert.Contains(t, text, "//cli/printlog/compact:compact  RunNogo")
	assert.NotContains(t, text, "feeds")

	// i again goes back.
	m.updateKey(keyPress("i"))
	assert.Equal(t, viewTree, m.view)
	assert.Equal(t, path, m.current().path)
}

// A generated file names the steps that wrote it, and what they were made from.
func TestUIInfoViewShowsWhatProducedAFile(t *testing.T) {
	useTempLogCache(t)
	tr := newTree()
	require.NoError(t, tr.parse(log1))

	const generated = "bazel-out/linux_x86_64_musl-fastbuild/bin/cli/printlog/compact/compact.a"
	n := tr.find(generated)
	require.NotNil(t, n, "the example log should contain %s", generated)

	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 40
	m.infoSubject = &uiRow{node: n, path: generated}
	m.fileInfo = &fileDetail{path: generated}
	m.view = viewInfo
	m.Update(fileInfoMsg{path: generated, summary: summarizeFile([]string{generated}, []logSource{{path: log1}})})

	m.infoView.cursor = groupRow(t, m, groupProducers, &m.infoView)
	m.updateKey(keyPress("l"))
	text := infoText(m)

	assert.Contains(t, text, "output by 1 target")
	assert.Contains(t, text, "//cli/printlog/compact:compact  GoCompilePkg")
	// What it went on to feed is the other row's business.
	assert.Less(t, strings.Index(text, "output by"), strings.Index(text, "used as an input by"))
}

// A graph already loaded for a target answers for that build without reading it
// again - even if we no longer have the file it came from.
func TestUIInfoViewReusesGraph(t *testing.T) {
	useTempLogCache(t)
	tr := newTree()
	require.NoError(t, tr.parseFile(logInfo{name: "gone.binpb.zst"}, log1))
	graph, err := loadSpawnGraph(log1)
	require.NoError(t, err)

	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 40
	m.graph, m.graphLog = graph, 0

	const path = "cli/printlog/compact/compact.go"
	target := tr.find(path)
	require.NotNil(t, target)
	m.reveal(&uiRow{node: target, path: path, ancestors: ancestorsOf(m, path)})
	require.Equal(t, path, m.current().path)

	cmd := m.updateKey(keyPress("i"))
	require.NotNil(t, cmd)
	m.Update(cmd())

	text := infoText(m)
	assert.Contains(t, text, "from 1 build")
	assert.Contains(t, text, "used as an input by 1 target")
	assert.NotContains(t, text, "not kept locally")
}

// ancestorsOf lists the directories that have to be open to reveal a path.
func ancestorsOf(m *treeModel, path string) []*node {
	var ancestors []*node
	n := m.t.root
	parts := splitPath(path)
	for _, part := range parts[:len(parts)-1] {
		n = n.children[part]
		ancestors = append(ancestors, n)
	}
	return ancestors
}

// A step listed on the file info page belongs to a target, and both keys that
// mean "tell me about this" go there.
func TestUIInfoViewFollowsSteps(t *testing.T) {
	for _, key := range []string{"i", "enter"} {
		t.Run(key, func(t *testing.T) {
			m, _ := infoModel(t)
			m.Update(m.updateKey(keyPress("i"))())
			m.infoView.cursor = groupRow(t, m, groupConsumers, &m.infoView)
			m.updateKey(keyPress("l"))
			m.infoView.cursor++

			row := m.current()
			require.NotNil(t, row.target, "a step row should carry the target it belongs to")
			label := row.target.label

			cmd := m.updateKey(keyPress(key))
			require.NotNil(t, cmd, "%s should start reading the log", key)
			m.Update(cmd())
			assert.Equal(t, viewTargetInfo, m.view)
			assert.Equal(t, label, m.targetSubject.label)

			// And going back lands on the file it was opened from, not on the
			// targets list.
			m.updateKey(keyPress("esc"))
			assert.Equal(t, viewInfo, m.view)
			assert.Contains(t, infoText(m), "used as an input by")
		})
	}
}

// The lists are pooled across every build that mentions the file: a step that
// only used it in an older build is still one of the steps that used it.
func TestUIInfoViewPoolsEveryBuild(t *testing.T) {
	useTempLogCache(t)
	dir := t.TempDir()
	newest := filepath.Join(dir, "newest.binpb.zst")
	older := filepath.Join(dir, "older.binpb.zst")
	// The same source, compiled by a different target in each build.
	require.NoError(t, os.WriteFile(newest, graphLog(t,
		fileEntry(1, "shared.go"),
		fileEntry(2, "new.a"),
		setEntry(10, []uint32{1}, nil),
		spawnEntry("//new:new", "GoCompilePkg", 10, 2),
	), 0644))
	require.NoError(t, os.WriteFile(older, graphLog(t,
		fileEntry(1, "shared.go"),
		fileEntry(2, "old.a"),
		setEntry(10, []uint32{1}, nil),
		spawnEntry("//old:old", "GoCompilePkg", 10, 2),
	), 0644))

	tr := newTree()
	require.NoError(t, tr.parse(newest))
	require.NoError(t, tr.parse(older))
	n := tr.find("shared.go")
	require.NotNil(t, n)
	// The file is in both logs, and the view has to read both to know that.
	require.Equal(t, []int{0, 1}, n.entry.logs().indexes())

	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 40
	for i, row := range m.treeView.rows {
		if row.path == "shared.go" {
			m.treeView.cursor = i
		}
	}
	require.Equal(t, "shared.go", m.current().path)
	m.Update(m.updateKey(keyPress("i"))())

	assert.Contains(t, infoText(m), "from 2 builds")
	assert.Contains(t, infoText(m), "used as an input by 2 targets")
	m.infoView.cursor = groupRow(t, m, groupConsumers, &m.infoView)
	m.updateKey(keyPress("l"))
	text := infoText(m)
	assert.Contains(t, text, "//new:new  GoCompilePkg")
	assert.Contains(t, text, "//old:old  GoCompilePkg")
}

// A build that mentions the file but whose log we no longer have is counted,
// not passed over in silence.
func TestUIInfoViewReportsMissingLogs(t *testing.T) {
	useTempLogCache(t)
	tr := newTree()
	require.NoError(t, tr.parse(log1))
	// A second build that also had the file, whose log is gone.
	tr.beginLog(logInfo{name: "gone.binpb.zst"})
	const path = "cli/printlog/compact/compact.go"
	n := tr.find(path)
	require.NotNil(t, n)
	n.entry.versions[0].logs = n.entry.versions[0].logs.set(1)

	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 40
	m.reveal(&uiRow{node: n, path: path, ancestors: ancestorsOf(m, path)})
	m.Update(m.updateKey(keyPress("i"))())

	text := infoText(m)
	assert.Contains(t, text, "from 1 build")
	assert.Contains(t, text, "1 not kept locally")
}

// esc leaves the info view, the same as it leaves the most-changed view.
func TestUIInfoViewEscapes(t *testing.T) {
	m, _ := infoModel(t)
	m.Update(m.updateKey(keyPress("i"))())
	m.updateKey(keyPress("esc"))
	assert.Equal(t, viewTree, m.view)
}

// Directories have no inputs to report.
func TestUIInfoViewNeedsAFile(t *testing.T) {
	m, _ := infoModel(t)
	m.collapse() // move up to the directory
	require.False(t, m.current().node.isDir() == false)

	assert.Nil(t, m.updateKey(keyPress("i")))
	assert.Equal(t, viewTree, m.view)
	assert.Contains(t, m.statusMsg, "select a file")
}

// Without a local copy of the log there's nothing to read.
func TestUIInfoViewWithoutTheLog(t *testing.T) {
	useTempLogCache(t)
	m := newTreeModel(uiVersionTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 30
	m.expand()
	m.treeView.cursor = 1

	assert.Nil(t, m.updateKey(keyPress("i")))
	assert.Equal(t, viewTree, m.view)
	assert.Contains(t, m.statusMsg, "no local copy")
}

// A result that arrives after the user has moved on is dropped.
func TestUIInfoViewIgnoresStaleResults(t *testing.T) {
	m, _ := infoModel(t)
	m.Update(m.updateKey(keyPress("i"))())
	before := infoText(m)

	m.Update(fileInfoMsg{path: "some/other/file.go", summary: &fileSummary{read: 7}})
	assert.Equal(t, before, infoText(m))
}

// Logs that couldn't be read are counted rather than passed off as an answer.
func TestUIInfoViewReportsReadErrors(t *testing.T) {
	m, path := infoModel(t)
	m.updateKey(keyPress("i"))

	m.Update(fileInfoMsg{path: path, summary: &fileSummary{read: 2, failed: 1}})
	text := infoText(m)
	assert.Contains(t, text, "from 2 builds")
	assert.Contains(t, text, "1 could not be read")
	// The versions are still listed: they come from the tree, not the logs.
	assert.Contains(t, text, "versions seen")

	// With nothing read at all, an empty list would be a claim we can't make.
	m.Update(fileInfoMsg{path: path, summary: &fileSummary{failed: 3}})
	text = infoText(m)
	assert.Contains(t, text, "could not read any of the 3 logs")
	assert.NotContains(t, text, "no spawn found")
}

// The info view's rows are prose, not nodes, so every key that acts on a node
// has to cope with there not being one.
func TestUIInfoViewSurvivesEveryKey(t *testing.T) {
	watchBrowser(t)
	for _, key := range []string{
		"enter", " ", "l", "h", "right", "left", "j", "k", "g", "G", "n", "N",
		"m", "c", "b", "x", "t", "/", "esc",
	} {
		t.Run(key, func(t *testing.T) {
			m, _ := infoModel(t)
			m.Update(m.updateKey(keyPress("i"))())
			require.Equal(t, viewInfo, m.view)

			// Whatever the key does, it mustn't panic or leave the view
			// showing rows it can't render.
			m.updateKey(keyPress(key))
			m.View()
		})
	}
}

// infoRowWithVersion returns the index of the first info row that carries a
// version, which are the rows the compare keys act on.
func infoRowWithVersion(t *testing.T, m *treeModel) int {
	t.Helper()
	for i, r := range m.infoView.rows {
		if r.version != nil {
			return i
		}
	}
	t.Fatal("the info view should list the file's versions as rows")
	return -1
}

// The versions listed in the info view can be marked and compared without
// leaving it.
func TestUIInfoViewComparesVersions(t *testing.T) {
	opened := watchBrowser(t)
	useTempLogCache(t)
	const path = "cli/printlog/compact/compact.go"

	tr := newTree()
	require.NoError(t, tr.parse(log1))
	// Nothing actually changed between the example logs, so give this file a
	// second version, and the logs the invocation metadata a fetched log would
	// carry.
	tr.repo = "https://github.com/buildbuddy-io/buildbuddy"
	tr.logs[0].invocationID = "inv-one"
	tr.logs[0].cacheHost, tr.logs[0].instanceName = "remote.buildbuddy.io", "ci"
	tr.logs = append(tr.logs, logInfo{
		name:         "second.binpb.zst",
		invocationID: "inv-two",
		cacheHost:    "remote.buildbuddy.io",
		instanceName: "ci",
	})
	tr.add(path, fromLog(newDigest("ffff0000", 42), 1))

	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 30
	var target *uiRow
	for i := range m.files {
		if m.files[i].path == path {
			target = &m.files[i]
		}
	}
	require.NotNil(t, target)
	m.reveal(target)

	m.Update(m.updateKey(keyPress("i"))())
	require.Equal(t, viewInfo, m.view)

	first := infoRowWithVersion(t, m)
	m.infoView.cursor = first
	m.updateKey(keyPress("m"))
	require.NotNil(t, m.base, "m should mark the version under the cursor")
	assert.Equal(t, path, m.base.path)
	assert.Equal(t, viewInfo, m.view, "marking shouldn't leave the info view")
	// The marked row says so, and the help line offers the compare.
	assert.Contains(t, stripANSI(m.rowView(&m.infoView, first)), uiBaseMarker)
	assert.Contains(t, stripANSI(m.helpView()), "c compare")

	// The next version down compares against it, still without leaving.
	m.infoView.cursor = first + 1
	require.NotNil(t, m.current().version)
	m.updateKey(keyPress("c"))
	assert.Equal(t, viewInfo, m.view)
	require.Len(t, *opened, 1)
	// Same file, two builds: the code viewer's diff.
	assert.Contains(t, (*opened)[0], "/code/")
	assert.Contains(t, (*opened)[0], "compare_invocation_id=inv-two")
}

// n and N step between modified files, which would mean leaving the file the
// info view is about.
func TestUIInfoViewIgnoresNextModified(t *testing.T) {
	m, path := infoModel(t)
	m.Update(m.updateKey(keyPress("i"))())

	for _, key := range []string{"n", "N"} {
		m.updateKey(keyPress(key))
		assert.Equal(t, viewInfo, m.view, "%s shouldn't leave the info view", key)
		assert.Equal(t, path, m.filePath())
		assert.Empty(t, m.statusMsg)
	}
}

// Expanding and collapsing only mean something on a node.
func TestUIExpandCollapseIgnoreProseRows(t *testing.T) {
	m, _ := infoModel(t)
	m.Update(m.updateKey(keyPress("i"))())
	rows := len(m.infoView.rows)

	assert.Nil(t, m.expand())
	m.collapse()

	assert.Equal(t, viewInfo, m.view)
	assert.Len(t, m.infoView.rows, rows)
	// Nothing got marked expanded on the way through.
	assert.NotContains(t, m.expanded, (*node)(nil))
}

// targetsModel builds a model over a real log, showing its targets.
func targetsModel(t *testing.T) *treeModel {
	t.Helper()
	// Point the log cache somewhere empty, so logFile finds the log by the name
	// it was parsed under rather than a stray entry from a real run.
	useTempLogCache(t)
	tr := newTree()
	require.NoError(t, tr.parse(log1))
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 30

	m.updateKey(keyPress("T"))
	require.Equal(t, viewTargets, m.view)
	require.NotEmpty(t, m.targetsView.rows, "the example log should have run some targets")
	return m
}

// compileTarget is the target that compiled a known source file in the example
// log, which is a target we know has inputs, outputs and a build to point at.
func compileTarget(t *testing.T) targetMnemonic {
	t.Helper()
	g, err := loadSpawnGraph(log1)
	require.NoError(t, err)
	_, consumers := g.stepsFor("cli/printlog/compact/compact.go")
	require.NotEmpty(t, consumers)
	return consumers[0]
}

// targetInfoModel opens the target info view for a target, and returns the
// model and the step it was opened for.
func targetInfoModel(t *testing.T) (*treeModel, targetMnemonic) {
	t.Helper()
	step := compileTarget(t)
	m := targetsModel(t)
	found := false
	for i, row := range m.targetsView.rows {
		if row.target.label == step.target {
			m.targetsView.cursor, found = i, true
			break
		}
	}
	require.True(t, found, "the targets view should list %s", step.target)

	cmd := m.updateKey(keyPress("i"))
	require.NotNil(t, cmd, "i should start reading the log")
	require.Equal(t, viewTargetInfo, m.view)
	require.Contains(t, targetInfoText(m), "reading", "the view says what it's waiting on")
	m.Update(cmd())
	return m, step
}

// targetInfoText renders the target info view's rows as plain lines.
func targetInfoText(m *treeModel) string {
	lines := make([]string, 0, len(m.targetInfoView.rows))
	for i := range m.targetInfoView.rows {
		lines = append(lines, strings.TrimRight(stripANSI(m.rowView(&m.targetInfoView, i)), " "))
	}
	return strings.Join(lines, "\n")
}

// groupRow is the index of the summary row of the given kind in a view.
func groupRow(t *testing.T, m *treeModel, kind groupKind, views ...*viewport) int {
	t.Helper()
	v := &m.targetInfoView
	if len(views) > 0 {
		v = views[0]
	}
	for i, row := range v.rows {
		if row.group != nil && row.group.kind == kind {
			return i
		}
	}
	t.Fatalf("the view should have a summary row of kind %v", kind)
	return -1
}

// pathRow is the index of the row for a file the step touched.
func pathRow(t *testing.T, m *treeModel, path string) int {
	t.Helper()
	for i, row := range m.targetInfoView.rows {
		if row.node != nil && row.path == path {
			return i
		}
	}
	t.Fatalf("no row for %s", path)
	return -1
}

func TestUITargetsView(t *testing.T) {
	m := targetsModel(t)

	// Every row is a target, and the counts never go up as you read down.
	last := len(m.targetsView.rows[0].target.logs)
	for _, row := range m.targetsView.rows {
		require.NotNil(t, row.target)
		assert.LessOrEqual(t, len(row.target.logs), last, row.target.label)
		last = len(row.target.logs)
	}
	first := m.targetsView.rows[0].target
	assert.Equal(t, fmt.Sprintf("%4d  %s", len(first.logs), first.label),
		strings.TrimRight(stripANSI(m.rowView(&m.targetsView, 0))[len(uiCursorGutter):], " "))
	assert.Contains(t, stripANSI(m.titleView()), "targets ·")
	// The detail pane says what the target ran.
	assert.Contains(t, stripANSI(m.detailView()), first.label)

	// T goes back where it came from.
	m.updateKey(keyPress("T"))
	assert.Equal(t, viewTree, m.view)
}

// The query filters the targets list, the same key that filters the tree.
func TestUITargetsViewFilters(t *testing.T) {
	m := targetsModel(t)
	all := len(m.targetsView.rows)

	m.updateKey(keyPress("/"))
	for _, key := range []string{"c", "l", "i"} {
		m.updateQuery(keyPress(key))
	}
	m.updateQuery(keyPress("enter"))

	require.NotEmpty(t, m.targetsView.rows)
	assert.Less(t, len(m.targetsView.rows), all)
	for _, row := range m.targetsView.rows {
		assert.Contains(t, strings.ToLower(row.target.label), "cli")
	}
	assert.Contains(t, stripANSI(m.titleView()), "/cli")
}

func TestUITargetInfoView(t *testing.T) {
	m, step := targetInfoModel(t)

	text := targetInfoText(m)
	assert.Contains(t, text, step.target)
	assert.Contains(t, text, step.mnemonic)
	assert.Contains(t, text, "inputs")
	assert.Contains(t, text, "outputs")
	assert.Contains(t, text, "invocations")
	assert.Contains(t, stripANSI(m.titleView()), "target · "+step.target)
	// The step says how widely it ran, and the lists how big they are.
	assert.Contains(t, text, "ran in 1 build")
	// The graph is kept, so the next target from the same build reuses it.
	assert.NotNil(t, m.graph)

	// The inputs open into the files the step consumed, including the source we
	// picked the target for.
	m.targetInfoView.cursor = groupRow(t, m, groupInputs)
	m.updateKey(keyPress("l"))
	assert.Contains(t, targetInfoText(m), "cli/printlog/compact/compact.go")

	// A file opens into its versions, the same as in the tree.
	m.targetInfoView.cursor = pathRow(t, m, "cli/printlog/compact/compact.go")
	m.updateKey(keyPress("l"))
	version := m.targetInfoView.rows[m.targetInfoView.cursor+1]
	require.NotNil(t, version.version)
	assert.Equal(t, "cli/printlog/compact/compact.go", version.path)

	// Closing the file leaves the highlight on it; closing again closes the
	// list it's in and moves there.
	m.updateKey(keyPress("h"))
	assert.Equal(t, "cli/printlog/compact/compact.go", m.current().path)
	m.updateKey(keyPress("h"))
	require.NotNil(t, m.current().group)
	assert.Equal(t, groupInputs, m.current().group.kind)
	assert.NotContains(t, targetInfoText(m), "cli/printlog/compact/compact.go")
}

// The step header says how its builds split across os and arch, which comes
// from the platform its spawns ran on.
func TestUITargetInfoShowsPlatforms(t *testing.T) {
	m, step := targetInfoModel(t)

	var found *targetStep
	for _, s := range m.targetInfo.steps {
		if s.mnemonic == step.mnemonic {
			found = s
		}
	}
	require.NotNil(t, found)
	// The example log ran everything remotely on one platform.
	require.Equal(t, []platformTally{{platform: "linux_amd64", builds: 1}}, found.platforms)
	assert.Contains(t, targetInfoText(m), "ran in 1 build (linux_amd64: 1)")
}

// The dependencies row opens into the steps that generated what this target
// consumed, each opening into the files that connect them, each of those into
// its versions.
func TestUITargetInfoDependencies(t *testing.T) {
	m, _ := targetInfoModel(t)
	assert.Contains(t, targetInfoText(m), "dependencies")

	m.targetInfoView.cursor = groupRow(t, m, groupDependencies)
	require.NotEmpty(t, m.targetInfo.dependencies, "a compile depends on what generated its inputs")
	m.updateKey(keyPress("l"))

	edge := m.targetInfoView.rows[m.targetInfoView.cursor+1]
	require.NotNil(t, edge.group)
	require.Equal(t, groupEdge, edge.group.kind)
	assert.Contains(t, stripANSI(edge.text), edge.group.edge.step.target)
	assert.Contains(t, stripANSI(edge.text), edge.group.edge.step.mnemonic)

	// The files that dependency generated for us, which are generated files -
	// and so hidden in the tree - but shown here regardless.
	require.False(t, m.show.includeGenerated)
	m.targetInfoView.cursor++
	m.updateKey(keyPress("l"))
	file := m.targetInfoView.rows[m.targetInfoView.cursor+1]
	require.NotNil(t, file.node, "the file should resolve to a node in the tree")
	assert.True(t, strings.HasPrefix(file.path, "bazel-out/"), file.path)
	assert.Contains(t, stripANSI(targetInfoText(m)), file.path)

	// And that file opens into its versions.
	m.targetInfoView.cursor += 1
	m.updateKey(keyPress("l"))
	assert.NotNil(t, m.targetInfoView.rows[m.targetInfoView.cursor+1].version)

	// Closing walks back out the way it came.
	m.updateKey(keyPress("h"))
	assert.Equal(t, file.path, m.current().path)
	m.updateKey(keyPress("h"))
	require.NotNil(t, m.current().group)
	assert.Equal(t, groupEdge, m.current().group.kind)
	m.updateKey(keyPress("h"))
	require.NotNil(t, m.current().group)
	assert.Equal(t, groupDependencies, m.current().group.kind)
}

// The dependents row is the same list read the other way, so a target that
// nothing consumes says so rather than showing an empty list.
func TestUITargetInfoDependents(t *testing.T) {
	m, step := targetInfoModel(t)
	row := groupRow(t, m, groupDependents)

	text := stripANSI(m.targetInfoView.rows[row].text)
	assert.Contains(t, text, "dependents")
	assert.Contains(t, text, edgeSummary(m.targetInfo.dependents))
	// The example log's compile feeds something: the target it belongs to.
	require.NotEmpty(t, m.targetInfo.dependents, "%s should feed something", step.target)

	m.targetInfoView.cursor = row
	m.updateKey(keyPress("l"))
	edge := m.targetInfoView.rows[row+1]
	require.NotNil(t, edge.group)
	assert.Equal(t, groupEdge, edge.group.kind)
	// A dependent takes our outputs, so the files listed under it are ours.
	m.targetInfoView.cursor = row + 1
	m.updateKey(keyPress("l"))
	file := m.targetInfoView.rows[row+2]
	require.NotNil(t, file.node)
	assert.Contains(t, m.targetInfo.steps[0].outputs, file.path)
}

// The counts name targets, and say how many steps of theirs are involved when
// that's a different number.
func TestEdgeSummary(t *testing.T) {
	edges := []*targetEdge{
		{graphEdge: graphEdge{step: targetMnemonic{target: "//a:a", mnemonic: "GoCompilePkg"}}},
		{graphEdge: graphEdge{step: targetMnemonic{target: "//b:b", mnemonic: "GoCompilePkg"}}},
	}
	assert.Equal(t, "2 targets", edgeSummary(edges))
	assert.Equal(t, "1 target", edgeSummary(edges[:1]))
	assert.Equal(t, "0 targets", edgeSummary(nil))

	// Two steps of the same target count once as a target, but are still two
	// rows.
	edges[1].step.target = "//a:a"
	edges[1].step.mnemonic = "GoLink"
	assert.Equal(t, "1 target · 2 steps", edgeSummary(edges))
}

func TestUITargetInfoOutputsAndInvocations(t *testing.T) {
	m, _ := targetInfoModel(t)

	m.targetInfoView.cursor = groupRow(t, m, groupOutputs)
	m.updateKey(keyPress("l"))
	// A compile writes something, and what it writes is generated.
	outputs := m.targetInfoView.rows[m.targetInfoView.cursor+1]
	assert.Contains(t, outputs.path+outputs.text, "bazel-out")

	m.targetInfoView.cursor = groupRow(t, m, groupInvocations)
	m.updateKey(keyPress("l"))
	build := m.targetInfoView.rows[m.targetInfoView.cursor+1]
	require.NotNil(t, build.invocation)
	assert.Equal(t, log1, build.invocation.name)
}

// The builds a step ran in are listed newest first, with their timestamps, and
// enter opens one.
func TestUITargetInfoInvocationRows(t *testing.T) {
	opened := watchBrowser(t)
	m := &treeModel{
		t:          newTree(),
		openGroups: map[*infoGroup]bool{},
		width:      120,
		height:     30,
	}
	m.t.logs = []logInfo{
		{name: "older", invocationID: "inv-old", branch: "main", updatedAtUsec: 1_000_000},
		{name: "newer", invocationID: "inv-new", branch: "main", updatedAtUsec: 2_000_000},
	}
	rows := m.invocationRows(m.t.sortLogsByTime([]int{0, 1}))

	require.Len(t, rows, 2)
	assert.Contains(t, stripANSI(rows[0].text), "newer")
	assert.Contains(t, stripANSI(rows[0].text), "1970-01-01 00:00:02")
	assert.Contains(t, stripANSI(rows[1].text), "older")

	m.view = viewTargetInfo
	m.targetInfoView.rows = rows
	m.updateKey(keyPress("enter"))
	assert.Equal(t, []string{"https://app.example.com/invocation/inv-new"}, *opened)
	// The detail pane describes the build the cursor is on.
	assert.Contains(t, stripANSI(m.detailView()), "inv-new")
}

// The versions listed under a step can be marked and compared without leaving
// the view, the same as anywhere else.
func TestUITargetInfoComparesVersions(t *testing.T) {
	opened := watchBrowser(t)
	m, _ := targetInfoModel(t)
	// Give the file a second version from a second build, as a run over more
	// than one log would have.
	m.t.logs = append(m.t.logs, logInfo{name: "second", invocationID: "inv-two"})
	m.t.logs[0].invocationID = "inv-one"
	n := m.t.find("cli/printlog/compact/compact.go")
	require.NotNil(t, n)
	n.entry.add(fromLog(newDigest("ffff", 6), 1))

	m.targetInfoView.cursor = groupRow(t, m, groupInputs)
	m.updateKey(keyPress("l"))
	m.targetInfoView.cursor = pathRow(t, m, "cli/printlog/compact/compact.go")
	m.updateKey(keyPress("l"))

	m.targetInfoView.cursor++
	require.NotNil(t, m.current().version)
	m.updateKey(keyPress("m"))
	require.NotNil(t, m.base)
	assert.Contains(t, stripANSI(m.rowView(&m.targetInfoView, m.targetInfoView.cursor)), uiBaseMarker)

	m.targetInfoView.cursor++
	m.updateKey(keyPress("c"))
	require.Len(t, *opened, 1)
	assert.Contains(t, (*opened)[0], "inv-one")
	assert.Contains(t, (*opened)[0], "inv-two")
}

// i on a file listed under a step describes the file; esc comes back here
// rather than dropping into the tree.
func TestUITargetInfoOpensFileInfo(t *testing.T) {
	m, _ := targetInfoModel(t)
	m.targetInfoView.cursor = groupRow(t, m, groupInputs)
	m.updateKey(keyPress("l"))
	m.targetInfoView.cursor = pathRow(t, m, "cli/printlog/compact/compact.go")

	cmd := m.updateKey(keyPress("i"))
	require.NotNil(t, cmd)
	m.Update(cmd())
	assert.Equal(t, viewInfo, m.view)
	assert.Contains(t, infoText(m), "used as an input by")

	m.updateKey(keyPress("esc"))
	assert.Equal(t, viewTargetInfo, m.view)

	// On a file, i keeps meaning that file. It's the rows about the target that
	// take it back to the targets list.
	m.updateKey(keyPress("i"))
	assert.Equal(t, viewInfo, m.view)
	m.updateKey(keyPress("esc"))
	m.targetInfoView.cursor = groupRow(t, m, groupInputs)
	m.updateKey(keyPress("i"))
	assert.Equal(t, viewTargets, m.view)
}

// A step that last ran in an older build is read from that build's log, not
// from the target's newest one.
func TestUITargetInfoReadsEachStepsOwnLog(t *testing.T) {
	useTempLogCache(t)
	dir := t.TempDir()
	newest := filepath.Join(dir, "newest.binpb.zst")
	older := filepath.Join(dir, "older.binpb.zst")
	require.NoError(t, os.WriteFile(newest, graphLog(t,
		fileEntry(1, "compiled.go"),
		setEntry(10, []uint32{1}, nil),
		spawnEntry("//t:t", "GoCompilePkg", 10),
	), 0644))
	// The older build ran a step that the newest one didn't.
	require.NoError(t, os.WriteFile(older, graphLog(t,
		fileEntry(1, "compiled.go"),
		fileEntry(2, "linked.a"),
		setEntry(10, []uint32{1}, nil),
		setEntry(11, []uint32{2}, nil),
		spawnEntry("//t:t", "GoCompilePkg", 10),
		spawnEntry("//t:t", "GoLink", 11),
	), 0644))

	tr := newTree()
	require.NoError(t, tr.parse(newest))
	require.NoError(t, tr.parse(older))
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 40
	m.updateKey(keyPress("T"))
	m.Update(m.updateKey(keyPress("i"))())

	steps := map[string]*targetStep{}
	for _, step := range m.targetInfo.steps {
		steps[step.mnemonic] = step
	}
	require.Len(t, steps, 2)
	// The compile ran in both builds and is read from the newer one; the link
	// only ran in the older, and is read from there.
	assert.Equal(t, []string{"compiled.go"}, steps["GoCompilePkg"].inputs)
	assert.Equal(t, "newest.binpb.zst", m.t.logName(steps["GoCompilePkg"].logIdx))
	assert.Equal(t, []string{"linked.a"}, steps["GoLink"].inputs)
	assert.Equal(t, "older.binpb.zst", m.t.logName(steps["GoLink"].logIdx))

	text := targetInfoText(m)
	assert.Contains(t, text, "in newest.binpb.zst")
	assert.Contains(t, text, "in older.binpb.zst")
	// The builds a step ran in are listed above the files read from them.
	assert.Less(t, strings.Index(text, "invocations"), strings.Index(text, "inputs"))
}

// i and enter on a dependency or dependent open that target's own info view.
func TestUITargetInfoFollowsEdges(t *testing.T) {
	for _, key := range []string{"i", "enter"} {
		t.Run(key, func(t *testing.T) {
			m, step := targetInfoModel(t)
			m.targetInfoView.cursor = groupRow(t, m, groupDependencies)
			m.updateKey(keyPress("l"))
			m.targetInfoView.cursor++
			edge := m.current().group.edge
			require.NotEqual(t, step.target, edge.step.target)

			cmd := m.updateKey(keyPress(key))
			require.NotNil(t, cmd, "%s should start reading the log", key)
			m.Update(cmd())

			assert.Equal(t, viewTargetInfo, m.view)
			assert.Equal(t, edge.step.target, m.targetSubject.label)
			assert.Contains(t, targetInfoText(m), edge.step.target)
			// The targets list came along, so going back lands on the target
			// we're now looking at.
			m.updateKey(keyPress("esc"))
			require.Equal(t, viewTargets, m.view)
			assert.Equal(t, edge.step.target, m.current().target.label)
		})
	}
}

// An edge naming a target no merged log ran can't happen, but mustn't open an
// empty view if it does.
func TestUITargetInfoFollowsUnknownEdge(t *testing.T) {
	m, step := targetInfoModel(t)
	before := targetInfoText(m)

	assert.Nil(t, m.followEdge(&targetEdge{
		graphEdge: graphEdge{step: targetMnemonic{target: "//nothing:here"}},
	}))
	assert.Equal(t, step.target, m.targetSubject.label)
	assert.Equal(t, before, targetInfoText(m))
	assert.Contains(t, m.statusMsg, "//nothing:here")
}

func TestUITargetInfoEscapes(t *testing.T) {
	m, _ := targetInfoModel(t)
	m.updateKey(keyPress("esc"))
	assert.Equal(t, viewTargets, m.view)
	m.updateKey(keyPress("esc"))
	assert.Equal(t, viewTree, m.view)
}

// A result for a target the user has navigated away from is dropped.
func TestUITargetInfoIgnoresStaleResults(t *testing.T) {
	m, _ := targetInfoModel(t)
	before := targetInfoText(m)

	m.Update(targetDetailMsg{label: "//some:other", read: true})
	assert.Equal(t, before, targetInfoText(m))
}

func TestUITargetInfoReportsReadErrors(t *testing.T) {
	m, step := targetInfoModel(t)

	m.Update(targetDetailMsg{label: step.target, err: fmt.Errorf("boom")})
	text := targetInfoText(m)
	assert.Contains(t, text, "boom")
	// The steps are still listed: they come from the merge, not from the log we
	// failed to read. Their lists are unknown rather than empty.
	assert.Contains(t, text, step.mnemonic)
	assert.Contains(t, text, "unknown")
	assert.NotContains(t, text, "0 files")
}

// Each step names the build its files were read from, and a step whose log
// couldn't be read says so rather than looking empty.
func TestUITargetInfoNamesEachStepsLog(t *testing.T) {
	m, step := targetInfoModel(t)
	m.t.logs = append(m.t.logs, logInfo{name: "second.binpb.zst"})

	m.Update(targetDetailMsg{
		label:   step.target,
		read:    true,
		sources: map[string]int{step.mnemonic: 1},
		steps:   map[string]*stepFiles{step.mnemonic: {inputs: []string{"a.go"}}},
	})

	text := targetInfoText(m)
	assert.Contains(t, text, "1 file")
	assert.Contains(t, text, "in second.binpb.zst")
	// The target's other step wasn't in the message, so its files are unknown.
	assert.Contains(t, text, "unknown")
}

// Without a local copy of the log there's nothing to read.
func TestUITargetInfoWithoutTheLog(t *testing.T) {
	useTempLogCache(t)
	tr := newTree()
	tr.logs = []logInfo{{name: "gone.binpb.zst"}}
	tr.addStep(0, spawnStep{target: "//a:a", mnemonic: "GoLink"})
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 120, 30
	m.updateKey(keyPress("T"))

	assert.Nil(t, m.updateKey(keyPress("i")))
	assert.Equal(t, viewTargets, m.view)
	assert.Contains(t, m.statusMsg, "no local copy")
}

// Whatever the key, the target views mustn't panic or leave rows they can't
// render - their rows are a mix of prose, targets, files and builds.
func TestUITargetViewsSurviveEveryKey(t *testing.T) {
	watchBrowser(t)
	keys := []string{
		"enter", " ", "l", "h", "right", "left", "j", "k", "g", "G", "n", "N",
		"m", "c", "b", "x", "t", "T", "i", "/", "esc",
	}
	for _, key := range keys {
		t.Run("targets/"+key, func(t *testing.T) {
			m := targetsModel(t)
			m.updateKey(keyPress(key))
			m.View()
		})
		t.Run("info/"+key, func(t *testing.T) {
			m, _ := targetInfoModel(t)
			// With every list open, so the keys land on each kind of row.
			for _, step := range m.targetInfo.steps {
				for _, g := range step.groups {
					m.openGroups[g] = true
				}
			}
			m.rebuildTargetInfoRows()
			for _, cursor := range []int{0, len(m.targetInfoView.rows) / 2, len(m.targetInfoView.rows) - 1} {
				m.targetInfoView.cursor = cursor
				m.updateKey(keyPress(key))
				m.View()
			}
		})
	}
}

// configTree has one source and one generated file built in two
// configurations, one of which also changed between the two builds.
func configTree(t *testing.T) *tree {
	t.Helper()
	tr := newTree()
	tr.logs = []logInfo{{name: "one"}, {name: "two"}}
	tr.add("cli/cli.go", newDigest("source", 1))
	for _, logIdx := range []int{0, 1} {
		tr.add("bazel-out/k8-fastbuild/bin/cli/cli.a", fromLog(newDigest("fast", 2), logIdx))
	}
	tr.add("bazel-out/k8-opt/bin/cli/cli.a", newDigest("opt", 3))
	tr.add("bazel-out/k8-opt/bin/cli/cli.a", fromLog(newDigest("opt2", 4), 1))
	require.Empty(t, tr.conflicts)
	return tr
}

// p merges the configurations of a generated file into one row, and back.
func TestUIMergeConfigsTogglesTheTree(t *testing.T) {
	m := newTreeModel(configTree(t), printOptions{}, filterOptions{includeGenerated: true}, nil)
	m.width, m.height = 120, 40
	openAll := func() []string {
		for range 4 {
			for i := range m.treeView.rows {
				m.treeView.cursor = i
				m.expand()
			}
		}
		return paths(m)
	}

	// Each configuration is its own subtree to start with.
	assert.Contains(t, openAll(), "bazel-out/k8-opt/bin/cli/cli.a")
	assert.Contains(t, paths(m), "bazel-out/k8-fastbuild/bin/cli/cli.a")

	m.updateKey(keyPress("p"))
	assert.Contains(t, stripANSI(m.titleView()), "+merged configs")
	assert.Contains(t, m.statusMsg, "merging configurations")
	// The list starts over at the top, since the tree underneath it changed.
	assert.Zero(t, m.treeView.cursor)

	merged := openAll()
	assert.Contains(t, merged, "bazel-out/bin/cli/cli.a")
	assert.NotContains(t, merged, "bazel-out/k8-opt/bin/cli/cli.a")
	assert.Contains(t, merged, "cli/cli.go", "sources have no configuration to merge")

	// The merged row carries every version of both configurations.
	n := m.t.find("bazel-out/bin/cli/cli.a")
	require.NotNil(t, n)
	assert.Len(t, n.entry.versions, 3)
	// And it counts as modified, because one configuration changed between the
	// builds - not merely because the two configurations differ.
	assert.True(t, n.entry.modified())

	m.updateKey(keyPress("p"))
	assert.NotContains(t, stripANSI(m.titleView()), "+merged")
	assert.Contains(t, openAll(), "bazel-out/k8-opt/bin/cli/cli.a")
}

// A file built the same way in every build isn't modified just because its
// configurations differ.
func TestUIMergeConfigsDoesNotInventChanges(t *testing.T) {
	tr := newTree()
	tr.logs = []logInfo{{name: "one"}, {name: "two"}}
	for _, logIdx := range []int{0, 1} {
		tr.add("bazel-out/k8-opt/bin/a.a", fromLog(newDigest("opt", 1), logIdx))
		tr.add("bazel-out/k8-fastbuild/bin/a.a", fromLog(newDigest("fast", 2), logIdx))
	}
	m := newTreeModel(tr, printOptions{}, filterOptions{includeGenerated: true, mergeConfigs: true}, nil)
	m.width, m.height = 120, 40

	n := m.t.find("bazel-out/bin/a.a")
	require.NotNil(t, n)
	require.Len(t, n.entry.versions, 2)
	assert.False(t, n.entry.modified())
	assert.Zero(t, m.visibleModified)
	assert.NotContains(t, stripANSI(m.rowView(&m.treeView, 0)), modifiedMarker)
}

// The step file lists are paths out of a log, and merge the same way.
func TestUIMergeConfigsMergesStepFiles(t *testing.T) {
	m, _ := targetInfoModel(t)
	openInputs := func() []string {
		m.targetInfoView.cursor = groupRow(t, m, groupInputs)
		m.updateKey(keyPress("l"))
		var listed []string
		for _, row := range m.targetInfoView.rows {
			if row.depth == 2 && strings.HasPrefix(row.path, generatedRoot+"/") {
				listed = append(listed, row.path)
			}
		}
		return listed
	}

	raw := openInputs()
	require.NotEmpty(t, raw, "the step should take some generated inputs")

	m.updateKey(keyPress("p"))
	shown := openInputs()

	// Every listed path is one of the raw ones with its configuration taken
	// out, and the ones that collide are listed once.
	want := map[string]bool{}
	for _, p := range raw {
		assert.Regexp(t, `^bazel-out/[^/]+/(bin|genfiles|testlogs)/`, p)
		want[stripConfig(p)] = true
	}
	assert.Len(t, shown, len(want))
	for _, p := range shown {
		assert.Contains(t, want, p)
		require.NotNil(t, m.t.find(p), "%s should resolve in the merged tree", p)
	}

	// The count on the summary row agrees with the rows under it.
	files := 0
	for _, row := range m.targetInfoView.rows {
		if row.depth == 2 {
			files++
		}
	}
	assert.Contains(t, stripANSI(m.targetInfoView.rows[groupRow(t, m, groupInputs)].text),
		plural(files, "file"))
}

// With configurations merged, one row stands for several real files, and the
// file info view has to ask the logs about all of them.
func TestUIMergeConfigsAsksAboutEveryConfiguration(t *testing.T) {
	useTempLogCache(t)
	dir := t.TempDir()
	logPath := filepath.Join(dir, "build.binpb.zst")
	require.NoError(t, os.WriteFile(logPath, graphLog(t,
		fileEntry(1, "bazel-out/k8-opt/bin/a.a"),
		fileEntry(2, "bazel-out/k8-fastbuild/bin/a.a"),
		setEntry(10, []uint32{1}, nil),
		setEntry(11, []uint32{2}, nil),
		spawnEntry("//opt:opt", "GoLink", 10),
		spawnEntry("//fast:fast", "GoLink", 11),
	), 0644))

	tr := newTree()
	require.NoError(t, tr.parse(logPath))
	m := newTreeModel(tr, printOptions{}, filterOptions{includeGenerated: true, mergeConfigs: true}, nil)
	m.width, m.height = 120, 40

	// One row for both files, standing for both real paths.
	require.Equal(t, []string{"bazel-out/bin/a.a"}, []string{m.files[0].path})
	assert.ElementsMatch(t,
		[]string{"bazel-out/k8-opt/bin/a.a", "bazel-out/k8-fastbuild/bin/a.a"},
		m.realPaths("bazel-out/bin/a.a"))

	m.reveal(&m.files[0])
	m.Update(m.updateKey(keyPress("i"))())

	text := infoText(m)
	assert.Contains(t, text, "2 configurations")
	// Both configurations' consumers, from the one row.
	m.infoView.cursor = groupRow(t, m, groupConsumers, &m.infoView)
	m.updateKey(keyPress("l"))
	text = infoText(m)
	assert.Contains(t, text, "//opt:opt  GoLink")
	assert.Contains(t, text, "//fast:fast  GoLink")
}

// A query shouldn't spill every matching file's versions into the tree.
func TestUIQueryDoesNotExpandFiles(t *testing.T) {
	m := newTreeModel(uiVersionTree(t), printOptions{}, filterOptions{}, nil)

	m.query = "metacache"
	m.applyQuery()

	assert.Equal(t, []string{"a", "a/metacache.go"}, paths(m))
}

// topPaths returns "<count> <path>" for each row of the most-changed view.
func topPaths(m *treeModel) []string {
	out := make([]string, 0, len(m.topView.rows))
	for _, r := range m.topView.rows {
		out = append(out, fmt.Sprintf("%d %s", len(r.node.entry.versions), r.path))
	}
	return out
}

func TestUITopViewOrdersByDigestCount(t *testing.T) {
	tr := newTree()
	tr.logs = []logInfo{{name: "one"}, {name: "two"}, {name: "three"}}
	// Three distinct digests for churn.go, two for sometimes.go, one for the
	// rest.
	tr.add("a/churn.go", newDigest("a1", 1))
	tr.add("a/churn.go", fromLog(newDigest("a2", 1), 1))
	tr.add("a/churn.go", fromLog(newDigest("a3", 1), 2))
	tr.add("b/sometimes.go", newDigest("b1", 1))
	tr.add("b/sometimes.go", fromLog(newDigest("b2", 1), 1))
	tr.add("a/stable.go", newDigest("c1", 1))
	tr.add("bazel-out/gen.go", newDigest("d1", 1))
	tr.add("bazel-out/gen.go", fromLog(newDigest("d2", 1), 1))

	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)

	// Most digests first, ties broken by path. Generated files are hidden by
	// default, however much they changed.
	assert.Equal(t, []string{
		"3 a/churn.go",
		"2 b/sometimes.go",
		"1 a/stable.go",
	}, topPaths(m))
}

func TestUITopViewToggleAndRender(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20

	m.updateKey(keyPress("t"))
	assert.Equal(t, viewTop, m.view)

	lines := strings.Split(m.View().Content, "\n")
	assert.Len(t, lines, m.height)
	// The full path and the digest count are both on the row.
	assert.Contains(t, stripANSI(lines[1]), "2  server/util/rexec/rexec.go")
	assert.Contains(t, stripANSI(lines[0]), "most changed")

	// The detail pane still describes the selected file.
	assert.Contains(t, m.View().Content, "second.binpb.zst")

	m.updateKey(keyPress("t"))
	assert.Equal(t, viewTree, m.view)
}

// Enter on a row of the most-changed view goes to that file in the tree,
// expanding whatever was hiding it.
func TestUITopViewEnterRevealsInTree(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)
	m.view = viewTop
	m.topView.cursor = 0
	require.Equal(t, "server/util/rexec/rexec.go", m.topView.rows[0].path)

	m.updateKey(keyPress("enter"))

	assert.Equal(t, viewTree, m.view)
	require.NotNil(t, m.current())
	assert.Equal(t, "server/util/rexec/rexec.go", m.current().path)
	assert.Empty(t, m.statusMsg)
}

func TestUITopViewRespectsFilter(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)

	m.query = "rexec_test"
	m.applyQuery()
	assert.Equal(t, []string{"1 server/util/rexec/rexec_test.go"}, topPaths(m))

	m.query = ""
	m.applyQuery()
	assert.Len(t, topPaths(m), 4)
}

func TestUITopViewScrollsIndependently(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20

	m.updateKey(keyPress("j"))
	assert.Equal(t, 1, m.treeView.cursor)
	assert.Equal(t, 0, m.topView.cursor)

	m.updateKey(keyPress("t"))
	m.updateKey(keyPress("G"))
	assert.Equal(t, len(m.topView.rows)-1, m.topView.cursor)
	// Moving in one view leaves the other where it was.
	assert.Equal(t, 1, m.treeView.cursor)
}

// Generated and external files are hidden by default and can be toggled on
// without rebuilding the tree.
func TestUIToggleGeneratedAndExternal(t *testing.T) {
	tr := uiTestTree(t)
	tr.add("external/repo/dep.go", newDigest("1111", 7))
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)

	assert.NotContains(t, paths(m), "bazel-out")
	assert.NotContains(t, paths(m), "external")
	assert.Equal(t, 4, m.visibleFiles)

	m.updateKey(keyPress("b"))
	assert.Contains(t, paths(m), "bazel-out")
	assert.NotContains(t, paths(m), "external")
	assert.Equal(t, 5, m.visibleFiles)
	assert.Equal(t, "showing generated files", m.statusMsg)

	m.updateKey(keyPress("x"))
	assert.Contains(t, paths(m), "bazel-out")
	assert.Contains(t, paths(m), "external")
	assert.Equal(t, 6, m.visibleFiles)

	// And back off again.
	m.updateKey(keyPress("b"))
	m.updateKey(keyPress("x"))
	assert.NotContains(t, paths(m), "bazel-out")
	assert.NotContains(t, paths(m), "external")
	assert.Equal(t, 4, m.visibleFiles)
	assert.Equal(t, "hiding external repo sources", m.statusMsg)
}

// The toggles have to reach the flat views and the counts too, not just the
// tree rows.
func TestUIToggleUpdatesEveryView(t *testing.T) {
	tr := uiTestTree(t)
	tr.add("bazel-out/k8-fastbuild/bin/gen.go", fromLog(newDigest("9999", 5), 1))
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)

	assert.Equal(t, 1, m.visibleModified)
	assert.Len(t, m.modified, 1)
	assert.NotContains(t, strings.Join(topPaths(m), "\n"), "bazel-out")

	m.updateKey(keyPress("b"))

	// The generated file was modified too, so it shows up everywhere at once.
	assert.Equal(t, 2, m.visibleModified)
	assert.Len(t, m.modified, 2)
	assert.Contains(t, topPaths(m), "2 bazel-out/k8-fastbuild/bin/gen.go")
	assert.Equal(t, 1, m.t.root.children["bazel-out"].modifiedCount)
}

// A directory hash covers what's on screen, so toggling hidden content changes
// the hashes above it.
func TestUIToggleChangesDirHashes(t *testing.T) {
	tr := newTree()
	tr.add("a/b.go", newDigest("aaaa", 1))
	tr.add("a/bazel-out/gen.go", newDigest("bbbb", 2))
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)

	a := tr.root.children["a"]
	sourceOnly := m.dirHash(a)
	// Hiding the generated file means the hash covers only b.go.
	assert.Equal(t, hashChildren([]*node{a.children["b.go"]}, (*node).hash), sourceOnly)

	m.updateKey(keyPress("b"))
	assert.NotEqual(t, sourceOnly, m.dirHash(a))
	// With everything showing, it matches the plain whole-tree hash.
	assert.Equal(t, a.hash(), m.dirHash(a))

	m.updateKey(keyPress("b"))
	assert.Equal(t, sourceOnly, m.dirHash(a))
}

// Toggling shifts whole subtrees in and out, so it starts over at the top
// rather than leaving the cursor mid-list.
func TestUIToggleScrollsToTop(t *testing.T) {
	m := newTreeModel(uiTestTree(t), printOptions{}, filterOptions{}, nil)
	m.width, m.height = 100, 20

	m.jumpToModified(1)
	require.Equal(t, "server/util/rexec/rexec.go", m.current().path)
	require.NotZero(t, m.treeView.cursor)
	// Scroll the most-changed list too, so both are known to be off the top.
	m.topView.cursor, m.topView.offset = 2, 1

	m.updateKey(keyPress("b"))

	assert.Zero(t, m.treeView.cursor)
	assert.Zero(t, m.treeView.offset)
	assert.Zero(t, m.topView.cursor)
	assert.Zero(t, m.topView.offset)
	// The first row is the top of the tree, which now includes what was hidden.
	require.NotNil(t, m.current())
	assert.Equal(t, "bazel-out", m.current().path)
}

func TestUIEmptyTree(t *testing.T) {
	tr := newTree()
	m := newTreeModel(tr, printOptions{}, filterOptions{}, nil)
	m.width, m.height = 80, 24

	assert.Empty(t, m.treeView.rows)
	assert.Nil(t, m.current())
	// Rendering an empty tree, moving around in it, and collapsing nothing must
	// not panic.
	m.treeView.move(1, m.rowsHeight())
	m.collapse()
	m.expand()
	assert.Len(t, strings.Split(m.View().Content, "\n"), m.height)
}
