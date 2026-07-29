package main

import (
	"context"
	"fmt"
	"net/url"
	"os/exec"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// uiHashLength is the hash length used in the UI when --hash_length wasn't set.
// Full digests are too wide to browse.
const uiHashLength = 12

// uiDetailLines is how many lines of the selected node's detail are shown under
// the tree, not counting the separator.
const uiDetailLines = 5

// uiGutter and uiCursorGutter are the leading columns of every row. They must be
// the same width so that the tree lines up. The marker is deliberately
// unstyled: it has to show up in terminals that don't render the highlight.
const (
	uiGutter       = "  "
	uiCursorGutter = "o "
)

// uiTopIndent lines a file's versions up under its path in the most-changed
// view, where there's a count column instead of tree drawing.
const uiTopIndent = "      "

// The target info view isn't a tree, so its rows are indented by hand: each
// step's summary rows sit under the step, and what they open into sits under
// them.
const (
	uiGroupIndent = "  "
	uiListIndent  = "      "
)

// uiBaseMarker labels the version armed as the left side of a comparison.
const uiBaseMarker = "(base)"

var (
	uiTitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#ffffff")).
			Background(lipgloss.Color("#5533cc")).
			Padding(0, 1)
	uiDirStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("#00cccc"))
	uiHashStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("#666666"))
	uiModifiedStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffcc00")).Bold(true)
	uiCursorStyle   = lipgloss.NewStyle().Background(lipgloss.Color("#1a1a2e"))
	uiLabelStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("#666666"))
	uiHelpStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("#666666"))
	uiFilterStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#00cccc"))
)

// uiRow is one line of a view: a node, plus where it sits.
type uiRow struct {
	node *node
	// path is the node's full path, used by the detail pane.
	path string
	// prefix is the tree drawing to the left of the name, e.g. "│   ├── ".
	// Empty in the flat views.
	prefix string
	depth  int
	// ancestors are the directories that have to be expanded to reveal this
	// node in the tree view. Only set for rows built by the flat walk.
	ancestors []*node
	// version is set on the rows under an expanded file, one per distinct set
	// of contents the file was seen with.
	version *version
	// target is set on the targets view's rows, which stand for a build target
	// rather than a file.
	target *targetSteps
	// group is set on the target info view's summary rows, which open into a
	// list rather than into a node's contents.
	group *infoGroup
	// invocation is set on the rows under an expanded list of builds.
	invocation *logInfo
	// text is a pre-rendered line, used by views that show prose rather than
	// nodes. Rows with text set have no node.
	text string
	// fileIndex is the row's position among all visible files, in tree order.
	// Only set for rows built by the flat walk, where it's what "the next
	// modified file after this one" is measured against.
	fileIndex int
}

// compareBase is the file version picked as the left side of a comparison. It
// holds the version rather than the row, because rows are rebuilt constantly
// while the versions in the tree are stable.
type compareBase struct {
	path    string
	version *version
}

// blobState is what we know about whether a version's contents are still in the
// cache. Blobs are evicted over time, so an old version's digest is often no
// longer fetchable even though we know what it was.
type blobState int

const (
	blobUnknown blobState = iota
	blobChecking
	blobCached
	blobEvicted
)

// blobStatusMsg carries the result of a cache lookup back into the UI.
type blobStatusMsg struct {
	state map[*version]blobState
	err   error
}

// blobChecker looks digests up in the CAS. apiClient implements it; it's nil
// when the logs came from files, since then there's no server to ask.
type blobChecker interface {
	missingBlobs(ctx context.Context, instanceName string, digestFunction repb.DigestFunction_Value, digests []*repb.Digest) ([]*repb.Digest, error)
}

// viewport is a scrollable list of rows.
type viewport struct {
	rows   []uiRow
	cursor int
	offset int
}

func (v *viewport) current() *uiRow {
	if v.cursor < 0 || v.cursor >= len(v.rows) {
		return nil
	}
	return &v.rows[v.cursor]
}

func (v *viewport) move(delta, height int) {
	v.cursor += delta
	v.clamp(height)
}

// scrollToMidpoint scrolls so the selected row sits no lower than the middle of
// the viewport, which is what makes room for whatever it just expanded. Rows
// already above the midpoint are left where they are.
func (v *viewport) scrollToMidpoint(height int) {
	if mid := height / 2; v.cursor-v.offset > mid {
		v.offset = v.cursor - mid
	}
	v.clamp(height)
}

func (v *viewport) clamp(height int) {
	if len(v.rows) == 0 {
		v.cursor, v.offset = 0, 0
		return
	}
	v.cursor = min(max(v.cursor, 0), len(v.rows)-1)
	maxOffset := max(len(v.rows)-height, 0)
	v.offset = min(max(v.offset, 0), maxOffset)
	if v.cursor < v.offset {
		v.offset = v.cursor
	}
	if v.cursor >= v.offset+height {
		v.offset = v.cursor - height + 1
	}
}

// viewKind is which of the screens is showing.
type viewKind int

const (
	viewTree viewKind = iota
	viewTop
	viewInfo
	viewTargets
	viewTargetInfo
)

// fileInfoMsg carries a file's summary back into the UI once every build that
// mentions it has been read.
type fileInfoMsg struct {
	path    string
	summary *fileSummary
}

// fileDetail is what the file info view shows: the steps that produced the file
// and the steps that consumed it, pooled across builds.
type fileDetail struct {
	path    string
	summary *fileSummary
	// missing is how many of the builds that mention the file we have no local
	// copy of, and so couldn't ask. configs is how many real files the row
	// stands for, which is more than one when configurations are merged.
	missing int
	configs int
	// groups holds the two rows the lists are shown under.
	groups []*infoGroup
	// status replaces them while the logs are being read.
	status string
}

// targetDetailMsg carries a target's files back into the UI once its logs have
// been read.
type targetDetailMsg struct {
	label string
	// logIdx is the target's newest build, which is what the edges below were
	// read from, and graph is its graph, kept for the next target.
	logIdx int
	graph  *spawnGraph
	// read says whether that log could be read at all.
	read bool
	// steps is what each of the target's steps touched, and sources which log
	// each answer came from. A step whose log couldn't be read is in sources but
	// not in steps.
	steps   map[string]*stepFiles
	sources map[string]int
	// dependencies and dependents are the steps on the far end of the target's
	// files.
	dependencies []graphEdge
	dependents   []graphEdge
	err          error
}

// targetDetail is what the target info view shows: the steps a target ran, and
// for each of them, the files it touched and the builds it ran in.
type targetDetail struct {
	label string
	// logIdx is the log the file lists were read from. Every build that ran the
	// target has its own idea of what it consumed, and reading all of them would
	// mean reading every log, so this is one build's answer.
	logIdx int
	// read says whether we managed to read that log at all. Until we have, the
	// lists below are empty because we don't know, not because there's nothing
	// in them.
	read  bool
	steps []*targetStep
	// dependencies are the steps that produced what this target consumed, and
	// dependents the steps that consumed what it produced. Unlike the steps,
	// these are about the target as a whole. groups holds the two rows they're
	// shown under.
	dependencies []*targetEdge
	dependents   []*targetEdge
	groups       []*infoGroup
	// status replaces the lists while we're still reading the log, or when we
	// couldn't.
	status string
}

// targetEdge is one step on the far end of a target's inputs or outputs, as the
// view shows it.
type targetEdge struct {
	graphEdge
	group *infoGroup
}

// targetStep is one mnemonic of a target: what it touched, and where it ran.
type targetStep struct {
	mnemonic string
	inputs   []string
	outputs  []string
	// logs are the logs that ran this step, newest build first, and logIdx is
	// the one its files were read from: the first of them.
	logs   []int
	logIdx int
	// platforms is how those builds split across os and arch.
	platforms []platformTally
	// read is false when that log couldn't be read, in which case the file
	// lists are unknown rather than empty.
	read   bool
	groups []*infoGroup
}

// groupKind is one of the lists the target info view opens up into.
type groupKind int

const (
	// The lists under a build step.
	groupInputs groupKind = iota
	groupOutputs
	groupInvocations
	// The lists about the target as a whole, and one entry within them.
	groupDependencies
	groupDependents
	groupEdge
	// The lists about a file.
	groupProducers
	groupConsumers
)

// infoGroup is one of the target info view's expandable rows. Rows are rebuilt
// constantly, so which of them are open is tracked by these, which aren't.
type infoGroup struct {
	kind groupKind
	// step is set on the rows under a build step, and edge on the rows under a
	// dependency or dependent.
	step *targetStep
	edge *targetEdge
}

// files is the list a group of files opens into.
func (g *infoGroup) files() []string {
	switch g.kind {
	case groupInputs:
		return g.step.inputs
	case groupOutputs:
		return g.step.outputs
	case groupEdge:
		return g.edge.files
	default:
		// The other kinds open into builds, or into other steps.
		return nil
	}
}

type treeModel struct {
	// t is the tree being shown: full, or the copy with configurations merged.
	// Node identity is what most of the state below is keyed on, so switching
	// between them means starting that state over.
	t        *tree
	full     *tree
	stripped *tree
	opts     printOptions

	view     viewKind
	treeView viewport
	// topView lists every file by how many distinct digests it was seen with,
	// most-changed first.
	topView viewport
	// infoView describes one file: what built with it, and every version of it
	// we saw. Its rows are plain text rather than nodes.
	infoView viewport
	// infoSubject is the row the info view was opened from, fileInfo what we
	// found out about the file it describes, and infoReturn the view it was
	// opened from, which is where leaving it goes back to.
	infoSubject *uiRow
	fileInfo    *fileDetail
	infoReturn  viewKind
	// targets is every target the logs ran, most builds first, and targetsView
	// lists the ones the query allows.
	targets     []*targetSteps
	targetsView viewport
	// targetInfoView describes one target, targetSubject is the target it was
	// opened for, and targetInfo is what we found out about it. openGroups holds
	// the summary rows the user has opened.
	targetInfoView viewport
	targetSubject  *targetSteps
	targetInfo     *targetDetail
	openGroups     map[*infoGroup]bool
	// targetReturn is the view the target info was opened from, which is where
	// leaving it goes back to.
	targetReturn viewKind
	// graph and graphLog memoize the last log read for an info view, since
	// reading one is expensive and neighbouring files usually share a build.
	graph    *spawnGraph
	graphLog int

	// show is which parts of the tree are visible. The tree itself is never
	// pruned, so generated and external files can be toggled on and off without
	// rebuilding it.
	show filterOptions
	// hidden holds the nodes that `show` is currently filtering out. Only the
	// top of each hidden subtree is recorded: nothing below one is ever asked
	// about.
	hidden map[*node]bool
	// hashes memoizes directory hashes over the visible children, so they agree
	// with what's on screen.
	hashes map[*node]string

	// expanded holds the directories the user has opened.
	expanded map[*node]bool
	// matched holds the nodes on a path matching the current query. It is empty
	// when no query is set.
	matched map[*node]bool
	query   string
	// typing is true while the query is being typed.
	typing bool

	// files is every visible file, in tree order, and modified is the subset
	// that was seen with more than one set of contents. fileIndex locates a
	// file in that order.
	files     []uiRow
	modified  []uiRow
	fileIndex map[*node]int

	// base is the file version picked as the left side of a comparison, if
	// any.
	base *compareBase

	// checker looks blobs up in the CAS, and blobs records what it said about
	// each version we've asked about.
	checker blobChecker
	blobs   map[*version]blobState

	// visibleFiles and visibleModified summarize what's on screen.
	visibleFiles    int
	visibleModified int

	width     int
	height    int
	statusMsg string
}

func newTreeModel(t *tree, opts printOptions, show filterOptions, checker blobChecker) *treeModel {
	if opts.hashLength == 0 {
		opts.hashLength = uiHashLength
	}
	m := &treeModel{
		t:          t,
		full:       t,
		opts:       opts,
		show:       show,
		checker:    checker,
		targets:    t.sortedTargets(),
		blobs:      map[*version]blobState{},
		hidden:     map[*node]bool{},
		hashes:     map[*node]string{},
		expanded:   map[*node]bool{},
		matched:    map[*node]bool{},
		openGroups: map[*infoGroup]bool{},
		// Sensible defaults until the first WindowSizeMsg arrives.
		width:  80,
		height: 24,
	}
	if show.mergeConfigs {
		m.stripped = stripConfigs(t)
		m.t = m.stripped
	}
	m.refresh()
	return m
}

// refresh recomputes everything that depends on what's visible: which nodes are
// filtered out, the per-directory counts, the memoized hashes, and every row
// set. It runs on startup and whenever the query or the show options change.
func (m *treeModel) refresh() {
	m.recomputeHidden()
	clear(m.hashes)
	m.recount()
	m.rebuildRows()
	m.files = m.collectFiles()
	m.modified = nil
	m.fileIndex = make(map[*node]int, len(m.files))
	for i := range m.files {
		m.files[i].fileIndex = i
		m.fileIndex[m.files[i].node] = i
		if m.files[i].node.entry.modified() {
			m.modified = append(m.modified, m.files[i])
		}
	}
	m.rebuildTopRows()
	m.rebuildTargetRows()
	m.rebuildTargetInfoRows()
}

// recomputeHidden records the subtrees that the current show options prune.
// This mirrors what filter does to the tree for the text output, without
// touching the tree.
func (m *treeModel) recomputeHidden() {
	clear(m.hidden)
	var walk func(n *node, depth int)
	walk = func(n *node, depth int) {
		for _, c := range n.children {
			if shouldPrune(c.name, c, m.show, depth) {
				m.hidden[c] = true
				continue
			}
			if c.isDir() {
				walk(c, depth+1)
			}
		}
	}
	walk(m.t.root, 0)
}

// recount recomputes each directory's file and modified counts over the files
// that are currently visible, so a collapsed directory reports what it would
// show if opened.
func (m *treeModel) recount() {
	var walk func(n *node) (int, int)
	walk = func(n *node) (int, int) {
		if !n.isDir() {
			n.fileCount = 1
			n.modifiedCount = 0
			if n.entry.modified() {
				n.modifiedCount = 1
			}
			return n.fileCount, n.modifiedCount
		}
		files, modified := 0, 0
		for _, c := range m.visibleChildren(n) {
			cf, cm := walk(c)
			files += cf
			modified += cm
		}
		n.fileCount, n.modifiedCount = files, modified
		return files, modified
	}
	m.visibleFiles, m.visibleModified = walk(m.t.root)
}

// dirHash is the hash of a directory's visible contents. It differs from
// node.hash when something is filtered out, which is the point: the hash should
// describe what's on screen.
func (m *treeModel) dirHash(n *node) string {
	if !n.isDir() {
		return n.entry.hash()
	}
	if h, ok := m.hashes[n]; ok {
		return h
	}
	h := hashChildren(m.visibleChildren(n), m.dirHash)
	m.hashes[n] = h
	return h
}

// collectFiles walks the visible tree and returns a row per file, in path
// order, each carrying the directories that hide it in the tree view.
func (m *treeModel) collectFiles() []uiRow {
	var rows []uiRow
	var walk func(n *node, prefix string, ancestors []*node)
	walk = func(n *node, prefix string, ancestors []*node) {
		for _, c := range m.visibleChildren(n) {
			p := prefix + c.name
			if c.isDir() {
				walk(c, p+"/", append(ancestors, c))
				continue
			}
			rows = append(rows, uiRow{node: c, path: p, ancestors: slices.Clone(ancestors)})
		}
	}
	walk(m.t.root, "", nil)
	return rows
}

// rebuildTopRows orders the files by how many distinct digests each was seen
// with, so the ones that changed most are at the top. Expanded files list their
// versions underneath, the same as in the tree.
func (m *treeModel) rebuildTopRows() {
	files := slices.Clone(m.files)
	slices.SortStableFunc(files, func(a, b uiRow) int {
		if n := len(b.node.entry.versions) - len(a.node.entry.versions); n != 0 {
			return n
		}
		return strings.Compare(a.path, b.path)
	})
	m.topView.rows = m.topView.rows[:0]
	for _, f := range files {
		m.topView.rows = append(m.topView.rows, f)
		if m.isExpanded(f.node) {
			m.topView.rows = append(m.topView.rows, m.versionRows(f.node, uiTopIndent, f.path, 1)...)
		}
	}
	m.topView.clamp(m.rowsHeight())
}

// rebuildTargetRows lists the targets the logs ran, most-built first, filtered
// by the query the same way the tree is.
func (m *treeModel) rebuildTargetRows() {
	m.targetsView.rows = m.targetsView.rows[:0]
	needle := strings.ToLower(m.query)
	for _, ts := range m.targets {
		if needle != "" && !strings.Contains(strings.ToLower(ts.label), needle) {
			continue
		}
		m.targetsView.rows = append(m.targetsView.rows, uiRow{target: ts, path: ts.label})
	}
	m.targetsView.clamp(m.rowsHeight())
}

// rebuildTargetInfoRows lays out the target info view: a block per step, each
// with summary rows that open into the files it touched and the builds it ran
// in.
func (m *treeModel) rebuildTargetInfoRows() {
	rows := m.targetInfoView.rows[:0]
	d := m.targetInfo
	if d == nil {
		m.targetInfoView.rows = rows
		return
	}
	rows = append(rows, uiRow{text: d.label}, uiRow{text: ""})
	if d.status != "" {
		rows = append(rows, uiRow{text: d.status}, uiRow{text: ""})
	}
	// What's on the far end of this target's files comes first: it's about the
	// target rather than about any one of its steps.
	for _, g := range d.groups {
		rows = append(rows, uiRow{text: m.groupText(g), group: g})
		if m.openGroups[g] {
			rows = append(rows, m.edgeRows(m.edges(g))...)
		}
	}
	rows = append(rows, uiRow{text: ""})
	for _, step := range d.steps {
		rows = append(rows, uiRow{text: m.stepText(step)})
		for _, g := range step.groups {
			rows = append(rows, uiRow{text: m.groupText(g), depth: 1, group: g})
			if m.openGroups[g] {
				rows = append(rows, m.groupRows(g)...)
			}
		}
		rows = append(rows, uiRow{text: ""})
	}
	m.targetInfoView.rows = rows
	m.targetInfoView.clamp(m.rowsHeight())
}

// edges is the list a dependencies or dependents row opens into.
func (m *treeModel) edges(g *infoGroup) []*targetEdge {
	if g.kind == groupDependents {
		return m.targetInfo.dependents
	}
	return m.targetInfo.dependencies
}

// edgeRows lists the steps on the far end of a target's files, each opening
// into the files that connect them.
func (m *treeModel) edgeRows(edges []*targetEdge) []uiRow {
	var rows []uiRow
	for _, edge := range edges {
		rows = append(rows, uiRow{text: m.groupText(edge.group), depth: 1, group: edge.group})
		if m.openGroups[edge.group] {
			rows = append(rows, m.fileRows(m.displayPaths(edge.files))...)
		}
	}
	return rows
}

// stepText heads a step's block with the mnemonic and how widely it ran.
func (m *treeModel) stepText(step *targetStep) string {
	ran := fmt.Sprintf("  ran in %s", plural(len(step.logs), "build"))
	if breakdown := platformBreakdown(step.platforms); breakdown != "" {
		ran += " " + breakdown
	}
	return step.mnemonic + uiLabelStyle.Render(ran)
}

// platformBreakdown says how the builds split across os and arch, e.g.
// "(linux_amd64: 17, darwin_arm64: 26)". Builds whose spawns didn't say where
// they ran aren't counted, so this needn't add up to the total, and a build that
// ran the step on more than one platform counts under each.
func platformBreakdown(tallies []platformTally) string {
	if len(tallies) == 0 {
		return ""
	}
	parts := make([]string, 0, len(tallies))
	for _, t := range tallies {
		parts = append(parts, fmt.Sprintf("%s: %d", t.platform, t.builds))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// groupText is a summary row: what's in the list under it, and whether it's
// open.
func (m *treeModel) groupText(g *infoGroup) string {
	marker := "▸ "
	if m.openGroups[g] {
		marker = "▾ "
	}
	switch g.kind {
	case groupDependencies:
		return marker + m.edgeGroupText("dependencies", m.targetInfo.dependencies)
	case groupDependents:
		return marker + m.edgeGroupText("dependents", m.targetInfo.dependents)
	case groupEdge:
		return uiGroupIndent + marker + g.edge.step.String() +
			uiLabelStyle.Render("  "+plural(len(m.displayPaths(g.edge.files)), "file"))
	case groupInvocations:
		return uiGroupIndent + marker +
			fmt.Sprintf("%-12s %s", "invocations", plural(len(g.step.logs), "build"))
	case groupInputs:
		return uiGroupIndent + marker + m.fileGroupText("inputs", g)
	case groupOutputs:
		return uiGroupIndent + marker + m.fileGroupText("outputs", g)
	case groupProducers:
		return marker + "output by " + stepSummary(m.fileGroupSteps(g))
	case groupConsumers:
		return marker + "used as an input by " + stepSummary(m.fileGroupSteps(g))
	}
	return ""
}

// stepSummary counts the targets a list of steps belongs to, and the steps
// themselves when a target contributes more than one.
func stepSummary(steps []targetMnemonic) string {
	targets := map[string]bool{}
	for _, s := range steps {
		targets[s.target] = true
	}
	summary := plural(len(targets), "target")
	if len(targets) != len(steps) {
		summary += fmt.Sprintf(" · %s", plural(len(steps), "step"))
	}
	return summary
}

// edgeGroupText counts what's on the far end of the target's files.
func (m *treeModel) edgeGroupText(name string, edges []*targetEdge) string {
	if !m.targetInfo.read {
		return fmt.Sprintf("%-13s %s", name, uiLabelStyle.Render("unknown"))
	}
	return fmt.Sprintf("%-13s %s%s", name, edgeSummary(edges), m.inLogText(m.targetInfo.logIdx))
}

// fileGroupText counts the files in one of a step's lists, and names the build
// they were read from: the step's newest, which needn't be the target's.
func (m *treeModel) fileGroupText(name string, g *infoGroup) string {
	if !g.step.read {
		return fmt.Sprintf("%-12s %s", name, uiLabelStyle.Render("unknown"))
	}
	return fmt.Sprintf("%-12s %s%s", name, plural(len(m.displayPaths(g.files())), "file"),
		m.inLogText(g.step.logIdx))
}

// inLogText names the log a list was read from, which is one of possibly many
// builds that ran the step.
func (m *treeModel) inLogText(logIdx int) string {
	return uiLabelStyle.Render("  in " + m.t.logName(logIdx))
}

// edgeSummary counts the targets on the far end of a list of edges, and the
// steps of theirs involved when those aren't the same number.
func edgeSummary(edges []*targetEdge) string {
	steps := make([]targetMnemonic, 0, len(edges))
	for _, edge := range edges {
		steps = append(steps, edge.step)
	}
	return stepSummary(steps)
}

// groupRows is what an open summary row shows underneath it.
func (m *treeModel) groupRows(g *infoGroup) []uiRow {
	if g.kind == groupInvocations {
		return m.invocationRows(g.step.logs)
	}
	return m.fileRows(m.displayPaths(g.files()))
}

// fileRows lists the files a step touched. A path we have a node for behaves
// like a file in the tree: it opens up into the versions the logs saw. A
// directory, or a path no log named a file at, is just a line.
func (m *treeModel) fileRows(paths []string) []uiRow {
	rows := make([]uiRow, 0, len(paths))
	for i, p := range paths {
		branch, cont := branchFor(i, len(paths))
		n := m.t.find(p)
		if n == nil || n.isDir() {
			suffix := ""
			if n != nil {
				suffix = "/"
			}
			rows = append(rows, uiRow{text: uiHashStyle.Render(uiListIndent+branch) + p + suffix, depth: 2})
			continue
		}
		rows = append(rows, uiRow{node: n, path: p, prefix: uiListIndent + branch, depth: 2})
		if m.isExpanded(n) {
			rows = append(rows, m.versionRows(n, uiListIndent+cont, p, 3)...)
		}
	}
	return rows
}

// invocationRows lists the builds that ran a step, newest first.
func (m *treeModel) invocationRows(logs []int) []uiRow {
	// Only logs fetched from a server know when their build ran. If none of
	// these do, there's no column to line up.
	timed := false
	for _, logIdx := range logs {
		if m.t.log(logIdx).updatedAtUsec != 0 {
			timed = true
			break
		}
	}
	rows := make([]uiRow, 0, len(logs))
	for i, logIdx := range logs {
		branch, _ := branchFor(i, len(logs))
		src := m.t.log(logIdx)
		line := uiHashStyle.Render(uiListIndent + branch)
		if timed {
			when := src.when()
			if when == "" {
				when = strings.Repeat(" ", len(time.DateTime))
			}
			line += uiLabelStyle.Render(when) + "  "
		}
		line += src.name
		if src.branch != "" {
			line += uiLabelStyle.Render("  " + src.branch)
		}
		rows = append(rows, uiRow{text: line, depth: 2, invocation: &src})
	}
	return rows
}

// plural renders a count with its unit, e.g. "1 file" or "3 files".
func plural(n int, unit string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, unit)
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

// branchFor draws the tree connector for item i of n, along with the prefix
// that lines its own children up underneath it.
func branchFor(i, n int) (string, string) {
	if i == n-1 {
		return "└── ", "    "
	}
	return "├── ", "│   "
}

// rebuildRows flattens the expanded parts of the tree into the visible rows.
func (m *treeModel) rebuildRows() {
	m.treeView.rows = m.treeView.rows[:0]
	m.appendRows(m.t.root, "", "", 0)
	m.treeView.clamp(m.rowsHeight())
}

func (m *treeModel) appendRows(n *node, prefix, path string, depth int) {
	children := m.visibleChildren(n)
	for i, c := range children {
		branch, cont := branchFor(i, len(children))
		childPrefix := prefix + cont
		p := path + c.name
		m.treeView.rows = append(m.treeView.rows, uiRow{node: c, path: p, prefix: prefix + branch, depth: depth})
		if !m.isExpanded(c) {
			continue
		}
		if c.isDir() {
			m.appendRows(c, childPrefix, p+"/", depth+1)
			continue
		}
		m.treeView.rows = append(m.treeView.rows, m.versionRows(c, childPrefix, p, depth+1)...)
	}
}

// versionRows lists a file's distinct contents, so the versions can be shown
// underneath it in either view.
func (m *treeModel) versionRows(n *node, prefix, path string, depth int) []uiRow {
	versions := n.entry.versions
	rows := make([]uiRow, 0, len(versions))
	for i, v := range versions {
		branch, _ := branchFor(i, len(versions))
		rows = append(rows, uiRow{
			node:    n,
			path:    path,
			prefix:  prefix + branch,
			depth:   depth,
			version: v,
		})
	}
	return rows
}

// visibleChildren returns the children to show under a node: everything that
// isn't filtered out by the show options or hidden by the query.
func (m *treeModel) visibleChildren(n *node) []*node {
	children := n.sortedChildren()
	kept := children[:0]
	for _, c := range children {
		if m.hidden[c] {
			continue
		}
		if m.query != "" && !m.matched[c] {
			continue
		}
		kept = append(kept, c)
	}
	return kept
}

// isExpanded reports whether a node's contents are shown: a directory's
// children, or a file's versions.
func (m *treeModel) isExpanded(n *node) bool {
	if n == nil {
		return false
	}
	if m.expanded[n] {
		return true
	}
	// Directories on a matching path open automatically so the matches are
	// visible. Files don't, or every match would spill its versions.
	return n.isDir() && m.query != "" && m.matched[n]
}

// applyQuery recomputes which nodes are on a path matching the query.
func (m *treeModel) applyQuery() {
	clear(m.matched)
	if m.query != "" {
		needle := strings.ToLower(m.query)
		var walk func(n *node, path string) bool
		walk = func(n *node, path string) bool {
			match := strings.Contains(strings.ToLower(path), needle)
			for _, c := range n.children {
				childPath := path + c.name
				if c.isDir() {
					childPath += "/"
				}
				if walk(c, childPath) {
					match = true
				}
			}
			if match {
				m.matched[n] = true
			}
			return match
		}
		walk(m.t.root, "")
	}
	m.refresh()
}

func (m *treeModel) Init() tea.Cmd {
	return nil
}

func (m *treeModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		for _, v := range []*viewport{&m.treeView, &m.topView, &m.infoView, &m.targetsView, &m.targetInfoView} {
			v.clamp(m.rowsHeight())
		}
		return m, nil
	case tea.KeyPressMsg:
		if m.typing {
			return m, m.updateQuery(msg)
		}
		return m, m.updateKey(msg)
	case blobStatusMsg:
		for v, state := range msg.state {
			m.blobs[v] = state
		}
		// The info view bakes what it knows into its rows, so it has to be told.
		m.rebuildInfoRows()
		if msg.err != nil {
			m.statusMsg = "could not check the cache: " + msg.err.Error()
		}
		return m, nil
	case fileInfoMsg:
		m.applyFileInfo(msg)
		return m, nil
	case targetDetailMsg:
		m.applyTargetDetail(msg)
		return m, nil
	}
	return m, nil
}

// updateQuery handles keys while the query is being typed.
func (m *treeModel) updateQuery(msg tea.KeyPressMsg) tea.Cmd {
	switch msg.String() {
	case "enter":
		m.typing = false
	case "esc", "ctrl+c":
		m.typing = false
		m.query = ""
		m.applyQuery()
	case "backspace":
		if m.query != "" {
			m.query = m.query[:len(m.query)-1]
			m.applyQuery()
		}
	default:
		// Ignore anything that isn't a plain character, e.g. arrow keys.
		if s := msg.String(); len(s) == 1 {
			m.query += s
			m.applyQuery()
		}
	}
	return nil
}

func (m *treeModel) updateKey(msg tea.KeyPressMsg) tea.Cmd {
	m.statusMsg = ""
	v := m.active()
	switch msg.String() {
	case "q", "ctrl+c":
		return tea.Quit
	case "up", "k":
		v.move(-1, m.rowsHeight())
	case "down", "j":
		v.move(1, m.rowsHeight())
	case "pgup", "ctrl+u":
		v.move(-m.rowsHeight(), m.rowsHeight())
	case "pgdown", "ctrl+d":
		v.move(m.rowsHeight(), m.rowsHeight())
	case "home", "g":
		v.cursor = 0
		v.clamp(m.rowsHeight())
	case "end", "G":
		v.cursor = len(v.rows) - 1
		v.clamp(m.rowsHeight())
	case "m":
		m.setCompareBase(m.current())
	case "c":
		m.compareWithBase()
	case "b":
		m.toggleShow(&m.show.includeGenerated, "generated files")
	case "x":
		m.toggleShow(&m.show.includeExternal, "external repo sources")
	case "p":
		m.toggleMergeConfigs()
	case "i":
		return m.openInfoView()
	case "t":
		m.toggleTopView()
	case "T":
		m.toggleTargetsView()
	case "esc":
		switch {
		case m.view == viewInfo:
			m.view = m.infoReturn
		case m.view == viewTargetInfo:
			m.view = m.targetReturn
		case m.view != viewTree:
			m.view = viewTree
		case m.query != "":
			m.query = ""
			m.applyQuery()
		}
	case "/":
		m.typing = true
	case "n":
		m.jumpToModified(1)
	case "N":
		m.jumpToModified(-1)
	case "enter":
		row := m.current()
		switch {
		case row == nil:
		case row.version != nil:
			m.openLog(m.t.log(row.version.firstLog))
		case row.invocation != nil:
			m.openLog(*row.invocation)
		case row.target != nil:
			return m.openTargetInfo()
		case row.group != nil && row.group.kind == groupEdge:
			// Following the edge is the interesting thing to do with it; the
			// expand keys still open its files.
			return m.followEdge(row.group.edge)
		case m.view == viewTop:
			// Jump to the selected file in the tree.
			m.reveal(row)
		default:
			return m.expand()
		}
	case "right", "l", " ":
		return m.expand()
	case "left", "h":
		m.collapse()
	}
	return nil
}

// toggleShow flips one of the show options and rebuilds around it. Whole
// subtrees appear and disappear, so both lists go back to the top rather than
// leaving the cursor wherever the old rows happened to put it.
func (m *treeModel) toggleShow(option *bool, description string) {
	*option = !*option
	m.refresh()
	m.treeView.cursor, m.treeView.offset = 0, 0
	m.topView.cursor, m.topView.offset = 0, 0

	m.statusMsg = "showing " + description
	if !*option {
		m.statusMsg = "hiding " + description
	}
}

// toggleMergeConfigs switches between the tree as it was logged and the one
// with generated files' configuration directories taken out, building the
// second the first time it's asked for.
func (m *treeModel) toggleMergeConfigs() {
	m.show.mergeConfigs = !m.show.mergeConfigs
	if m.show.mergeConfigs && m.stripped == nil {
		m.stripped = stripConfigs(m.full)
	}
	m.t = m.full
	if m.show.mergeConfigs {
		m.t = m.stripped
	}
	// The two trees share no nodes, and everything remembered about a node is
	// keyed on which node it is.
	clear(m.expanded)
	clear(m.hidden)
	clear(m.hashes)
	m.refresh()
	m.treeView.cursor, m.treeView.offset = 0, 0
	m.topView.cursor, m.topView.offset = 0, 0

	m.statusMsg = "merging configurations"
	if !m.show.mergeConfigs {
		m.statusMsg = "showing each configuration separately"
	}
}

// displayPath is how a path out of a log is shown: with configurations merged,
// a generated path loses the directory that names its own.
func (m *treeModel) displayPath(p string) string {
	if !m.show.mergeConfigs {
		return p
	}
	return stripConfig(p)
}

// displayPaths is a list of paths out of a log as they're shown, which with
// configurations merged is a shorter list.
func (m *treeModel) displayPaths(paths []string) []string {
	if !m.show.mergeConfigs {
		return paths
	}
	shown := make([]string, 0, len(paths))
	for _, p := range paths {
		shown = append(shown, stripConfig(p))
	}
	slices.Sort(shown)
	return slices.Compact(shown)
}

// realPaths returns the paths a shown path stands for: with configurations
// merged, one row can be the same file built several ways, and it's the real
// paths that the logs know about.
func (m *treeModel) realPaths(p string) []string {
	rest, ok := strings.CutPrefix(p, generatedRoot+"/")
	if !m.show.mergeConfigs || !ok {
		return []string{p}
	}
	configs := m.full.root.children[generatedRoot]
	if configs == nil {
		return []string{p}
	}
	var real []string
	for _, config := range configs.sortedChildren() {
		candidate := generatedRoot + "/" + config.name + "/" + rest
		if m.full.find(candidate) != nil {
			real = append(real, candidate)
		}
	}
	if len(real) == 0 {
		return []string{p}
	}
	return real
}

// openInfoView is what "i" means in each view: describe the thing under the
// cursor, or go back if that's what's already on screen.
func (m *treeModel) openInfoView() tea.Cmd {
	switch m.view {
	case viewInfo:
		// A step listed here belongs to a target with a page of its own.
		if row := m.current(); row != nil && row.target != nil {
			return m.openTargetInfo()
		}
		m.view = m.infoReturn
	case viewTargets:
		return m.openTargetInfo()
	case viewTargetInfo:
		// A file listed under a step is a file like any other, and a step on the
		// far end of one belongs to a target worth describing in its own right.
		// Anything else here is about the target this view already shows, so i
		// goes back.
		row := m.current()
		switch {
		case row != nil && row.node != nil:
			return m.openInfo()
		case row != nil && row.group != nil && row.group.kind == groupEdge:
			return m.followEdge(row.group.edge)
		}
		m.view = viewTargets
	case viewTree, viewTop:
		return m.openInfo()
	}
	return nil
}

// openInfo shows what a file was built from and what was built with it, pooled
// across every merged build that mentions it. Which steps touch a file changes
// as a repo does, so no single build has the whole answer.
func (m *treeModel) openInfo() tea.Cmd {
	row := m.current()
	if row == nil || row.node == nil || row.node.isDir() {
		m.statusMsg = "select a file to see what was built with it"
		return nil
	}
	logs := row.node.entry.logs().indexes()
	var sources []logSource
	missing := 0
	for _, logIdx := range logs {
		// A log already read for a target answers for free, whether or not we
		// still have the file it came from.
		if m.graph != nil && m.graphLog == logIdx {
			sources = append(sources, logSource{graph: m.graph})
			continue
		}
		file := m.t.logFile(logIdx)
		if file == "" {
			missing++
			continue
		}
		sources = append(sources, logSource{path: file})
	}
	if len(sources) == 0 {
		m.statusMsg = fmt.Sprintf("no local copy of the %s that mention it; see --log_cache_dir",
			plural(len(logs), "log"))
		return nil
	}

	subject := *row
	m.infoSubject = &subject
	m.infoReturn = m.view
	m.view = viewInfo
	m.fileInfo = &fileDetail{
		path:    row.path,
		configs: len(m.realPaths(row.path)),
		missing: missing,
		status:  fmt.Sprintf("reading %s…", plural(len(sources), "log")),
	}
	m.rebuildInfoRows()
	m.infoView.cursor, m.infoView.offset = 0, 0

	// Reading a log is slow enough to do off the UI goroutine, and there may be
	// dozens of them.
	path, real := row.path, m.realPaths(row.path)
	return func() tea.Msg {
		return fileInfoMsg{path: path, summary: summarizeFile(real, sources)}
	}
}

// applyFileInfo fills the info view in once the logs have been read. A result
// for a file the user has since navigated away from is dropped.
func (m *treeModel) applyFileInfo(msg fileInfoMsg) {
	if m.fileInfo == nil || msg.path != m.fileInfo.path {
		return
	}
	m.fileInfo.summary = msg.summary
	m.fileInfo.status = ""
	m.fileInfo.groups = []*infoGroup{{kind: groupProducers}, {kind: groupConsumers}}
	// Both start closed: for a file in a large build either list can run to
	// hundreds of steps, and the counts are the answer most of the time.
	for _, g := range m.fileInfo.groups {
		delete(m.openGroups, g)
	}
	m.rebuildInfoRows()
}

// rebuildInfoRows lays the file info view out: what produced the file and what
// consumed it, each a list that opens up, then every version we saw of it.
func (m *treeModel) rebuildInfoRows() {
	rows := m.infoView.rows[:0]
	d := m.fileInfo
	if d == nil {
		m.infoView.rows = rows
		return
	}
	rows = append(rows, uiRow{text: d.path}, uiRow{text: m.coverageText(d)}, uiRow{text: ""})
	if d.status != "" {
		rows = append(rows, uiRow{text: d.status})
	}
	if s := d.summary; s != nil && s.read == 0 && s.failed > 0 {
		// Nothing was read, so an empty list would be a claim we can't make.
		rows = append(rows, uiRow{text: fmt.Sprintf("could not read any of the %s that mention this file",
			plural(s.failed, "log"))})
		d.groups = nil
	}
	for _, g := range d.groups {
		steps := m.fileGroupSteps(g)
		if len(steps) == 0 {
			// A list with nothing in it is a fact about the file, not a row to
			// open: a file no spawn wrote is a source file.
			rows = append(rows, uiRow{text: emptyGroupText(g)})
			continue
		}
		rows = append(rows, uiRow{text: m.groupText(g), group: g})
		if !m.openGroups[g] {
			continue
		}
		for i, step := range steps {
			branch, _ := branchFor(i, len(steps))
			// The step belongs to a target with a page of its own, which is
			// where enter goes.
			rows = append(rows, uiRow{
				text:   uiHashStyle.Render(uiGroupIndent+branch) + step.String(),
				target: m.t.targets[step.target],
				depth:  1,
			})
		}
	}
	// The versions come last, and unlike the rest of the view they're rows the
	// compare keys can act on.
	if m.infoSubject != nil && m.infoSubject.node != nil {
		rows = append(rows, m.infoVersionRows(m.infoSubject.node)...)
	}
	m.infoView.rows = rows
	m.infoView.clamp(m.rowsHeight())
}

// coverageText says how much of the file's history the lists below it are drawn
// from.
func (m *treeModel) coverageText(d *fileDetail) string {
	if d.summary == nil {
		return ""
	}
	line := "from " + plural(d.summary.read, "build")
	if d.configs > 1 {
		line += fmt.Sprintf(" · %d configurations", d.configs)
	}
	if d.missing > 0 {
		line += fmt.Sprintf(" · %d not kept locally", d.missing)
	}
	if d.summary.failed > 0 {
		line += fmt.Sprintf(" · %d could not be read", d.summary.failed)
	}
	return uiLabelStyle.Render(line)
}

// fileGroupSteps is the list one of the file info view's rows opens into.
func (m *treeModel) fileGroupSteps(g *infoGroup) []targetMnemonic {
	if m.fileInfo == nil || m.fileInfo.summary == nil {
		return nil
	}
	if g.kind == groupProducers {
		return m.fileInfo.summary.producers
	}
	return m.fileInfo.summary.consumers
}

// emptyGroupText replaces a list that turned out to be empty, which for a file
// says something worth saying on its own.
func emptyGroupText(g *infoGroup) string {
	if g.kind == groupProducers {
		return "no spawn found that outputs this file."
	}
	return "no spawn found that takes this file as an input."
}

// infoVersionRows lists every version of the file, which comes from the merged
// tree rather than from any one log. They carry their version so that the
// compare keys work here the same as they do in the tree.
func (m *treeModel) infoVersionRows(n *node) []uiRow {
	rows := []uiRow{
		{text: ""},
		{text: fmt.Sprintf("versions seen (%d):", len(n.entry.versions))},
	}
	for _, v := range n.entry.versions {
		src := m.t.log(v.firstLog)
		line := "  " + m.opts.formatHash(v.key())
		if v.symlinkTarget == "" {
			line += fmt.Sprintf("  %d bytes", v.digest.GetSizeBytes())
		}
		line += "  " + src.name
		if src.branch != "" {
			line += "  " + src.branch
		}
		if s := m.blobStateText(v); s != "" {
			line += "  " + s
		}
		rows = append(rows, uiRow{text: line, version: v, path: m.filePath()})
	}
	return rows
}

// openTargetInfo describes the target under the cursor, which is either a row
// of the targets list or one of the steps a target info view points at.
func (m *treeModel) openTargetInfo() tea.Cmd {
	row := m.current()
	switch {
	case row != nil && row.target != nil:
		return m.showTarget(row.target)
	case row != nil && row.group != nil && row.group.kind == groupEdge:
		// The step on the far end of a file belongs to a target of its own, and
		// that target has all of this to say for itself.
		return m.followEdge(row.group.edge)
	}
	m.statusMsg = "select a target to see what it built with"
	return nil
}

// followEdge moves to the target on the far end of a dependency or dependent.
func (m *treeModel) followEdge(edge *targetEdge) tea.Cmd {
	ts := m.t.targets[edge.step.target]
	if ts == nil {
		// Every step in the graph ran in a log the merge read, so this shouldn't
		// happen; say so rather than opening an empty view.
		m.statusMsg = "no merged log ran " + edge.step.target
		return nil
	}
	return m.showTarget(ts)
}

// showTarget describes a target: what each of its steps touched, and where they
// ran.
//
// Each step's files come from one log - the newest build that ran that step -
// because expanding what a step consumed means walking its input sets, and
// doing that for every build that ran it would mean reading every log. Steps
// that last ran in different builds are read from different logs, though they
// nearly always share one.
func (m *treeModel) showTarget(ts *targetSteps) tea.Cmd {
	// The edges are about the target as a whole, so they come from its newest
	// build, which is also where most of its steps will be read from.
	logs := m.t.sortLogsByTime(ts.logs)
	logIdx := logs[0]
	logFile := m.t.logFile(logIdx)
	if logFile == "" {
		m.statusMsg = "no local copy of " + m.t.logName(logIdx) + " to read; see --log_cache_dir"
		return nil
	}
	// Each step is read from its own newest build, and the ones we have no local
	// copy of are left unread rather than answered from the wrong build.
	sources := map[string]int{}
	files := map[int]string{logIdx: logFile}
	for mnemonic, ms := range ts.mnemonics {
		stepLog := m.t.sortLogsByTime(ms.logs)[0]
		if _, ok := files[stepLog]; !ok {
			if f := m.t.logFile(stepLog); f != "" {
				files[stepLog] = f
			} else {
				continue
			}
		}
		sources[mnemonic] = stepLog
	}

	m.targetSubject = ts
	m.targetInfo = &targetDetail{label: ts.label, logIdx: logIdx, status: readingStatus(m.t, files)}
	// Leave the targets list on the target being shown, so that going back
	// lands somewhere sensible whichever view the jump came from.
	for i, row := range m.targetsView.rows {
		if row.target == ts {
			m.targetsView.cursor = i
			m.targetsView.clamp(m.rowsHeight())
			break
		}
	}
	if m.view != viewTargetInfo {
		// A chain of jumps between targets keeps the view it started from.
		m.targetReturn = m.view
	}
	m.view = viewTargetInfo
	m.rebuildTargetInfoRows()
	m.targetInfoView.cursor, m.targetInfoView.offset = 0, 0

	// A log already read for a file, or for a neighbouring target, answers this
	// straight away; otherwise reading one is slow enough to do off the UI
	// goroutine.
	memo, label := m.graph, ts.label
	memoLog := m.graphLog
	return func() tea.Msg {
		graphs := map[int]*spawnGraph{}
		if memo != nil {
			graphs[memoLog] = memo
		}
		read := func(idx int) (*spawnGraph, error) {
			if g, ok := graphs[idx]; ok {
				return g, nil
			}
			g, err := loadSpawnGraph(files[idx])
			if err != nil {
				return nil, err
			}
			graphs[idx] = g
			return g, nil
		}

		primary, err := read(logIdx)
		if err != nil {
			return targetDetailMsg{label: label, logIdx: logIdx, err: err}
		}
		described := primary.describe(label)
		msg := targetDetailMsg{
			label:        label,
			logIdx:       logIdx,
			graph:        primary,
			read:         true,
			steps:        map[string]*stepFiles{},
			sources:      sources,
			dependencies: described.dependencies,
			dependents:   described.dependents,
		}
		for mnemonic, idx := range sources {
			from := described
			if idx != logIdx {
				g, err := read(idx)
				if err != nil {
					// The rest of the target is still worth showing; this step
					// says its files are unknown.
					continue
				}
				from = g.describe(label)
			}
			if step, ok := from.step(mnemonic); ok {
				msg.steps[mnemonic] = step
			}
		}
		return msg
	}
}

// readingStatus says which logs the view is waiting on.
func readingStatus(t *tree, logs map[int]string) string {
	if len(logs) == 1 {
		for idx := range logs {
			return "reading " + t.logName(idx) + "…"
		}
	}
	return fmt.Sprintf("reading %s…", plural(len(logs), "log"))
}

// applyTargetDetail fills the target info view in once its log has been read. A
// result for a target the user has since navigated away from is dropped.
func (m *treeModel) applyTargetDetail(msg targetDetailMsg) {
	if m.targetSubject == nil || msg.label != m.targetSubject.label {
		return
	}
	if msg.graph != nil {
		m.graph, m.graphLog = msg.graph, msg.logIdx
	}
	detail := &targetDetail{label: msg.label, logIdx: msg.logIdx, read: msg.read}
	if msg.err != nil {
		detail.status = "could not read " + m.t.logName(msg.logIdx) + ": " + msg.err.Error()
	}
	// The steps come from the merge, which saw every log; the files under them
	// come from the build each step last ran in.
	for _, ms := range m.targetSubject.sortedMnemonics() {
		logs := m.t.sortLogsByTime(ms.logs)
		step := &targetStep{
			mnemonic:  ms.name,
			logs:      logs,
			logIdx:    logs[0],
			platforms: ms.sortedPlatforms(),
		}
		if idx, ok := msg.sources[ms.name]; ok {
			step.logIdx = idx
		}
		if files, ok := msg.steps[ms.name]; ok {
			step.inputs, step.outputs, step.read = files.inputs, files.outputs, true
		}
		// The builds it ran in come first: which of them the files below were
		// read from is the first thing to know about them.
		step.groups = []*infoGroup{
			{step: step, kind: groupInvocations},
			{step: step, kind: groupInputs},
			{step: step, kind: groupOutputs},
		}
		detail.steps = append(detail.steps, step)
	}
	detail.dependencies = targetEdges(msg.dependencies)
	detail.dependents = targetEdges(msg.dependents)
	detail.groups = []*infoGroup{{kind: groupDependencies}, {kind: groupDependents}}
	m.targetInfo = detail
	clear(m.openGroups)
	m.rebuildTargetInfoRows()
}

// targetEdges wraps what the graph found in the rows the view opens them into.
func targetEdges(found []graphEdge) []*targetEdge {
	edges := make([]*targetEdge, 0, len(found))
	for _, e := range found {
		edge := &targetEdge{graphEdge: e}
		edge.group = &infoGroup{kind: groupEdge, edge: edge}
		edges = append(edges, edge)
	}
	return edges
}

// filePath names the file the info view is showing, if any.
func (m *treeModel) filePath() string {
	if m.fileInfo == nil {
		return ""
	}
	return m.fileInfo.path
}

// targetLabel names the target the info view is showing, if any.
func (m *treeModel) targetLabel() string {
	if m.targetSubject == nil {
		return ""
	}
	return m.targetSubject.label
}

// toggleTargetsView shows the targets the logs ran, or goes back to the tree.
func (m *treeModel) toggleTargetsView() {
	if m.view == viewTargets {
		m.view = viewTree
		return
	}
	m.view = viewTargets
	m.targetsView.clamp(m.rowsHeight())
}

// openLog opens a build in the browser, using the same app URL the logs were
// downloaded from.
func (m *treeModel) openLog(src logInfo) {
	if src.invocationID == "" {
		m.statusMsg = "no invocation to open: " + src.name + " was read from a file"
		return
	}
	m.open(appBaseURL() + "/invocation/" + src.invocationID)
}

// setCompareBase arms a version as the left side of a comparison, or disarms it
// if it was already the base.
func (m *treeModel) setCompareBase(row *uiRow) {
	if row == nil || row.version == nil {
		m.statusMsg = "select a file version to mark as the compare base"
		return
	}
	src := m.t.log(row.version.firstLog)
	if src.invocationID == "" {
		m.statusMsg = "no invocation to compare: " + src.name + " was read from a file"
		return
	}
	if m.base != nil && m.base.version == row.version {
		m.base = nil
		m.statusMsg = "compare base cleared"
		return
	}
	m.base = &compareBase{path: row.path, version: row.version}
	m.statusMsg = "compare base: " + row.path + " from " + src.invocationID
}

// compareWithBase opens the app's comparison between the base build and the
// build that produced the selected version.
func (m *treeModel) compareWithBase() {
	row := m.current()
	if row == nil || row.version == nil {
		m.statusMsg = "select a file version to compare"
		return
	}
	if m.base == nil {
		m.statusMsg = "no compare base yet: press b on a file version first"
		return
	}
	baseID := m.t.log(m.base.version.firstLog).invocationID
	src := m.t.log(row.version.firstLog)
	if src.invocationID == "" {
		m.statusMsg = "no invocation to compare: " + src.name + " was read from a file"
		return
	}
	if src.invocationID == baseID {
		m.statusMsg = "both versions came from the same build"
		return
	}
	// Two versions of the same file diff nicely in the code viewer. Anything
	// else can only be compared build to build.
	if row.path == m.base.path {
		if url := m.diffURL(m.base.version, row.version, row.path); url != "" {
			m.open(url)
			return
		}
	}
	m.open(appBaseURL() + "/compare/" + baseID + "..." + src.invocationID)
}

// diffURL builds a link to the code viewer's diff of two versions of one file.
// It returns "" when we don't know enough to address the blobs, in which case
// the caller falls back to comparing the builds.
func (m *treeModel) diffURL(base, other *version, path string) string {
	repo := repoPath(m.t.repo)
	baseURI := m.bytestreamURI(base)
	otherURI := m.bytestreamURI(other)
	if repo == "" || baseURI == "" || otherURI == "" {
		return ""
	}
	params := url.Values{
		"bytestream_url":         {baseURI},
		"invocation_id":          {m.t.log(base.firstLog).invocationID},
		"filename":               {path},
		"compare_bytestream_url": {otherURI},
		"compare_invocation_id":  {m.t.log(other.firstLog).invocationID},
		"compare_filename":       {path},
	}
	return fmt.Sprintf("%s/code/%s/?%s#diff", appBaseURL(), repo, params.Encode())
}

// bytestreamURI addresses a version's contents in the cache the build wrote
// them to.
func (m *treeModel) bytestreamURI(v *version) string {
	src := m.t.log(v.firstLog)
	if src.cacheHost == "" || v.digest.GetHash() == "" {
		return ""
	}
	digestFunction, err := digest.ParseFunction(m.t.hashFunction)
	if err != nil {
		return ""
	}
	d := &repb.Digest{Hash: v.digest.GetHash(), SizeBytes: v.digest.GetSizeBytes()}
	rn := digest.NewCASResourceName(d, src.instanceName, digestFunction)
	return "bytestream://" + src.cacheHost + "/" + strings.TrimPrefix(rn.DownloadString(), "/")
}

// repoPath turns a git remote URL into the "owner/repo" the code viewer expects.
func repoPath(repoURL string) string {
	trimmed := strings.TrimSuffix(repoURL, ".git")
	// Handle both https://host/owner/repo and git@host:owner/repo.
	if _, after, ok := strings.Cut(trimmed, "://"); ok {
		trimmed = after
	}
	if _, after, ok := strings.Cut(trimmed, "@"); ok {
		trimmed = after
	}
	trimmed = strings.Replace(trimmed, ":", "/", 1)
	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) < 3 {
		return ""
	}
	// Drop the host, keep owner/repo.
	return strings.Join(parts[len(parts)-2:], "/")
}

// open hands a URL to the browser and reports what happened.
func (m *treeModel) open(url string) {
	if err := openInBrowser(url); err != nil {
		m.statusMsg = "could not open " + url + ": " + err.Error()
		return
	}
	m.statusMsg = "opened " + url
}

// appBaseURL is the app URL without its trailing slash, ready to have a path
// appended.
func appBaseURL() string {
	return strings.TrimSuffix(*appURL, "/")
}

// openInBrowser hands a URL to the platform's opener. It's a variable so that
// tests can watch it instead of launching a browser.
var openInBrowser = func(url string) error {
	for _, name := range []string{"open", "xdg-open"} {
		if opener, err := exec.LookPath(name); err == nil {
			// Start rather than Run: the UI shouldn't wait on the browser.
			return exec.Command(opener, url).Start()
		}
	}
	return fmt.Errorf("no 'open' or 'xdg-open' binary found")
}

// active is the viewport of the screen that's showing.
func (m *treeModel) active() *viewport {
	switch m.view {
	case viewTop:
		return &m.topView
	case viewInfo:
		return &m.infoView
	case viewTargets:
		return &m.targetsView
	case viewTargetInfo:
		return &m.targetInfoView
	default:
		return &m.treeView
	}
}

func (m *treeModel) current() *uiRow {
	return m.active().current()
}

func (m *treeModel) toggleTopView() {
	if m.view == viewTop {
		m.view = viewTree
		return
	}
	m.view = viewTop
	m.topView.clamp(m.rowsHeight())
}

// rowExpandable reports whether a row has anything under it to open: a
// directory's children, a file's versions, or one of the target info view's
// lists. Version rows have nothing, and prose rows aren't anything.
func (m *treeModel) rowExpandable(row *uiRow) bool {
	if row == nil || row.version != nil {
		return false
	}
	return row.group != nil || row.node != nil
}

// rowOpen reports whether an expandable row is currently open.
func (m *treeModel) rowOpen(row *uiRow) bool {
	switch {
	case row == nil || row.version != nil:
		return false
	case row.group != nil:
		return m.openGroups[row.group]
	default:
		return m.isExpanded(row.node)
	}
}

// setRowOpen opens or closes a row, leaving rows with nothing to open alone.
func (m *treeModel) setRowOpen(row *uiRow, open bool) {
	switch {
	case !m.rowExpandable(row):
	case row.group != nil:
		if open {
			m.openGroups[row.group] = true
		} else {
			delete(m.openGroups, row.group)
		}
	default:
		if open {
			m.expanded[row.node] = true
		} else {
			delete(m.expanded, row.node)
		}
	}
}

// expand opens the selected row: a directory's contents, a file's versions, or
// one of a build step's lists. It returns the command to check whether a file's
// contents are still cached, if there's anything to check.
func (m *treeModel) expand() tea.Cmd {
	v := m.active()
	row := v.current()
	if !m.rowExpandable(row) || m.rowOpen(row) {
		return nil
	}
	// The rows are about to be rebuilt in place, so anything needed from this
	// one has to be read first.
	n := row.node
	m.setRowOpen(row, true)
	m.rebuildExpanded()
	// What was just opened is below the cursor, so pull the cursor up the
	// screen to show as much of it as we can.
	v.scrollToMidpoint(m.rowsHeight())
	if n == nil || n.isDir() {
		return nil
	}
	return m.checkBlobs(n)
}

// checkBlobs asks the CAS which of a file's versions can still be fetched. The
// lookup runs off the UI thread and lands back as a blobStatusMsg.
func (m *treeModel) checkBlobs(n *node) tea.Cmd {
	if m.checker == nil {
		return nil
	}
	digestFunction, err := digest.ParseFunction(m.t.hashFunction)
	if err != nil {
		return nil
	}
	// Versions from different builds can live under different remote instance
	// names, and FindMissingBlobs takes one per request.
	byInstance := map[string][]*version{}
	for _, v := range n.entry.versions {
		if v.digest.GetHash() == "" || m.blobs[v] != blobUnknown {
			continue
		}
		src := m.t.log(v.firstLog)
		if src.cacheHost == "" {
			continue
		}
		m.blobs[v] = blobChecking
		byInstance[src.instanceName] = append(byInstance[src.instanceName], v)
	}
	if len(byInstance) == 0 {
		return nil
	}
	checker := m.checker
	return func() tea.Msg {
		state := map[*version]blobState{}
		var firstErr error
		for instanceName, versions := range byInstance {
			digests := make([]*repb.Digest, 0, len(versions))
			for _, v := range versions {
				// The execution log has its own Digest message; the cache API
				// wants the remote execution one.
				digests = append(digests, &repb.Digest{
					Hash:      v.digest.GetHash(),
					SizeBytes: v.digest.GetSizeBytes(),
				})
			}
			missing, err := checker.missingBlobs(context.Background(), instanceName, digestFunction, digests)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				// Leave these unknown rather than claiming they're gone.
				for _, v := range versions {
					state[v] = blobUnknown
				}
				continue
			}
			gone := make(map[string]bool, len(missing))
			for _, d := range missing {
				gone[d.GetHash()] = true
			}
			for _, v := range versions {
				if gone[v.digest.GetHash()] {
					state[v] = blobEvicted
				} else {
					state[v] = blobCached
				}
			}
		}
		return blobStatusMsg{state: state, err: firstErr}
	}
}

// rebuildExpanded refreshes every row set after something opened or closed.
// Expansion is shared: a file opened in one view is open in the others.
func (m *treeModel) rebuildExpanded() {
	m.rebuildRows()
	m.rebuildTopRows()
	m.rebuildTargetInfoRows()
	m.rebuildInfoRows()
}

// collapse closes an open row, leaving the highlight on it. Anything with
// nothing to close - a directory or file that's already closed, or one of a
// file's versions - closes its parent instead and moves the highlight there, so
// repeating collapse walks back up.
func (m *treeModel) collapse() {
	v := m.active()
	row := v.current()
	if row == nil {
		return
	}
	if m.rowOpen(row) {
		m.setRowOpen(row, false)
		m.rebuildExpanded()
		return
	}
	m.collapseParent(v, row.depth)
}

// collapseParent closes the row that contains the one at the cursor and moves
// the highlight to it, scrolling it into view.
func (m *treeModel) collapseParent(v *viewport, depth int) {
	for i := v.cursor - 1; i >= 0; i-- {
		if v.rows[i].depth < depth {
			m.setRowOpen(&v.rows[i], false)
			m.rebuildExpanded()
			// Closing a row only removes the rows underneath it, so the parent
			// is still where it was.
			v.cursor = i
			v.clamp(m.rowsHeight())
			return
		}
	}
}

// jumpToModified moves to the next or previous modified file, expanding
// whatever is hiding it.
func (m *treeModel) jumpToModified(delta int) {
	if m.view != viewTree && m.view != viewTop {
		// The other views are each about one thing. Stepping to the next
		// modified file would mean leaving them, which isn't what the key is
		// for.
		return
	}
	if len(m.modified) == 0 {
		m.statusMsg = "no modified files"
		return
	}
	// Clear first, so the wrap notice below can tell whether reveal had
	// something of its own to say.
	m.statusMsg = ""
	anchor := m.anchorFile()
	var target *uiRow
	wrapped := false
	if delta > 0 {
		for i := range m.modified {
			if m.modified[i].fileIndex > anchor {
				target = &m.modified[i]
				break
			}
		}
		if target == nil {
			target, wrapped = &m.modified[0], true
		}
	} else {
		for i := len(m.modified) - 1; i >= 0; i-- {
			if m.modified[i].fileIndex < anchor {
				target = &m.modified[i]
				break
			}
		}
		if target == nil {
			target, wrapped = &m.modified[len(m.modified)-1], true
		}
	}
	m.reveal(target)
	if wrapped && m.statusMsg == "" {
		m.statusMsg = "wrapped around to the " + map[bool]string{true: "first", false: "last"}[delta > 0] + " modified file"
	}
}

// anchorFile is the position in the file order that a jump searches out from:
// the selected file, or for a directory, the slot just before the first file
// inside it, so that a directory's own contents are still ahead of the cursor.
func (m *treeModel) anchorFile() int {
	row := m.current()
	if row == nil || row.node == nil {
		return -1
	}
	if !row.node.isDir() {
		if i, ok := m.fileIndex[row.node]; ok {
			return i
		}
		return -1
	}
	prefix := row.path + "/"
	for i, f := range m.files {
		if strings.HasPrefix(f.path, prefix) {
			return i - 1
		}
	}
	return -1
}

// reveal switches to the tree view and puts the cursor on the given file,
// expanding the directories above it.
func (m *treeModel) reveal(row *uiRow) {
	if row == nil {
		return
	}
	for _, a := range row.ancestors {
		m.expanded[a] = true
	}
	m.view = viewTree
	m.rebuildRows()
	for i, r := range m.treeView.rows {
		if r.node == row.node {
			m.treeView.cursor = i
			m.treeView.clamp(m.rowsHeight())
			return
		}
	}
	m.statusMsg = "could not find " + row.path + " in the tree"
}

// rowsHeight is how many rows fit on screen, after the title, the detail pane,
// and the help line.
func (m *treeModel) rowsHeight() int {
	return max(m.height-uiDetailLines-3, 1)
}

// View lays out the title, the visible slice of rows, the detail pane, and the
// help line, always filling exactly the terminal height so nothing jumps around
// as the selection moves.
func (m *treeModel) View() tea.View {
	v := m.active()
	lines := make([]string, 0, m.height)
	lines = append(lines, m.titleView())
	for i := range m.rowsHeight() {
		if idx := v.offset + i; idx < len(v.rows) {
			lines = append(lines, m.rowView(v, idx))
		} else {
			lines = append(lines, "")
		}
	}
	lines = append(lines, uiHashStyle.Render(strings.Repeat("─", max(m.width, 1))))
	lines = append(lines, strings.Split(m.detailView(), "\n")...)
	lines = append(lines, m.helpView())

	view := tea.NewView(strings.Join(lines, "\n"))
	view.AltScreen = true
	return view
}

func (m *treeModel) titleView() string {
	title := uiTitleStyle.Render("srctree")
	summary := fmt.Sprintf(" %d files from %d log(s) · %d modified",
		m.visibleFiles, len(m.t.logs), m.visibleModified)
	switch m.view {
	case viewTop:
		summary = fmt.Sprintf(" most changed · %d files · %d modified", len(m.topView.rows), len(m.modified))
	case viewInfo:
		summary = " info · " + m.filePath()
	case viewTargets:
		summary = fmt.Sprintf(" targets · %d of %d", len(m.targetsView.rows), len(m.targets))
	case viewTargetInfo:
		summary = " target · " + m.targetLabel()
	case viewTree:
		// The counts above already describe the tree.
	}
	line := title + summary
	// Only mention the extra content when it's showing: hiding it is the
	// default and doesn't need saying.
	if m.show.includeGenerated {
		line += uiModifiedStyle.Render(" +generated")
	}
	if m.show.includeExternal {
		line += uiModifiedStyle.Render(" +external")
	}
	if m.show.mergeConfigs {
		line += uiModifiedStyle.Render(" +merged configs")
	}
	if m.base != nil {
		line += uiModifiedStyle.Render(" base " + m.opts.formatHash(m.base.version.key()))
	}
	if m.typing || m.query != "" {
		line += uiFilterStyle.Render(fmt.Sprintf("  /%s", m.query))
		if m.typing {
			line += uiFilterStyle.Render("█")
		}
	}
	return truncate(line, m.width)
}

// rowView renders one line of the given viewport.
func (m *treeModel) rowView(v *viewport, idx int) string {
	row := v.rows[idx]
	// Every row reserves a gutter so the list stays aligned, and the selected
	// one puts a marker in it. The background highlight alone isn't visible in
	// every terminal.
	line := uiGutter
	if idx == v.cursor {
		line = uiCursorGutter
	}
	switch {
	case row.target != nil && row.text == "":
		line += m.targetRowView(row)
	case row.node == nil:
		// A pre-rendered line, as the info views use. The base marker is the one
		// part that can change after the line was built.
		line += row.text
		if row.version != nil && m.base != nil && m.base.version == row.version {
			line += uiModifiedStyle.Render("  " + uiBaseMarker)
		}
	case row.version != nil:
		line += m.versionRowView(row)
	case m.view == viewTargetInfo:
		line += m.stepFileRowView(row)
	case m.view == viewTop:
		line += m.topRowView(row)
	default:
		line += m.treeRowView(row)
	}
	line = truncate(line, m.width)
	if idx == v.cursor {
		return uiCursorStyle.Render(padRight(line, m.width))
	}
	return line
}

func (m *treeModel) treeRowView(row uiRow) string {
	n := row.node
	line := uiHashStyle.Render(row.prefix)
	if n.isDir() {
		line += uiDirStyle.Render(n.name + "/")
		if m.opts.dirHashes {
			line += uiHashStyle.Render("  " + m.opts.formatHash(m.dirHash(n)))
		}
		if n.modifiedCount > 0 {
			line += uiModifiedStyle.Render(" " + modifiedMarker)
		}
		return line
	}
	line += n.name
	line += uiHashStyle.Render("  " + m.versionSummary(n.entry))
	if n.entry.modified() {
		line += uiModifiedStyle.Render(" " + modifiedMarker)
	}
	return line
}

// versionRowView renders one version of a file as part of the tree: its digest,
// the build it came from, and how big it was.
func (m *treeModel) versionRowView(row uiRow) string {
	v := row.version
	src := m.t.log(v.firstLog)
	line := uiHashStyle.Render(row.prefix) + m.opts.formatHash(v.key())

	origin := src.invocationID
	if origin == "" {
		// A log read from a file has no invocation to name.
		origin = src.name
	}
	line += uiLabelStyle.Render("  " + origin)
	if src.branch != "" {
		line += uiLabelStyle.Render("  " + src.branch)
	}
	if v.symlinkTarget == "" {
		line += uiHashStyle.Render(fmt.Sprintf("  %d", v.digest.GetSizeBytes()))
	}
	if m.base != nil && m.base.version == v {
		line += uiModifiedStyle.Render("  " + uiBaseMarker)
	}
	if s := m.blobStateText(v); s != "" {
		line += uiHashStyle.Render("  " + s)
	}
	return line
}

// blobStateText says whether a version's contents can still be fetched.
func (m *treeModel) blobStateText(v *version) string {
	switch m.blobs[v] {
	case blobChecking:
		return "checking cache…"
	case blobCached:
		return "cached"
	case blobEvicted:
		return "evicted"
	default:
		return ""
	}
}

// targetRowView renders a target as "<logs>  <label>": how many of the merged
// builds ran it, then which target it is.
func (m *treeModel) targetRowView(row uiRow) string {
	count := len(row.target.logs)
	countText := fmt.Sprintf("%4d  ", count)
	if count > 1 {
		return uiModifiedStyle.Render(countText) + row.target.label
	}
	return uiHashStyle.Render(countText) + row.target.label
}

// stepFileRowView renders one of the files a build step touched. Unlike the
// tree it shows the whole path, since these files are scattered across it.
func (m *treeModel) stepFileRowView(row uiRow) string {
	line := uiHashStyle.Render(row.prefix) + row.path
	line += uiHashStyle.Render("  " + m.versionSummary(row.node.entry))
	if row.node.entry.modified() {
		line += uiModifiedStyle.Render(" " + modifiedMarker)
	}
	return line
}

// topRowView renders a file as "<digests>  <path>": the count of distinct
// contents the walk saw for it, then where it lives.
func (m *treeModel) topRowView(row uiRow) string {
	count := len(row.node.entry.versions)
	countText := fmt.Sprintf("%4d  ", count)
	if count > 1 {
		return uiModifiedStyle.Render(countText) + row.path
	}
	return uiHashStyle.Render(countText) + row.path
}

// versionSummary describes a file's contents in one line: the first version,
// plus how many others there are. The detail pane has the rest.
func (m *treeModel) versionSummary(e *entry) string {
	v := e.versions[0]
	s := m.opts.formatHash(v.key())
	if v.symlinkTarget == "" {
		s = fmt.Sprintf("%s  %d", s, v.digest.GetSizeBytes())
	}
	if len(e.versions) > 1 {
		s += fmt.Sprintf("  +%d more", len(e.versions)-1)
	}
	return s
}

// detailView describes the selected node: for a file, every version and the log
// it first came from; for a directory, what's under it.
func (m *treeModel) detailView() string {
	lines := m.detailLines()
	if m.statusMsg != "" {
		lines = append(lines, uiModifiedStyle.Render(m.statusMsg))
	}
	for i := range lines {
		lines[i] = truncate(lines[i], m.width)
	}
	return padLines(lines, uiDetailLines)
}

func (m *treeModel) detailLines() []string {
	row := m.current()
	if m.view == viewInfo {
		// The info view is all detail already; the pane just says whose.
		row = m.infoSubject
	}
	switch {
	case row != nil && row.target != nil:
		return m.targetDetailLines(row.target)
	case row != nil && row.invocation != nil:
		return m.logDetailLines(*row.invocation)
	case m.view == viewTargetInfo && (row == nil || row.node == nil):
		// The rows between the files are about the target as a whole.
		if m.targetSubject == nil {
			return nil
		}
		return m.targetDetailLines(m.targetSubject)
	case row == nil || row.node == nil:
		return []string{uiLabelStyle.Render("no files to show")}
	}
	lines := []string{uiDirStyle.Render(row.path)}
	switch {
	case row.version != nil:
		return append(lines, m.versionDetail(row.version)...)
	case row.node.isDir():
		lines = append(lines, fmt.Sprintf("%s %d files, %d modified",
			uiLabelStyle.Render("contains"), row.node.fileCount, row.node.modifiedCount))
		if m.opts.dirHashes {
			lines = append(lines, uiLabelStyle.Render("hash     ")+m.dirHash(row.node))
		}
		return lines
	}
	for i, v := range row.node.entry.versions {
		if len(lines) >= uiDetailLines {
			lines = append(lines, uiLabelStyle.Render(fmt.Sprintf("... and %d more versions",
				len(row.node.entry.versions)-i)))
			break
		}
		desc := v.key()
		if v.symlinkTarget == "" {
			desc = fmt.Sprintf("%s  %d bytes", desc, v.digest.GetSizeBytes())
		}
		lines = append(lines, fmt.Sprintf("%s %s", desc,
			uiLabelStyle.Render("first in "+m.t.logName(v.firstLog))))
	}
	return lines
}

// targetDetailLines describes a target: how widely it ran, and what it ran.
func (m *treeModel) targetDetailLines(ts *targetSteps) []string {
	lines := []string{uiDirStyle.Render(ts.label)}
	lines = append(lines, fmt.Sprintf("%s %s of %s",
		uiLabelStyle.Render("ran in  "), plural(len(ts.logs), "build"), plural(len(m.t.logs), "log")))
	mnemonics := make([]string, 0, len(ts.mnemonics))
	for _, ms := range ts.sortedMnemonics() {
		mnemonics = append(mnemonics, ms.name)
	}
	lines = append(lines, uiLabelStyle.Render("steps    ")+strings.Join(mnemonics, ", "))
	if m.view != viewTargetInfo {
		lines = append(lines, uiLabelStyle.Render("i        ")+"what it built with, and where it ran")
	}
	return lines
}

// logDetailLines describes one of the builds a step ran in.
func (m *treeModel) logDetailLines(src logInfo) []string {
	lines := []string{uiDirStyle.Render(src.name)}
	if when := src.when(); when != "" {
		lines = append(lines, uiLabelStyle.Render("ran at   ")+when)
	}
	if src.branch != "" {
		lines = append(lines, uiLabelStyle.Render("branch   ")+src.branch)
	}
	if src.invocationID != "" {
		lines = append(lines, uiLabelStyle.Render("enter    ")+appBaseURL()+"/invocation/"+src.invocationID)
	}
	return lines
}

// versionDetail describes the selected version of a file, and where enter
// would take you.
func (m *treeModel) versionDetail(v *version) []string {
	src := m.t.log(v.firstLog)
	desc := v.key()
	if v.symlinkTarget == "" {
		desc = fmt.Sprintf("%s  %d bytes", desc, v.digest.GetSizeBytes())
	}
	lines := []string{desc, uiLabelStyle.Render("first in  ") + src.name}
	if src.branch != "" {
		lines = append(lines, uiLabelStyle.Render("branch    ")+src.branch)
	}
	if src.invocationID != "" {
		lines = append(lines, uiLabelStyle.Render("enter     ")+appBaseURL()+"/invocation/"+src.invocationID)
	}
	switch {
	case m.base == nil:
		lines = append(lines, uiLabelStyle.Render("m         ")+"mark as the base for comparison")
	case m.base.version == v:
		lines = append(lines, uiLabelStyle.Render("base      ")+"this version; c compares another one against it")
	default:
		lines = append(lines, uiLabelStyle.Render("c         ")+"compare against "+
			m.base.path+" "+m.opts.formatHash(m.base.version.key()))
	}
	return lines
}

// padLines pads or trims a block of lines to exactly n lines, so that the panes
// below it don't move around.
func padLines(lines []string, n int) string {
	for len(lines) < n {
		lines = append(lines, "")
	}
	return strings.Join(lines[:n], "\n")
}

func (m *treeModel) helpView() string {
	if m.typing {
		return uiHelpStyle.Render("type to filter · enter accept · esc cancel")
	}
	row := m.current()
	help := "↑↓/jk move · →←/lh expand · i info · n/N modified · t most changed · T targets · b/x/p generated/external/configs · / filter · q quit"
	switch m.view {
	case viewInfo:
		switch {
		case row != nil && row.version != nil:
			help = "↑↓/jk move · m compare base · c compare · i/esc back · q quit"
		case row != nil && row.target != nil:
			help = "↑↓/jk move · i/enter go to target · esc back · q quit"
		default:
			help = "↑↓/jk move · →←/lh open · i/esc back · q quit"
		}
		return uiHelpStyle.Render(truncate(help, m.width))
	case viewTargets:
		help = "↑↓/jk move · i/enter target info · / filter · t most changed · T back to tree · q quit"
		return uiHelpStyle.Render(truncate(help, m.width))
	case viewTargetInfo:
		switch {
		case row != nil && row.version != nil:
			help = "↑↓/jk move · enter open build · m compare base · c compare · esc back · q quit"
		case row != nil && row.invocation != nil:
			help = "↑↓/jk move · enter open build · esc back · q quit"
		case row != nil && row.node != nil:
			help = "↑↓/jk move · →←/lh versions · i file info · esc back · q quit"
		case row != nil && row.group != nil && row.group.kind == groupEdge:
			help = "↑↓/jk move · →←/lh files · i/enter go to target · esc back · q quit"
		default:
			help = "↑↓/jk move · →←/lh open · i/esc back · q quit"
		}
		return uiHelpStyle.Render(truncate(help, m.width))
	case viewTop:
		help = "↑↓/jk move · →←/lh versions · i info · enter show in tree · t back to tree · T targets · b/x/p filters · q quit"
	case viewTree:
		// The default line covers it.
	}
	// A file version has its own actions, and they're the interesting ones.
	if row != nil && row.version != nil {
		help = "↑↓/jk move · ←/h close · enter open build · i info · m compare base · c compare · q quit"
	}
	return uiHelpStyle.Render(truncate(help, m.width))
}

// truncate cuts a rendered line to the given width, counting display cells so
// that styling escape sequences don't count against it.
func truncate(s string, width int) string {
	if width <= 0 || lipgloss.Width(s) <= width {
		return s
	}
	return lipgloss.NewStyle().MaxWidth(width).Render(s)
}

func padRight(s string, width int) string {
	if pad := width - lipgloss.Width(s); pad > 0 {
		return s + strings.Repeat(" ", pad)
	}
	return s
}

// runUI shows the tree in an interactive browser, and reports which parts of it
// were showing when the user quit. The checker may be nil, in which case the
// UI doesn't report whether a file's versions are still cached.
func runUI(t *tree, opts printOptions, show filterOptions, checker blobChecker) (filterOptions, error) {
	m := newTreeModel(t, opts, show, checker)
	if _, err := tea.NewProgram(m).Run(); err != nil {
		return show, err
	}
	return m.show, nil
}
