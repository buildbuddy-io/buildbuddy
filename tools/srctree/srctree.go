// srctree reconstructs the source tree of a build from one or more Bazel
// compact execution logs (--experimental_execution_log_compact_file), and
// prints it as a tree of file names and content digests.
//
// The compact execution log is a zstd-compressed stream of length-delimited
// ExecLogEntry protos. Every file that participated in the build - as a spawn
// input or output - appears in the log exactly once as a File entry (or as part
// of a Directory entry), so walking the entries is enough to recover the paths
// and digests of the whole tree; we never need to reconstruct the spawns
// themselves.
//
// A single log only covers the files that the actions in that invocation
// happened to touch, so multiple logs can be merged into one tree to get better
// coverage of the repository. When the same path shows up with different
// contents in different logs, every distinct version is kept, and the file is
// reported as modified and marked with "(!)" in the output.
//
// The full tree (including generated files under bazel-out) is built up while
// reading the logs; generated paths are only filtered out as a final step
// before printing, so that later uses of this tool can keep them.
//
// Logs can either be given as local files:
//
//	bb run //tools/srctree -- $PWD/tools/srctree/example-cel.binpb.zst $PWD/tools/srctree/example-cel-2.binpb.zst
//
// or fetched from a BuildBuddy server, which prompts for one of the repos built
// in the last week and then walks back through that repo's CI invocations,
// merging each one's log until history runs out or you interrupt it:
//
//	bb run //tools/srctree -- --api_key=$API_KEY
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/term"
	"google.golang.org/protobuf/encoding/protowire"

	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

var (
	format           = flag.String("format", "", "Output format: 'ui' for the interactive browser, 'tree', or 'flat'. Defaults to 'ui' when stdout is a terminal, 'tree' otherwise.")
	includeGenerated = flag.Bool("include_generated", false, "Include generated files (bazel-out and friends) in the output.")
	includeExternal  = flag.Bool("include_external", false, "Include sources of external repositories (external/...) in the output.")
	mergeConfigs     = flag.Bool("merge_configs", false, "Merge generated files built in different Bazel configurations, dropping the configuration directory (bazel-out/<config>/...) from their paths.")
	hashLength       = flag.Int("hash_length", 0, "Truncate printed digest hashes to this many characters. 0 means no truncation.")
	dirHashes        = flag.Bool("dir_hashes", true, "Print a recursively computed hash for each directory.")
	listModified     = flag.Bool("list_modified", false, "List every modified file, and the log each version came from, in the summary.")
	listInvocations  = flag.Bool("list_invocations", false, "List the invocation ID of every merged log in the summary.")

	// Flags for fetching logs from a BuildBuddy server instead of reading them
	// from disk.
	apiKey         = flag.String("api_key", "", "BuildBuddy API key. If set, execution logs are fetched from --target instead of read from local files.")
	target         = flag.String("target", "grpcs://remote.buildbuddy.io", "BuildBuddy API server gRPC target, used to look up invocations.")
	appURL         = flag.String("app_url", "https://app.buildbuddy.io", "BuildBuddy app URL, used to download execution logs.")
	repoURL        = flag.String("repo", "", "Repo to fetch invocations for. If unset, you'll be prompted to pick one of the repos built recently.")
	days           = flag.Int("days", 7, "How many days back to look for repos and invocations.")
	maxInvocations = flag.Int("max_invocations", 0, "Stop after merging this many CI invocations. 0 means keep going until the search window runs out.")
	groupID        = flag.String("group_id", "", "Group ID to list repos for. Defaults to the group that owns the API key.")
	logCacheDir    = flag.String("log_cache_dir", "/tmp/buildbuddy/srctree", "Directory to keep fetched execution logs in, so that re-runs don't download them again. Empty disables the cache.")
	trace          = flag.Bool("trace", false, "Log when each batch, invocation, and log fetch starts and finishes, to see where the time goes.")
)

// generatedDirs are path components that mark a subtree as build outputs rather
// than sources. bazel-out is what actually shows up in execution logs; the rest
// are the convenience symlinks, included so that trees assembled from other
// sources don't sneak generated files in.
var generatedDirs = map[string]bool{
	"bazel-out":      true,
	"bazel-bin":      true,
	"bazel-genfiles": true,
	"bazel-testlogs": true,
}

// modifiedMarker is appended to files that were seen with more than one set of
// contents across the logs.
const modifiedMarker = "(!)"

// generatedRoot is the directory Bazel writes build outputs under. Its
// immediate children are configuration directories, e.g.
// bazel-out/linux_x86_64-opt-ST-9dfe9018e3d4/bin/..., which name the platform
// and compilation mode a file was built in.
const generatedRoot = "bazel-out"

// stripConfig removes the configuration directory from a generated path, so
// that the same file built for different platforms comes out the same:
//
//	bazel-out/linux_x86_64-opt-ST-9dfe9018e3d4/bin/cli/cli.a -> bazel-out/bin/cli/cli.a
//
// Paths that aren't generated are left alone: only bazel-out has a
// configuration directory, and the convenience symlinks beside it are already
// resolved to one.
func stripConfig(p string) string {
	rest, ok := strings.CutPrefix(p, generatedRoot+"/")
	if !ok {
		return p
	}
	_, after, ok := strings.Cut(rest, "/")
	if !ok {
		return p
	}
	return generatedRoot + "/" + after
}

// version is one observed set of contents for a path: a file digest or an
// unresolved symlink target.
type version struct {
	digest        *spb.Digest
	symlinkTarget string

	// firstLog is the index of the log this version was first seen in, and logs
	// holds every log it was seen in. Together they distinguish a file that
	// changed between invocations from a path that was logged inconsistently
	// within a single invocation.
	firstLog int
	logs     logSet
}

// logSet is a bit per log, so that a path can say exactly which builds it
// appeared in. A path in every one of a hundred builds costs two words.
type logSet []uint64

func (s logSet) set(logIdx int) logSet {
	word := logIdx / 64
	for len(s) <= word {
		s = append(s, 0)
	}
	s[word] |= 1 << uint(logIdx%64)
	return s
}

func (s logSet) has(logIdx int) bool {
	word := logIdx / 64
	return word < len(s) && s[word]&(1<<uint(logIdx%64)) != 0
}

// or adds everything in another set to this one.
func (s logSet) or(other logSet) logSet {
	for len(s) < len(other) {
		s = append(s, 0)
	}
	for i, w := range other {
		s[i] |= w
	}
	return s
}

// equal reports whether two sets hold the same logs. The words past either
// one's length are empty, so a longer set can still equal a shorter one.
func (s logSet) equal(other logSet) bool {
	for i := range max(len(s), len(other)) {
		var a, b uint64
		if i < len(s) {
			a = s[i]
		}
		if i < len(other) {
			b = other[i]
		}
		if a != b {
			return false
		}
	}
	return true
}

func (s logSet) count() int {
	n := 0
	for _, w := range s {
		n += bits.OnesCount64(w)
	}
	return n
}

// indexes lists the logs in the set, in merge order.
func (s logSet) indexes() []int {
	idx := make([]int, 0, s.count())
	for word, w := range s {
		for w != 0 {
			idx = append(idx, word*64+bits.TrailingZeros64(w))
			// Clear the lowest set bit.
			w &= w - 1
		}
	}
	return idx
}

// clone copies a version, log set and all, so that merging it into another tree
// can't reach back into this one.
func (v *version) clone() *version {
	return &version{
		digest:        v.digest,
		symlinkTarget: v.symlinkTarget,
		firstLog:      v.firstLog,
		logs:          slices.Clone(v.logs),
	}
}

// key uniquely identifies the contents of a version.
func (v *version) key() string {
	if v.symlinkTarget != "" {
		return "symlink:" + v.symlinkTarget
	}
	if v.digest.GetHash() == "" {
		// Bazel omits the digest of empty files.
		return "<empty>"
	}
	return v.digest.GetHash()
}

// entry is the payload of a leaf node: every distinct version of a file or
// symlink that the logs contained for its path, in the order they were seen.
type entry struct {
	versions []*version
}

func newEntry(v *version) *entry {
	v.logs = v.logs.set(v.firstLog)
	return &entry{versions: []*version{v}}
}

// logs are all the logs the path appeared in, whichever contents it had. The
// versions between them already know, so there's nothing to keep here.
func (e *entry) logs() logSet {
	var logs logSet
	for _, v := range e.versions {
		logs = logs.or(v.logs)
	}
	return logs
}

// modified reports whether the path was seen with different contents in
// different builds.
//
// Versions that turned up in exactly the same builds as each other aren't a
// change over time. With configurations merged that's one file built two ways,
// which nearly every generated file is; without them it's a log contradicting
// itself, which is reported as a conflict instead.
func (e *entry) modified() bool {
	for _, v := range e.versions[1:] {
		if !v.logs.equal(e.versions[0].logs) {
			return true
		}
	}
	return false
}

// inMultipleLogs reports whether the path appeared in more than one log, i.e.
// whether there was anything to compare.
func (e *entry) inMultipleLogs() bool {
	return e.logs().count() > 1
}

// add records a version of the path. It returns false if a conflicting version
// was already recorded for the same log, which is unexpected and reported
// rather than silently resolved.
func (e *entry) add(v *version) bool {
	sameLog := false
	for _, existing := range e.versions {
		if existing.key() == v.key() {
			existing.logs = existing.logs.set(v.firstLog)
			return true
		}
		// A log that already contributed a different version of this path is
		// contradicting itself.
		if existing.logs.has(v.firstLog) {
			sameLog = true
		}
	}
	v.logs = v.logs.set(v.firstLog)
	e.versions = append(e.versions, v)
	return !sameLog
}

// hash returns a content hash covering every version of the entry, so that a
// modified file changes the hash of the directories above it.
func (e *entry) hash() string {
	keys := make([]string, 0, len(e.versions))
	for _, v := range e.versions {
		keys = append(keys, v.key())
	}
	return strings.Join(keys, ",")
}

// node is a single file, symlink, or directory in the reconstructed tree.
// Directories have a nil entry; leaves have a nil children map.
type node struct {
	name     string
	entry    *entry
	children map[string]*node

	// cachedHash memoizes the recursive directory hash.
	cachedHash string

	// fileCount and modifiedCount are the number of files, and the number of
	// modified files, at or below this node. They are computed by
	// annotateCounts once the tree has been filtered, so that a collapsed
	// directory can still show how big it is and whether something under it
	// changed.
	fileCount     int
	modifiedCount int
}

func (n *node) isDir() bool {
	return n.entry == nil
}

// childDir returns the child directory with the given name, creating it if
// necessary, and reports whether it had to create it. It returns a nil node if
// a leaf with that name already exists.
func (n *node) childDir(name string) (*node, bool) {
	if c, ok := n.children[name]; ok {
		if !c.isDir() {
			return nil, false
		}
		return c, false
	}
	c := &node{name: name, children: map[string]*node{}}
	n.children[name] = c
	return c, true
}

// sortedChildren returns the node's children in name order.
func (n *node) sortedChildren() []*node {
	children := make([]*node, 0, len(n.children))
	for _, c := range n.children {
		children = append(children, c)
	}
	slices.SortFunc(children, func(a, b *node) int {
		return strings.Compare(a.name, b.name)
	})
	return children
}

// hash returns the node's content hash. For files it is the digest hash; for
// directories it is a hash over the names and hashes of the directory's
// contents, so that two trees can be compared a subtree at a time.
func (n *node) hash() string {
	if !n.isDir() {
		return n.entry.hash()
	}
	if n.cachedHash != "" {
		return n.cachedHash
	}
	n.cachedHash = hashChildren(n.sortedChildren(), (*node).hash)
	return n.cachedHash
}

// hashChildren hashes a directory's contents from its children's names and
// hashes. Callers decide which children count, so that a view hiding some of
// them can hash what it actually shows.
func hashChildren(children []*node, hashOf func(*node) string) string {
	h := sha256.New()
	for _, c := range children {
		kind := "f"
		if c.isDir() {
			kind = "d"
		}
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00", kind, c.name, hashOf(c))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// logInfo describes one of the merged logs, so that a file's versions can be
// traced back to the build that produced them.
type logInfo struct {
	// name identifies the log in messages: a file path, or a label naming the
	// invocation it was fetched from.
	name string
	// invocationID comes from the log's own Invocation entry, or from the
	// invocation it was fetched from. It's what links a version to a build.
	invocationID string
	// branch is only known for logs fetched from a server, which is where the
	// invocation metadata lives.
	branch string
	// cacheHost and instanceName describe where this build's blobs live. They
	// come from the bytestream URI of the log itself, which is the only place
	// the invocation's cache address appears, and are what let us look a
	// version's digest back up in the CAS.
	cacheHost    string
	instanceName string
	// updatedAtUsec is when the invocation last changed, for logs fetched from a
	// server. A log read from a file has no timestamp: the log itself doesn't
	// say when the build ran.
	updatedAtUsec int64
}

// when renders the log's timestamp, or "" if we don't know it.
func (l logInfo) when() string {
	if l.updatedAtUsec == 0 {
		return ""
	}
	return time.UnixMicro(l.updatedAtUsec).Format(time.DateTime)
}

// tree is the reconstructed source tree, rooted at the build's exec root, and
// merged across all of the logs that were read into it.
type tree struct {
	root *node

	// logs describes the logs that were merged, indexed by log index.
	logs []logInfo

	// targets is every target the logs ran, by label. See targets.go for why
	// this is all the merge keeps about them.
	targets map[string]*targetSteps

	// repo is the repo the logs were fetched for, if they were fetched.
	repo string

	hashFunction string

	// conflicts records paths that a single log recorded twice with differing
	// contents, or that were added both as a file and as a directory. These
	// aren't expected, so they're reported rather than silently resolved.
	// quiet turns that off, for a tree built by merging paths that are meant to
	// collide.
	conflicts []string
	quiet     bool

	numFiles int

	// shownFiles and shownDirs count the paths that will survive filtering,
	// maintained as entries are added. They let a caller watch the tree grow
	// without filtering a copy of it after every log.
	shownFiles int
	shownDirs  int

	// filter is the output filter the shown counts are measured against.
	filter filterOptions
}

func newTree() *tree {
	return &tree{
		root:    &node{children: map[string]*node{}},
		targets: map[string]*targetSteps{},
	}
}

// shown reports whether a path survives the output filter. It has to agree with
// shouldPrune, which does the same job on the finished tree.
func (t *tree) shown(parts []string) bool {
	if !t.filter.includeGenerated {
		for _, part := range parts {
			if generatedDirs[part] {
				return false
			}
		}
	}
	if !t.filter.includeExternal && parts[0] == "external" {
		return false
	}
	return true
}

func (t *tree) conflictf(format string, args ...any) {
	if t.quiet {
		return
	}
	t.conflicts = append(t.conflicts, fmt.Sprintf(format, args...))
}

// stripConfigs returns a copy of the tree with the configuration directory
// taken out of every generated path, so that the same file built for several
// platforms is one file. Everything else about the tree - which logs it was
// merged from, which targets ran - is shared, since none of it depends on the
// paths.
func stripConfigs(src *tree) *tree {
	out := newTree()
	out.logs = src.logs
	out.targets = src.targets
	out.repo = src.repo
	out.hashFunction = src.hashFunction
	out.filter = src.filter
	// Whatever the logs disagreed about is still worth reporting; what merging
	// itself collides is not.
	out.conflicts = src.conflicts
	// Two configurations of a file disagreeing about its contents is the whole
	// point of merging them, not a log contradicting itself.
	out.quiet = true
	forEachFile(src.root, func(p string, e *entry) {
		stripped := stripConfig(p)
		for _, v := range e.versions {
			// A copy: merging pools the versions' log sets, and the tree these
			// came from is still being shown from.
			out.add(stripped, v.clone())
		}
	})
	return out
}

// log returns the log with the given index.
func (t *tree) log(logIdx int) logInfo {
	if logIdx < 0 || logIdx >= len(t.logs) {
		return logInfo{name: "?"}
	}
	return t.logs[logIdx]
}

// logFile returns where the log with the given index can be read again, for
// the deeper parse the info view needs, or "" if we no longer have it.
func (t *tree) logFile(logIdx int) string {
	src := t.log(logIdx)
	// A fetched log lives in the local cache, if it's enabled and the write
	// went through. A log named on the command line is its own file.
	var candidates []string
	if cacheableInvocationID(src.invocationID) {
		candidates = append(candidates, cachedLogPath(src.invocationID))
	}
	for _, candidate := range append(candidates, src.name) {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return ""
}

// logName returns a short name for the log with the given index, for use in
// messages.
func (t *tree) logName(logIdx int) string {
	return path.Base(t.log(logIdx).name)
}

// invocationIDs returns the invocation ID of every log that had one.
func (t *tree) invocationIDs() []string {
	ids := make([]string, 0, len(t.logs))
	for _, l := range t.logs {
		if l.invocationID != "" {
			ids = append(ids, l.invocationID)
		}
	}
	return ids
}

// add inserts or updates the leaf at the given exec-root-relative path.
func (t *tree) add(p string, v *version) {
	parts := splitPath(p)
	if len(parts) == 0 {
		t.conflictf("ignoring entry with empty path")
		return
	}
	shown := t.shown(parts)
	n := t.root
	for i, part := range parts[:len(parts)-1] {
		child, created := n.childDir(part)
		if child == nil {
			t.conflictf("%s: %s is a file, not a directory", p, strings.Join(parts[:i+1], "/"))
			return
		}
		// Every file under a given top-level directory is either shown or
		// pruned as a group, so a directory on the way to a shown file is
		// itself shown.
		if created && shown {
			t.shownDirs++
		}
		n = child
	}
	name := parts[len(parts)-1]
	existing, ok := n.children[name]
	if !ok {
		n.children[name] = &node{name: name, entry: newEntry(v)}
		t.numFiles++
		if shown {
			t.shownFiles++
		}
		return
	}
	if existing.isDir() {
		t.conflictf("%s: already present as a directory", p)
		return
	}
	if !existing.entry.add(v) {
		t.conflictf("%s: present with two different digests within %s", p, t.logName(v.firstLog))
	}
}

// find returns the node at an exec-root-relative path, or nil if the logs never
// mentioned it.
func (t *tree) find(p string) *node {
	n := t.root
	for _, part := range splitPath(p) {
		if !n.isDir() {
			return nil
		}
		if n = n.children[part]; n == nil {
			return nil
		}
	}
	if n == t.root {
		return nil
	}
	return n
}

// splitPath splits an exec-root-relative path into its components, dropping
// empty ones so that a stray leading or doubled slash doesn't create an
// unnamed directory.
func splitPath(p string) []string {
	parts := strings.Split(p, "/")
	out := parts[:0]
	for _, part := range parts {
		if part != "" && part != "." {
			out = append(out, part)
		}
	}
	return out
}

// ExecLogEntry field numbers. Only these five carry paths; the rest of the
// oneof - input sets, symlink entry sets, runfiles trees - only reference files
// that appear as entries of their own, and are usually most of a log's bytes.
// They're skipped without being unmarshalled at all. Spawns are read for their
// labels alone; see fieldSpawn.
const (
	fieldInvocation        = 2
	fieldFile              = 3
	fieldDirectory         = 4
	fieldUnresolvedSymlink = 5
	fieldSymlinkAction     = 8
)

// Spawn field numbers for the parts that name a build step and say where it
// ran. The rest of a spawn is skipped in the same way as the entries above.
const (
	fieldSpawnPlatform    = 3
	fieldSpawnTargetLabel = 7
	fieldSpawnMnemonic    = 8
)

// Platform field numbers, down to the two properties that name an os and an
// architecture.
const (
	fieldPlatformProperties = 1
	fieldPropertyName       = 1
	fieldPropertyValue      = 2
)

// The platform properties that say what a spawn ran on. The rest of them - the
// container image, the pool - say where.
const (
	osFamilyProperty = "OSFamily"
	archProperty     = "Arch"
)

// maxEntrySize caps the length prefix we're willing to allocate for, so a
// corrupt log can't ask for an absurd buffer.
const maxEntrySize = 1 << 31

// zstdMagic starts every compressed log. Bazel writes them compressed; the
// local cache keeps them expanded.
var zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

// decodeBatchSize is how many decoded entries are handed over at a time when a
// log is decoded ahead of the merge.
const decodeBatchSize = 2048

// decodedEntry is one update a log makes to the tree. Decoding is the expensive
// half of a merge and doesn't need the tree, so it runs on its own goroutine;
// entries carry everything the merge needs except the log index, which isn't
// known until the log's turn comes around.
type decodedEntry struct {
	// path and version describe a file.
	path    string
	version *version

	// step names a build step the log ran, and where it ran. It's the whole of a
	// spawn entry: what the spawn consumed and produced is left encoded.
	step spawnStep

	// hashFunction and invocationID are what the log's Invocation entry said.
	// An entry with none of the fields above is that entry.
	hashFunction string
	invocationID string
}

// spawnStep is a build step as a spawn entry names it: which target's action it
// was, and what it ran on.
type spawnStep struct {
	target   string
	mnemonic string
	// platform is the os and arch the spawn's platform named, e.g.
	// "linux_amd64", or "" for a spawn that didn't say.
	platform string
}

// named reports whether the entry says anything about a step at all.
func (s spawnStep) named() bool {
	return s.target != "" || s.mnemonic != ""
}

// labelSet interns the strings a log says over and over: a build runs tens of
// thousands of spawns, but they share a few thousand target labels and a
// handful of mnemonics between them. Looking a byte slice up in a map doesn't
// allocate, so naming a label we've already seen costs nothing.
type labelSet map[string]string

func (s labelSet) intern(b []byte) string {
	if v, ok := s[string(b)]; ok {
		return v
	}
	v := string(b)
	s[v] = v
	return v
}

// platformSet memoizes the os and arch an encoded Platform names, keyed by the
// encoded bytes. A build runs its tens of thousands of spawns on a handful of
// platforms, so unpacking the properties once per distinct platform is far
// cheaper than once per spawn.
type platformSet map[string]string

func (s platformSet) name(payload []byte) (string, error) {
	if v, ok := s[string(payload)]; ok {
		return v, nil
	}
	v, err := platformName(payload)
	if err != nil {
		return "", err
	}
	s[string(payload)] = v
	return v, nil
}

// parse reads a compact execution log from disk and merges every file it
// mentions into the tree.
func (t *tree) parse(logPath string) error {
	return t.parseFile(logInfo{name: logPath}, logPath)
}

// parseFile merges a log from disk, compressed or not.
func (t *tree) parseFile(src logInfo, logPath string) error {
	logIdx := t.beginLog(src)
	return decodeFile(logPath, func(batch []decodedEntry) error {
		t.applyEntries(logIdx, batch)
		return nil
	})
}

// parseBytes merges an in-memory compact execution log into the tree, as it
// comes off the wire: still zstd-compressed.
func (t *tree) parseBytes(src logInfo, b []byte) error {
	logIdx := t.beginLog(src)
	return decodeBytes(b, func(batch []decodedEntry) error {
		t.applyEntries(logIdx, batch)
		return nil
	})
}

// beginLog reserves the next log index, which fixes the order versions are
// attributed to builds in.
func (t *tree) beginLog(src logInfo) int {
	logIdx := len(t.logs)
	t.logs = append(t.logs, src)
	return logIdx
}

// applyEntries merges a decoded batch. This is the part that has to stay on one
// goroutine: it's the only thing that touches the tree.
func (t *tree) applyEntries(logIdx int, batch []decodedEntry) {
	for _, e := range batch {
		switch {
		case e.version != nil:
			e.version.firstLog = logIdx
			t.add(e.path, e.version)
		case e.step.named():
			t.addStep(logIdx, e.step)
		default:
			if t.hashFunction != "" && t.hashFunction != e.hashFunction {
				t.conflictf("%s uses hash function %s, but an earlier log used %s: digests aren't comparable",
					t.logName(logIdx), e.hashFunction, t.hashFunction)
			}
			t.hashFunction = e.hashFunction
			// The log knows its own invocation ID, which is all a local file
			// has to go on. A log fetched from a server already has one, and
			// they agree.
			if t.logs[logIdx].invocationID == "" {
				t.logs[logIdx].invocationID = e.invocationID
			}
		}
	}
}

// decodeFile decodes a log from disk, compressed or not.
func decodeFile(logPath string, emit func([]decodedEntry) error) error {
	f, err := os.Open(logPath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<20)
	magic, err := r.Peek(len(zstdMagic))
	if err != nil && err != io.EOF {
		return err
	}
	if bytes.Equal(magic, zstdMagic) {
		return decodeCompressed(r, emit)
	}
	return decodeLog(r, emit)
}

// decodeBytes decodes a compressed in-memory log.
func decodeBytes(b []byte, emit func([]decodedEntry) error) error {
	return decodeCompressed(bytes.NewReader(b), emit)
}

func decodeCompressed(in io.Reader, emit func([]decodedEntry) error) error {
	zr, err := zstd.NewReader(in)
	if err != nil {
		return fmt.Errorf("open zstd stream: %s", err)
	}
	defer zr.Close()
	return decodeLog(zr, emit)
}

// decodeLog reads a decompressed log and hands its tree updates to emit in
// batches. It never touches a tree, so it can run ahead of the merge.
func decodeLog(in io.Reader, emit func([]decodedEntry) error) error {
	r := bufio.NewReaderSize(in, 1<<20)
	batch := make([]decodedEntry, 0, decodeBatchSize)
	labels, platforms := labelSet{}, platformSet{}
	var buf []byte
	for {
		// Entries are length-delimited, and we read them one at a time so that
		// the uninteresting ones can be skipped without being parsed.
		size, err := binary.ReadUvarint(r)
		if err != nil {
			if err == io.EOF {
				if len(batch) > 0 {
					return emit(batch)
				}
				return nil
			}
			return fmt.Errorf("read execution log entry: %s", err)
		}
		if size > maxEntrySize {
			return fmt.Errorf("read execution log entry: entry of %d bytes is implausible", size)
		}
		if uint64(cap(buf)) < size {
			buf = make([]byte, size)
		}
		buf = buf[:size]
		if _, err := io.ReadFull(r, buf); err != nil {
			return fmt.Errorf("read execution log entry: %s", err)
		}
		if batch, err = appendEntry(batch, buf, labels, platforms); err != nil {
			return fmt.Errorf("read execution log entry: %s", err)
		}
		if len(batch) >= decodeBatchSize {
			if err := emit(batch); err != nil {
				return err
			}
			// The batch now belongs to whoever we handed it to.
			batch = make([]decodedEntry, 0, decodeBatchSize)
		}
	}
}

// appendEntry walks the top-level fields of an encoded ExecLogEntry,
// unmarshalling only the payloads that carry paths.
func appendEntry(dst []decodedEntry, b []byte, labels labelSet, platforms platformSet) ([]decodedEntry, error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return dst, protowire.ParseError(n)
		}
		b = b[n:]
		// Every field of the payload oneof is a message; the entry ID and
		// anything else scalar is skipped.
		if typ != protowire.BytesType {
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return dst, protowire.ParseError(n)
			}
			b = b[n:]
			continue
		}
		payload, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return dst, protowire.ParseError(n)
		}
		b = b[n:]

		var err error
		if dst, err = appendPayload(dst, num, payload, labels, platforms); err != nil {
			return dst, err
		}
	}
	return dst, nil
}

func appendPayload(dst []decodedEntry, field protowire.Number, payload []byte, labels labelSet, platforms platformSet) ([]decodedEntry, error) {
	switch field {
	case fieldInvocation:
		inv := &spb.ExecLogEntry_Invocation{}
		if err := inv.UnmarshalVT(payload); err != nil {
			return dst, err
		}
		return append(dst, decodedEntry{
			hashFunction: inv.GetHashFunctionName(),
			invocationID: inv.GetId(),
		}), nil
	case fieldFile:
		f := &spb.ExecLogEntry_File{}
		if err := f.UnmarshalVT(payload); err != nil {
			return dst, err
		}
		return append(dst, decodedEntry{path: f.GetPath(), version: &version{digest: f.GetDigest()}}), nil
	case fieldDirectory:
		// A source directory, fileset tree, or tree artifact. Its files are
		// listed with paths relative to the directory.
		d := &spb.ExecLogEntry_Directory{}
		if err := d.UnmarshalVT(payload); err != nil {
			return dst, err
		}
		for _, f := range d.GetFiles() {
			dst = append(dst, decodedEntry{
				path:    path.Join(d.GetPath(), f.GetPath()),
				version: &version{digest: f.GetDigest()},
			})
		}
		return dst, nil
	case fieldUnresolvedSymlink:
		s := &spb.ExecLogEntry_UnresolvedSymlink{}
		if err := s.UnmarshalVT(payload); err != nil {
			return dst, err
		}
		return append(dst, decodedEntry{
			path:    s.GetPath(),
			version: &version{symlinkTarget: s.GetTargetPath()},
		}), nil
	case fieldSymlinkAction:
		// The output of a symlink action is always a generated file; its input
		// shows up as a File entry of its own if it's used elsewhere.
		a := &spb.ExecLogEntry_SymlinkAction{}
		if err := a.UnmarshalVT(payload); err != nil {
			return dst, err
		}
		return append(dst, decodedEntry{
			path:    a.GetOutputPath(),
			version: &version{symlinkTarget: a.GetInputPath()},
		}), nil
	case fieldSpawn:
		// Only what names the step. What a spawn consumed and produced is
		// expensive to unpack and is only ever wanted for one target at a time,
		// so the targets view reads that back off the log when it's asked for.
		step, err := readSpawnStep(payload, labels, platforms)
		if err != nil {
			return dst, err
		}
		if !step.named() {
			return dst, nil
		}
		return append(dst, decodedEntry{step: step}), nil
	default:
		// Input sets, symlink entry sets and runfiles trees only reference files
		// that appear as entries of their own, so they're left encoded.
		return dst, nil
	}
}

// readSpawnStep pulls the target label, the mnemonic and the platform out of an
// encoded Spawn. Everything else about it - the command line, the environment,
// the inputs - is most of its bytes and none of its identity, so it's skipped
// unread.
func readSpawnStep(payload []byte, labels labelSet, platforms platformSet) (spawnStep, error) {
	var step spawnStep
	for len(payload) > 0 {
		num, typ, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return spawnStep{}, protowire.ParseError(n)
		}
		payload = payload[n:]
		wanted := num == fieldSpawnTargetLabel || num == fieldSpawnMnemonic || num == fieldSpawnPlatform
		if typ != protowire.BytesType || !wanted {
			if n = protowire.ConsumeFieldValue(num, typ, payload); n < 0 {
				return spawnStep{}, protowire.ParseError(n)
			}
			payload = payload[n:]
			continue
		}
		b, n := protowire.ConsumeBytes(payload)
		if n < 0 {
			return spawnStep{}, protowire.ParseError(n)
		}
		payload = payload[n:]
		switch num {
		case fieldSpawnTargetLabel:
			step.target = labels.intern(b)
		case fieldSpawnMnemonic:
			step.mnemonic = labels.intern(b)
		case fieldSpawnPlatform:
			name, err := platforms.name(b)
			if err != nil {
				return spawnStep{}, err
			}
			step.platform = name
		default:
			// Only the three fields above name the step.
		}
		// The platform comes before the labels, so by the time both of those are
		// in hand there's nothing left in the spawn that says which step it was.
		if step.target != "" && step.mnemonic != "" {
			break
		}
	}
	return step, nil
}

// platformName renders what a spawn's platform says it ran on, as
// "<os>_<arch>", e.g. "linux_amd64". Platforms that name neither have nothing
// to say about it.
func platformName(payload []byte) (string, error) {
	var osFamily, arch string
	for len(payload) > 0 {
		num, typ, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return "", protowire.ParseError(n)
		}
		payload = payload[n:]
		if num != fieldPlatformProperties || typ != protowire.BytesType {
			if n = protowire.ConsumeFieldValue(num, typ, payload); n < 0 {
				return "", protowire.ParseError(n)
			}
			payload = payload[n:]
			continue
		}
		property, n := protowire.ConsumeBytes(payload)
		if n < 0 {
			return "", protowire.ParseError(n)
		}
		payload = payload[n:]
		name, value, err := platformProperty(property)
		if err != nil {
			return "", err
		}
		switch name {
		case osFamilyProperty:
			osFamily = value
		case archProperty:
			arch = value
		}
	}
	switch {
	case osFamily == "" && arch == "":
		return "", nil
	case osFamily == "":
		return strings.ToLower(arch), nil
	case arch == "":
		return strings.ToLower(osFamily), nil
	}
	return strings.ToLower(osFamily + "_" + arch), nil
}

// platformProperty reads one name/value pair of a platform.
func platformProperty(payload []byte) (string, string, error) {
	var name, value string
	for len(payload) > 0 {
		num, typ, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		payload = payload[n:]
		if typ != protowire.BytesType {
			if n = protowire.ConsumeFieldValue(num, typ, payload); n < 0 {
				return "", "", protowire.ParseError(n)
			}
			payload = payload[n:]
			continue
		}
		b, n := protowire.ConsumeBytes(payload)
		if n < 0 {
			return "", "", protowire.ParseError(n)
		}
		payload = payload[n:]
		switch num {
		case fieldPropertyName:
			name = string(b)
		case fieldPropertyValue:
			value = string(b)
		default:
			// A property is a name and a value; there is nothing else.
		}
	}
	return name, value, nil
}

// filterOptions controls which parts of the reconstructed tree are shown, and
// how.
type filterOptions struct {
	includeGenerated bool
	includeExternal  bool
	// mergeConfigs shows generated files built in different configurations as
	// one file. Unlike the other two it changes the paths themselves, so it's
	// applied by rebuilding the tree rather than by hiding parts of it.
	mergeConfigs bool
}

// filter prunes generated (and optionally external) subtrees, along with any
// directories left empty as a result. It returns false if the node itself
// should be dropped.
//
// This runs as a last step, after the whole tree has been built, so that the
// generated files remain available to future callers that want them.
func filter(n *node, opts filterOptions, depth int) bool {
	if !n.isDir() {
		return true
	}
	// Any hash computed before filtering covers nodes that may be about to go
	// away.
	n.cachedHash = ""
	for name, c := range n.children {
		if shouldPrune(name, c, opts, depth) || !filter(c, opts, depth+1) {
			delete(n.children, name)
		}
	}
	// Keep the root even if it ends up empty, so we always print something.
	return depth == 0 || len(n.children) > 0
}

func shouldPrune(name string, n *node, opts filterOptions, depth int) bool {
	if !n.isDir() {
		return false
	}
	if !opts.includeGenerated && generatedDirs[name] {
		return true
	}
	if !opts.includeExternal && depth == 0 && name == "external" {
		return true
	}
	return false
}

// annotateCounts records, for every directory, how many files and how many
// modified files it contains. Call it after filtering: files that were pruned
// shouldn't make their parents look modified.
func annotateCounts(n *node) (int, int) {
	if !n.isDir() {
		n.fileCount = 1
		if n.entry.modified() {
			n.modifiedCount = 1
		}
		return n.fileCount, n.modifiedCount
	}
	files, modified := 0, 0
	for _, c := range n.children {
		cf, cm := annotateCounts(c)
		files += cf
		modified += cm
	}
	n.fileCount, n.modifiedCount = files, modified
	return files, modified
}

// printOptions controls how the tree is rendered.
type printOptions struct {
	hashLength int
	dirHashes  bool
}

func (o printOptions) formatHash(h string) string {
	if o.hashLength > 0 && len(h) > o.hashLength {
		return h[:o.hashLength]
	}
	return h
}

// describe renders the contents of a leaf: every distinct version it was seen
// with, followed by the modified marker if there was more than one.
func (o printOptions) describe(e *entry) string {
	descriptions := make([]string, 0, len(e.versions))
	for _, v := range e.versions {
		if v.symlinkTarget != "" {
			descriptions = append(descriptions, "-> "+v.symlinkTarget)
			continue
		}
		hash := "<empty>"
		if v.digest.GetHash() != "" {
			hash = o.formatHash(v.digest.GetHash())
		}
		descriptions = append(descriptions, fmt.Sprintf("%s  %d", hash, v.digest.GetSizeBytes()))
	}
	s := strings.Join(descriptions, ", ")
	if e.modified() {
		s += " " + modifiedMarker
	}
	return s
}

// shownTree is the tree to show under the given options: the one that was
// merged, or a copy with the configuration directories taken out.
func shownTree(t *tree, opts filterOptions) *tree {
	if !opts.mergeConfigs {
		return t
	}
	return stripConfigs(t)
}

func printTree(w io.Writer, t *tree, opts printOptions) {
	annotateCounts(t.root)
	fmt.Fprintf(w, ".")
	if opts.dirHashes {
		fmt.Fprintf(w, "  %s", opts.formatHash(t.root.hash()))
	}
	if t.root.modifiedCount > 0 {
		fmt.Fprintf(w, " %s", modifiedMarker)
	}
	fmt.Fprintln(w)
	printChildren(w, t.root, "", opts)
}

func printChildren(w io.Writer, n *node, prefix string, opts printOptions) {
	children := n.sortedChildren()
	for i, c := range children {
		branch, childPrefix := "├── ", prefix+"│   "
		if i == len(children)-1 {
			branch, childPrefix = "└── ", prefix+"    "
		}
		fmt.Fprintf(w, "%s%s%s", prefix, branch, c.name)
		if c.isDir() {
			fmt.Fprint(w, "/")
			if opts.dirHashes {
				fmt.Fprintf(w, "  %s", opts.formatHash(c.hash()))
			}
			// Mark directories containing modified files, so a change is
			// visible without reading every line under them.
			if c.modifiedCount > 0 {
				fmt.Fprintf(w, " %s", modifiedMarker)
			}
			fmt.Fprintln(w)
			printChildren(w, c, childPrefix, opts)
			continue
		}
		fmt.Fprintf(w, "  %s\n", opts.describe(c.entry))
	}
}

// printFlat prints one line per file, sorted by path, which makes it easy to
// diff the output of two runs with ordinary text tools.
func printFlat(w io.Writer, t *tree, opts printOptions) {
	forEachFile(t.root, func(p string, e *entry) {
		fmt.Fprintf(w, "%s\t%s\n", p, opts.describe(e))
	})
}

// forEachFile calls fn for every leaf in the tree, in path order.
func forEachFile(n *node, fn func(path string, e *entry)) {
	var walk func(n *node, prefix string)
	walk = func(n *node, prefix string) {
		for _, c := range n.sortedChildren() {
			p := prefix + c.name
			if c.isDir() {
				walk(c, p+"/")
				continue
			}
			fn(p, c.entry)
		}
	}
	walk(n, "")
}

// treeStats summarizes the files that are actually shown in the output.
type treeStats struct {
	files int
	// comparable is the number of files that appeared in more than one log, and
	// so could be compared at all.
	comparable int
	// modified is the number of files that appeared with more than one set of
	// contents.
	modified int
}

// stats counts the shown files. Files that appear in only one log are not
// counted as added or removed: a compact execution log only covers the files
// the invocation's actions touched, so their absence from another log says
// nothing about whether they exist in that build's source tree.
func stats(n *node) treeStats {
	var s treeStats
	forEachFile(n, func(_ string, e *entry) {
		s.files++
		if e.inMultipleLogs() {
			s.comparable++
		}
		if e.modified() {
			s.modified++
		}
	})
	return s
}

// readLogs merges the logs named on the command line, or, if an API key was
// given, the logs of the most recent CI invocations of a repo built on the
// BuildBuddy server.
// readLogs merges the logs named on the command line into the tree, returning
// no client. The API path is handled by run, which keeps its client open for
// the UI.
func readLogs(t *tree) error {
	if flag.NArg() == 0 {
		return fmt.Errorf("usage: srctree [flags] <compact_execution_log.binpb.zst>...\n" +
			"   or: srctree [flags] --api_key=<key> [--target=<grpc target>]")
	}
	// Decode a group of logs at a time in the background, merging them in the
	// order they were named. The group is what bounds how many decoders run at
	// once, and so how much decoded-but-unmerged data is in memory.
	for group := range slices.Chunk(flag.Args(), maxConcurrentDownloads) {
		results := make([]downloadResult, len(group))
		for i, logPath := range group {
			results[i] = downloadResult{path: logPath}
		}
		if err := mergeGroup(t, group, results); err != nil {
			return err
		}
	}
	return nil
}

// mergeGroup decodes a group of logs concurrently and merges them in order.
func mergeGroup(t *tree, paths []string, results []downloadResult) error {
	ctx, cancelDecoding := context.WithCancel(context.Background())
	defer cancelDecoding()
	streams := startDecoding(ctx, results)
	for i, logPath := range paths {
		if err := streams[i].merge(t, logInfo{name: logPath}); err != nil {
			return fmt.Errorf("%s: %s", logPath, err)
		}
	}
	return nil
}

func run() error {
	ctx := context.Background()
	t := newTree()
	t.filter = filterOptions{
		includeGenerated: *includeGenerated,
		includeExternal:  *includeExternal,
		mergeConfigs:     *mergeConfigs,
	}

	// The client outlives the fetch: the UI uses it to check whether a file's
	// versions are still in the cache.
	var client *apiClient
	switch {
	case *apiKey != "" && flag.NArg() > 0:
		return fmt.Errorf("pass either log files or --api_key, not both")
	case *apiKey != "":
		c, conn, err := dialAPI()
		if err != nil {
			return err
		}
		defer conn.Close()
		client = c
		if err := fetchAndMerge(ctx, client, t); err != nil {
			return err
		}
	default:
		if err := readLogs(t); err != nil {
			return err
		}
	}
	totalFiles := t.numFiles

	printOpts := printOptions{hashLength: *hashLength, dirHashes: *dirHashes}
	outputFormat := *format
	if outputFormat == "" {
		// Browsing is the friendlier default, but only when we're not being
		// piped somewhere.
		outputFormat = "tree"
		if term.IsTerminal(int(os.Stdout.Fd())) {
			outputFormat = "ui"
		}
	}
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	switch outputFormat {
	case "ui":
		// The UI filters as it renders, so that generated and external files
		// can be toggled without rebuilding anything, and reports back what was
		// showing when it exited. The summary below is printed once the UI
		// exits, so it survives in the scrollback rather than being wiped with
		// the alternate screen.
		// Careful: a nil *apiClient in an interface isn't a nil interface, and
		// the UI checks for nil before asking about the cache.
		var checker blobChecker
		if client != nil {
			checker = client
		}
		shown, err := runUI(t, printOpts, t.filter, checker)
		if err != nil {
			return err
		}
		// The summary below describes what was on screen when the UI exited,
		// which includes whichever way the user left the options.
		t = shownTree(t, shown)
		filter(t.root, shown, 0)
	case "tree":
		t = shownTree(t, t.filter)
		filter(t.root, t.filter, 0)
		printTree(w, t, printOpts)
	case "flat":
		t = shownTree(t, t.filter)
		filter(t.root, t.filter, 0)
		printFlat(w, t, printOpts)
	default:
		return fmt.Errorf("unknown --format %q: expected 'ui', 'tree', or 'flat'", *format)
	}
	w.Flush()

	for _, c := range t.conflicts {
		log.Warningf("%s", c)
	}
	if *listModified {
		forEachFile(t.root, func(p string, e *entry) {
			if !e.modified() {
				return
			}
			versions := make([]string, 0, len(e.versions))
			for _, v := range e.versions {
				versions = append(versions, fmt.Sprintf("%s (first in %s)", printOpts.formatHash(v.key()), t.logName(v.firstLog)))
			}
			log.Infof("modified: %s: %s", p, strings.Join(versions, ", "))
		})
	}
	s := stats(t.root)
	log.Infof("merged %d log(s) (%s)", len(t.logs), t.hashFunction)
	if *listInvocations {
		log.Infof("invocations: %s", strings.Join(t.invocationIDs(), ", "))
	}
	log.Infof("%d paths in the logs, %d shown", totalFiles, s.files)
	log.Infof("%d of the %d shown files appeared in more than one log; %d of those were modified",
		s.comparable, s.files, s.modified)
	return nil
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("%s", err)
	}
}
