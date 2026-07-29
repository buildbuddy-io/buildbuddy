package main

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	spb "github.com/buildbuddy-io/buildbuddy/proto/spawn"
)

// labelsOf names a list of targets, in the order they were given.
func labelsOf(targets []*targetSteps) []string {
	labels := make([]string, 0, len(targets))
	for _, ts := range targets {
		labels = append(labels, ts.label)
	}
	return labels
}

func namesOf(mnemonics []*mnemonicSteps) []string {
	names := make([]string, 0, len(mnemonics))
	for _, ms := range mnemonics {
		names = append(names, ms.name)
	}
	return names
}

// A target runs many spawns per build, but that's one appearance, not many.
func TestAddStepRecordsEachLogOnce(t *testing.T) {
	tr := newTree()
	tr.logs = []logInfo{{name: "first"}, {name: "second"}}
	tr.addStep(0, spawnStep{target: "//a:a", mnemonic: "GoCompilePkg", platform: "linux_amd64"})
	tr.addStep(0, spawnStep{target: "//a:a", mnemonic: "GoCompilePkg", platform: "linux_amd64"})
	tr.addStep(0, spawnStep{target: "//a:a", mnemonic: "GoLink"})
	tr.addStep(1, spawnStep{target: "//a:a", mnemonic: "GoCompilePkg", platform: "darwin_arm64"})
	tr.addStep(1, spawnStep{target: "//b:b", mnemonic: "GoLink"})

	a := tr.targets["//a:a"]
	require.NotNil(t, a)
	assert.Equal(t, []int{0, 1}, a.logs)
	assert.Equal(t, []int{0, 1}, a.mnemonics["GoCompilePkg"].logs)
	assert.Equal(t, []int{0}, a.mnemonics["GoLink"].logs)

	// Two builds ran the compile, one on each platform, however many spawns
	// each of them took.
	assert.Equal(t, []platformTally{
		{platform: "darwin_arm64", builds: 1},
		{platform: "linux_amd64", builds: 1},
	}, a.mnemonics["GoCompilePkg"].sortedPlatforms())
	// A spawn that didn't say where it ran isn't counted anywhere.
	assert.Empty(t, a.mnemonics["GoLink"].sortedPlatforms())

	// Most builds first, in both lists.
	assert.Equal(t, []string{"//a:a", "//b:b"}, labelsOf(tr.sortedTargets()))
	assert.Equal(t, []string{"GoCompilePkg", "GoLink"}, namesOf(a.sortedMnemonics()))
}

// The breakdown counts builds, not spawns, and lists the busiest platform
// first.
func TestPlatformTallies(t *testing.T) {
	tr := newTree()
	tr.logs = make([]logInfo, 4)
	for logIdx := range 3 {
		// Two spawns of the same step in the same build, on the same platform.
		tr.addStep(logIdx, spawnStep{target: "//a:a", mnemonic: "GoLink", platform: "linux_amd64"})
		tr.addStep(logIdx, spawnStep{target: "//a:a", mnemonic: "GoLink", platform: "linux_amd64"})
	}
	// A build that ran it on two platforms counts under each.
	tr.addStep(3, spawnStep{target: "//a:a", mnemonic: "GoLink", platform: "darwin_arm64"})
	tr.addStep(3, spawnStep{target: "//a:a", mnemonic: "GoLink", platform: "linux_amd64"})

	step := tr.targets["//a:a"].mnemonics["GoLink"]
	assert.Equal(t, []int{0, 1, 2, 3}, step.logs)
	assert.Equal(t, []platformTally{
		{platform: "linux_amd64", builds: 4},
		{platform: "darwin_arm64", builds: 1},
	}, step.sortedPlatforms())
	assert.Equal(t, "(linux_amd64: 4, darwin_arm64: 1)", platformBreakdown(step.sortedPlatforms()))
	assert.Empty(t, platformBreakdown(nil))
}

// The merge picks up the targets of a real log, alongside its files.
func TestMergeCollectsTargets(t *testing.T) {
	tr := newTree()
	require.NoError(t, tr.parse(log1))
	require.NotEmpty(t, tr.targets)

	// Whatever compiled a source file in the log is one of them, with the step
	// that did the compiling.
	g, err := loadSpawnGraph(log1)
	require.NoError(t, err)
	_, consumers := g.stepsFor("cli/printlog/compact/compact.go")
	require.NotEmpty(t, consumers)
	step := consumers[0]

	ts := tr.targets[step.target]
	require.NotNil(t, ts, "the merge should have seen %s", step.target)
	assert.Equal(t, []int{0}, ts.logs)
	require.Contains(t, ts.mnemonics, step.mnemonic)
	assert.Equal(t, []int{0}, ts.mnemonics[step.mnemonic].logs)

	// There's only one log, so nothing can have run in more than one.
	for _, ts := range tr.sortedTargets() {
		assert.Equal(t, []int{0}, ts.logs, ts.label)
	}
}

func TestMergeCountsTargetsAcrossLogs(t *testing.T) {
	tr := newTree()
	require.NoError(t, tr.parse(log1))
	require.NoError(t, tr.parse(log2))

	targets := tr.sortedTargets()
	require.NotEmpty(t, targets)
	// Two builds of the same repo, so the most-seen target ran in both.
	assert.Equal(t, []int{0, 1}, targets[0].logs)
	assert.True(t, slices.IsSortedFunc(targets, func(a, b *targetSteps) int {
		return len(b.logs) - len(a.logs)
	}), "targets should be listed most builds first")
}

// Some actions - the workspace status command, say - have no target label, and
// are still worth listing under their mnemonic.
func TestMergeReadsSpawnsWithoutATarget(t *testing.T) {
	logBytes := graphLog(t, &spb.ExecLogEntry{
		Type: &spb.ExecLogEntry_Spawn_{Spawn: &spb.ExecLogEntry_Spawn{
			Mnemonic: "BazelWorkspaceStatusAction",
		}},
	})
	tr := newTree()
	require.NoError(t, tr.parseBytes(logInfo{name: "test"}, logBytes))

	ts := tr.targets[""]
	require.NotNil(t, ts)
	assert.Contains(t, ts.mnemonics, "BazelWorkspaceStatusAction")
}

// A step is read straight off the wire, so the reader has to pick its three
// fields out of a spawn that carries plenty of others.
func TestReadSpawnStep(t *testing.T) {
	spawn := &spb.ExecLogEntry_Spawn{
		Args: []string{"/bin/true", "--flag"},
		Platform: &spb.Platform{Properties: []*spb.Platform_Property{
			{Name: "container-image", Value: "docker://example"},
			{Name: "OSFamily", Value: "Linux"},
			{Name: "Arch", Value: "amd64"},
		}},
		TargetLabel: "//server:server",
		Mnemonic:    "GoLink",
		ExitCode:    0,
	}
	payload, err := spawn.MarshalVT()
	require.NoError(t, err)

	step, err := readSpawnStep(payload, labelSet{}, platformSet{})
	require.NoError(t, err)
	assert.Equal(t, spawnStep{
		target:   "//server:server",
		mnemonic: "GoLink",
		platform: "linux_amd64",
	}, step)

	// A truncated spawn is a corrupt log, not an unlabelled step.
	_, err = readSpawnStep(payload[:len(payload)-1], labelSet{}, platformSet{})
	assert.Error(t, err)
}

// Only the two properties that say what a spawn ran on mean anything here.
func TestPlatformName(t *testing.T) {
	name := func(properties ...*spb.Platform_Property) string {
		t.Helper()
		payload, err := (&spb.Platform{Properties: properties}).MarshalVT()
		require.NoError(t, err)
		n, err := platformName(payload)
		require.NoError(t, err)
		return n
	}
	os := &spb.Platform_Property{Name: "OSFamily", Value: "Darwin"}
	arch := &spb.Platform_Property{Name: "Arch", Value: "arm64"}
	other := &spb.Platform_Property{Name: "Pool", Value: "bare"}

	assert.Equal(t, "darwin_arm64", name(other, os, arch))
	assert.Equal(t, "darwin", name(os))
	assert.Equal(t, "arm64", name(arch))
	assert.Empty(t, name(other))
	assert.Empty(t, name())
}

// The memo answers from the encoded bytes, so a platform is only unpacked once
// however many spawns share it.
func TestPlatformSetMemoizes(t *testing.T) {
	payload, err := (&spb.Platform{Properties: []*spb.Platform_Property{
		{Name: "OSFamily", Value: "Linux"},
		{Name: "Arch", Value: "amd64"},
	}}).MarshalVT()
	require.NoError(t, err)

	platforms := platformSet{}
	for range 3 {
		name, err := platforms.name(payload)
		require.NoError(t, err)
		assert.Equal(t, "linux_amd64", name)
	}
	assert.Len(t, platforms, 1)

	// The bytes come out of a buffer the decoder reuses, so the key has to be a
	// copy of them: zeroing the buffer mustn't touch what's in the map.
	same := slices.Clone(payload)
	clear(payload)
	assert.Equal(t, "linux_amd64", platforms[string(same)])
}

func TestLabelSetInterns(t *testing.T) {
	labels := labelSet{}
	assert.Equal(t, "//server:server", labels.intern([]byte("//server:server")))
	assert.Equal(t, "//server:server", labels.intern([]byte("//server:server")))
	assert.Equal(t, "GoLink", labels.intern([]byte("GoLink")))
	// One entry per distinct label, however many times it's seen.
	assert.Len(t, labels, 2)

	// The bytes come out of a buffer the decoder reuses for the next entry, so
	// the interned string has to be a copy of them.
	buf := []byte("//a:a")
	interned := labels.intern(buf)
	copy(buf, "//b:b")
	assert.Equal(t, "//a:a", interned)
}

// The invocation list is ordered by when the build ran, which only logs fetched
// from a server know.
func TestSortLogsByTime(t *testing.T) {
	tr := newTree()
	tr.logs = []logInfo{
		{name: "from-a-file"},
		{name: "older", updatedAtUsec: 1000},
		{name: "newer", updatedAtUsec: 2000},
		{name: "also-from-a-file"},
	}
	assert.Equal(t, []int{2, 1, 0, 3}, tr.sortLogsByTime([]int{0, 1, 2, 3}))
	assert.Equal(t, "1970-01-01 00:00:00", tr.log(1).when())
	assert.Empty(t, tr.log(0).when())
}

// Which builds a path appeared in is exact however far back the walk goes: the
// file info view reads a log per build, and a build it never heard of is one it
// can't read.
func TestLogSet(t *testing.T) {
	var s logSet
	assert.Equal(t, 0, s.count())
	assert.Empty(t, s.indexes())
	assert.False(t, s.has(0))

	for _, logIdx := range []int{0, 3, 64, 200} {
		s = s.set(logIdx)
	}
	assert.Equal(t, []int{0, 3, 64, 200}, s.indexes())
	assert.Equal(t, 4, s.count())
	assert.True(t, s.has(200))
	assert.False(t, s.has(1))
	assert.False(t, s.has(1000))

	// Setting the same log twice is one appearance.
	s = s.set(3)
	assert.Equal(t, 4, s.count())

	// A shorter set folded into a longer one, and the other way round.
	other := logSet{}.set(1)
	assert.Equal(t, []int{0, 1, 3, 64, 200}, s.or(other).indexes())
	assert.Equal(t, []int{0, 1, 3, 64, 200}, other.or(s).indexes())
}

// The tree tracks the logs each path appeared in past the 64th, which is what
// the file info view walks.
func TestEntryLogsPastTheFirstWords(t *testing.T) {
	tr := newTree()
	tr.logs = make([]logInfo, 200)
	tr.add("a.go", newDigest("aaaa", 1))
	for _, logIdx := range []int{70, 199} {
		tr.add("a.go", fromLog(newDigest("aaaa", 1), logIdx))
	}
	// A different version in a much later log.
	tr.add("a.go", fromLog(newDigest("bbbb", 1), 150))

	e := tr.find("a.go").entry
	assert.Equal(t, []int{0, 70, 150, 199}, e.logs().indexes())
	assert.True(t, e.inMultipleLogs())
	assert.True(t, e.modified())
	assert.Empty(t, tr.conflicts)

	// Two different versions from the same log is still a conflict that far out.
	tr.add("a.go", fromLog(newDigest("cccc", 1), 150))
	assert.Len(t, tr.conflicts, 1)
}

func TestStripConfig(t *testing.T) {
	assert.Equal(t, "bazel-out/bin/cli/cli.a",
		stripConfig("bazel-out/linux_x86_64-opt-ST-9dfe9018e3d4/bin/cli/cli.a"))
	assert.Equal(t, "bazel-out/testlogs/a/test.log",
		stripConfig("bazel-out/k8-fastbuild/testlogs/a/test.log"))
	// Sources have no configuration in them, and neither does bazel-out itself.
	assert.Equal(t, "cli/cli.go", stripConfig("cli/cli.go"))
	assert.Equal(t, "bazel-out/volatile-status.txt", stripConfig("bazel-out/volatile-status.txt"))
	assert.Equal(t, "bazel-out", stripConfig("bazel-out"))
	// The convenience symlinks are already resolved to one configuration.
	assert.Equal(t, "bazel-bin/cli/cli.a", stripConfig("bazel-bin/cli/cli.a"))
}

// Merging configurations brings the same file built several ways together,
// without touching the tree it was built from.
func TestStripConfigs(t *testing.T) {
	tr := newTree()
	tr.logs = []logInfo{{name: "one"}, {name: "two"}}
	tr.add("cli/cli.go", newDigest("source", 1))
	tr.add("bazel-out/k8-opt/bin/cli/cli.a", newDigest("opt", 2))
	tr.add("bazel-out/k8-fastbuild/bin/cli/cli.a", newDigest("fast", 3))
	// The same file in the same configuration, changed between builds.
	tr.add("bazel-out/k8-opt/bin/cli/cli.a", fromLog(newDigest("opt2", 4), 1))
	tr.add("bazel-out/k8-fastbuild/bin/cli/cli.a", fromLog(newDigest("fast", 3), 1))

	merged := stripConfigs(tr)

	// The source is untouched, and the two configurations are one file.
	assert.NotNil(t, merged.find("cli/cli.go"))
	assert.Nil(t, merged.find("bazel-out/k8-opt/bin/cli/cli.a"))
	n := merged.find("bazel-out/bin/cli/cli.a")
	require.NotNil(t, n)
	// In path order, so that the same tree comes out the same way every time.
	assert.Equal(t, []string{"fast", "opt", "opt2"}, versionKeys(n.entry))
	assert.Empty(t, merged.conflicts, "configurations disagreeing isn't a conflict")

	// The tree it was built from still has both, with their own versions.
	assert.Equal(t, []string{"opt", "opt2"}, versionKeys(tr.find("bazel-out/k8-opt/bin/cli/cli.a").entry))
	assert.Equal(t, []string{"fast"}, versionKeys(tr.find("bazel-out/k8-fastbuild/bin/cli/cli.a").entry))
	assert.Equal(t, []int{0, 1}, tr.find("bazel-out/k8-fastbuild/bin/cli/cli.a").entry.logs().indexes())
}

func versionKeys(e *entry) []string {
	keys := make([]string, 0, len(e.versions))
	for _, v := range e.versions {
		keys = append(keys, v.key())
	}
	return keys
}

// A file built two ways in the same builds hasn't changed; one built two ways
// in different builds has.
func TestModifiedIgnoresConfigurations(t *testing.T) {
	tr := newTree()
	tr.logs = make([]logInfo, 2)
	// Two configurations of the same file, both in both builds.
	for _, logIdx := range []int{0, 1} {
		tr.add("bazel-out/opt/bin/a.a", fromLog(newDigest("opt", 1), logIdx))
		tr.add("bazel-out/fast/bin/a.a", fromLog(newDigest("fast", 1), logIdx))
		// And one that changed between the builds.
		tr.add("bazel-out/opt/bin/b.a", fromLog(newDigest(fmt.Sprintf("b%d", logIdx), 1), logIdx))
	}
	merged := stripConfigs(tr)

	a := merged.find("bazel-out/bin/a.a").entry
	require.Len(t, a.versions, 2)
	assert.False(t, a.modified(), "two configurations of an unchanged file aren't a change")

	b := merged.find("bazel-out/bin/b.a").entry
	require.Len(t, b.versions, 2)
	assert.True(t, b.modified())
}

func TestTreeFind(t *testing.T) {
	tr := newTree()
	tr.add("a/b/c.go", newDigest("aaaa", 1))

	require.NotNil(t, tr.find("a/b/c.go"))
	assert.Equal(t, "c.go", tr.find("a/b/c.go").name)
	assert.True(t, tr.find("a/b").isDir())
	assert.Nil(t, tr.find("a/b/missing.go"))
	// A path that runs through a file isn't a path at all.
	assert.Nil(t, tr.find("a/b/c.go/d.go"))
	assert.Nil(t, tr.find(""))
}
