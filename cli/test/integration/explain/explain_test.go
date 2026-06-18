package main

import (
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	spawn_diff "github.com/buildbuddy-io/buildbuddy/proto/spawn_diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Set via x_defs in the BUILD file.
var (
	oldCompactLogRunfilePath string
	newCompactLogRunfilePath string
)

func runfilePath(t *testing.T, rlocationPath string) string {
	t.Helper()
	p, err := runfiles.Rlocation(rlocationPath)
	require.NoError(t, err, "resolve runfile %q", rlocationPath)
	return p
}

// The two compact execution logs are for builds of the same two stamped genrule
// targets (//server/version:populate_commit and :populate_version) but with
// different stable-status.txt and volatile-status.txt contents. Both status
// files are produced by Bazel's workspace status action, which isn't emitted to
// the execution log; bb explain injects a synthetic spawn for it. We therefore
// expect that action to be reported as the single non-hermetic root cause, with
// the two stamped genrules that consume the status files attributed to it as
// transitively invalidated.
func assertExpectedDiff(t *testing.T, result *spawn_diff.DiffResult) {
	require.Len(t, result.GetSpawnDiffs(), 1)
	sd := result.GetSpawnDiffs()[0]
	assert.Equal(t, "BazelWorkspaceStatusAction", sd.GetMnemonic())
	assert.Equal(t, "bazel-out/stable-status.txt", sd.GetPrimaryOutput())
	m := sd.GetModified()
	require.NotNil(t, m, "expected the workspace status action to be modified")
	require.Len(t, m.GetDiffs(), 1)
	oc := m.GetDiffs()[0].GetOutputContents()
	require.NotNil(t, oc, "expected an output contents diff (the status files changed)")
	var changedPaths []string
	for _, fd := range oc.GetFileDiffs() {
		changedPaths = append(changedPaths, fd.GetLogicalPath())
	}
	assert.ElementsMatch(t, []string{
		"bazel-out/stable-status.txt",
		"bazel-out/volatile-status.txt",
	}, changedPaths)
	assert.Equal(t, map[string]uint32{"Genrule": 2}, m.GetTransitivelyInvalidated())
}

func TestExplainJsonOutputIsClean(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	old := runfilePath(t, oldCompactLogRunfilePath)
	new := runfilePath(t, newCompactLogRunfilePath)

	stdout, stderr, err := testcli.SplitOutput(
		testcli.Command(t, ws, "explain",
			"--old="+old,
			"--new="+new,
			"--output_format=json",
		),
	)
	require.NoError(t, err, "bb explain failed; stderr: %s", stderr)

	var result spawn_diff.DiffResult
	require.NoError(t, protojson.Unmarshal(stdout, &result),
		"stdout should be valid protojson with no extra output from the CLI stack; stdout: %q", stdout)
	assertExpectedDiff(t, &result)
}

func TestExplainProtoOutputIsClean(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	old := runfilePath(t, oldCompactLogRunfilePath)
	new := runfilePath(t, newCompactLogRunfilePath)

	stdout, stderr, err := testcli.SplitOutput(
		testcli.Command(t, ws, "explain",
			"--old="+old,
			"--new="+new,
			"--output_format=proto",
		),
	)
	require.NoError(t, err, "bb explain failed; stderr: %s", stderr)

	var result spawn_diff.DiffResult
	require.NoError(t, proto.Unmarshal(stdout, &result),
		"stdout should be valid proto binary with no extra output from the CLI stack; stdout len: %d", len(stdout))
	assertExpectedDiff(t, &result)
}

// Verifies how the non-hermeticity introduced by stamping is surfaced in the
// default human-readable text output: the synthetic workspace status action is
// reported as a non-hermetic action whose stable-status.txt and
// volatile-status.txt outputs changed, and the stamped genrules consuming them
// are listed as transitively invalidated rather than as separate root causes.
func TestExplainStampingNonHermeticityText(t *testing.T) {
	ws := testcli.NewWorkspace(t)
	old := runfilePath(t, oldCompactLogRunfilePath)
	new := runfilePath(t, newCompactLogRunfilePath)

	stdout, stderr, err := testcli.SplitOutput(
		testcli.Command(t, ws, "explain",
			"--old="+old,
			"--new="+new,
		),
	)
	require.NoError(t, err, "bb explain failed; stderr: %s", stderr)

	out := string(stdout)
	assert.Contains(t, out, "BazelWorkspaceStatusAction")
	assert.Contains(t, out, "outputs changed (action is non-hermetic)")
	assert.Contains(t, out, "bazel-out/stable-status.txt")
	assert.Contains(t, out, "bazel-out/volatile-status.txt")
	assert.Contains(t, out, "transitively invalidated")
	assert.Contains(t, out, "Genrule")
}
