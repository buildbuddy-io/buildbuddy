package fix_test

import (
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/bzlmod"
	"github.com/stretchr/testify/require"
)

// Tests that `bb fix` reports failures, and that --diff reports pending
// changes, through its exit code.

func TestFix_FailsOnUnparseableBuildFile(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel":    "module(name = \"x\")\n",
		"bad/BUILD.bazel": "this is ( not starlark\n",
		"BUILD.bazel":     poorlyFormatted,
	})

	out, err := runFix(t, ws)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, "bad/BUILD.bazel")

	// Other files are still fixed.
	b, err := os.ReadFile(filepath.Join(ws, "BUILD.bazel"))
	require.NoError(t, err)
	require.NotEqual(t, poorlyFormatted, string(b), "BUILD.bazel should still have been reformatted")
}

func TestFix_KeepsGoingWhenBuildifierConfigIsBad(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel":     "module(name = \"x\")\n",
		"BUILD.bazel":      poorlyFormatted,
		"defs.bzl":         "def  f(): pass\n",
		".buildifier.json": "{not json",
	})

	out, err := runFix(t, ws)
	require.Error(t, err, "output: %s", out)
	// Buildifier fails for every file, rather than the first failure ending
	// `bb fix`.
	require.Contains(t, out, "buildifier BUILD.bazel")
	require.Contains(t, out, "buildifier defs.bzl")
}

func TestFix_DiffExitCode(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n",
		"BUILD.bazel":  poorlyFormatted,
	})

	out, err := runFix(t, ws, "--diff")
	require.Error(t, err, "--diff should fail when there are changes to apply; output: %s", out)

	out, err = runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	out, err = runFix(t, ws, "--diff")
	require.NoError(t, err, "--diff should pass once there's nothing to change; output: %s", out)
}

func TestFix_FailsWhenGazelleFails(t *testing.T) {
	// Gazelle refuses to process a BUILD file with two rules with the same
	// name (as happened in buildbuddy-internal, unnoticed, because `bb fix`
	// ignored Gazelle's exit code).
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n",
		"BUILD.bazel": `filegroup(
    name = "a",
    srcs = [],
)

filegroup(
    name = "a",
    srcs = [],
)
`,
	})

	out, err := runFix(t, ws)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, `multiple rules have the name "a"`)
}

// goProject is a minimal Go module with no Bazel setup.
var goProject = map[string]string{
	"go.mod": "module example.com/hello\n\ngo 1.24\n",
	"main.go": `package main

import "fmt"

func main() { fmt.Println("hello") }
`,
}

func TestFix_BootstrapsGoProject(t *testing.T) {
	// Uses the network: `bb fix` adds rules_go and gazelle with `bb add`.
	ws := fixWorkspace(t, goProject)

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	m, err := bzlmod.Load(ws)
	require.NoError(t, err)
	for _, dep := range []string{"rules_go", "gazelle"} {
		version, ok := m.BazelDep(dep)
		require.True(t, ok, "MODULE.bazel should depend on %s", dep)
		require.NotContains(t, version, "-", "should not pick a pre-release of %s", dep)
	}
	require.True(t, m.UsesExtension("//:extensions.bzl", "go_deps"))
	require.FileExists(t, filepath.Join(ws, "BUILD.bazel"), "gazelle should generate a BUILD file")

	// Running again changes nothing (in particular, doesn't add the deps
	// again).
	before := snapshot(t, ws)
	out, err = runFix(t, ws)
	require.NoError(t, err, "second run output: %s", out)
	require.Equal(t, before, snapshot(t, ws))
}

func TestFix_RegistersGoDepsWithGazellesRepoName(t *testing.T) {
	// Uses the network. gazelle is already a dep, under a repo_name, but
	// there's no go_deps yet.
	contents := map[string]string{
		"MODULE.bazel": `module(name = "x")

bazel_dep(name = "rules_go", version = "0.50.1", repo_name = "io_bazel_rules_go")
bazel_dep(name = "gazelle", version = "0.40.0", repo_name = "bazel_gazelle")
`,
	}
	maps.Copy(contents, goProject)
	ws := fixWorkspace(t, contents)

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)
	b, err := os.ReadFile(filepath.Join(ws, "MODULE.bazel"))
	require.NoError(t, err)
	require.Contains(t, string(b), `use_extension("@bazel_gazelle//:extensions.bzl", "go_deps")`)
}

func TestFix_LeavesIncludedModuleDepsAlone(t *testing.T) {
	// Uses the network. Like the BuildBuddy repos: the bazel_deps and go_deps
	// live in include()d files that MODULE.bazel doesn't mention by content.
	contents := map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n\ninclude(\"//deps:deps.MODULE.bazel\")\n",
		"deps/BUILD":   "",
		"deps/deps.MODULE.bazel": `bazel_dep(name = "rules_go", version = "0.50.1", repo_name = "io_bazel_rules_go")
bazel_dep(name = "gazelle", version = "0.40.0", repo_name = "bazel_gazelle")

deps = use_extension("@bazel_gazelle//:extensions.bzl", "go_deps")
deps.from_file(go_mod = "//:go.mod")
`,
	}
	maps.Copy(contents, goProject)
	ws := fixWorkspace(t, contents)

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)
	for _, f := range []string{"MODULE.bazel", "deps/deps.MODULE.bazel"} {
		b, err := os.ReadFile(filepath.Join(ws, f))
		require.NoError(t, err)
		require.Equal(t, contents[f], string(b), "%s should be unchanged", f)
	}
}
