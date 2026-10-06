// Package add_test contains end-to-end integration tests for `bb add`.
//
// These run the real `bb` binary against scratch workspaces, and hit
// registry.build and the Bazel Central Registry over the network, since
// looking modules up there is what `bb add` does.
package add_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/bzlmod"
	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

// includedDeps is a workspace whose bazel_deps live in an include()d file,
// like the BuildBuddy repos. gazelle has no version, as when it's overridden.
var includedDeps = map[string]string{
	"MODULE.bazel": `module(name = "x")

include("//deps:bazel_dep.MODULE.bazel")
`,
	"deps/BUILD": "",
	"deps/bazel_dep.MODULE.bazel": `bazel_dep(name = "rules_go", version = "0.50.1", repo_name = "io_bazel_rules_go")
bazel_dep(name = "gazelle", repo_name = "bazel_gazelle")
`,
}

func runAdd(t *testing.T, ws string, args ...string) (string, error) {
	cmd := testcli.Command(t, ws, append([]string{"add"}, args...)...)
	b, err := testcli.CombinedOutput(cmd)
	return string(b), err
}

func readFiles(t *testing.T, ws string, files map[string]string) map[string]string {
	out := map[string]string{}
	for name := range files {
		b, err := os.ReadFile(filepath.Join(ws, name))
		require.NoError(t, err)
		out[name] = string(b)
	}
	return out
}

func bazelDepVersion(t *testing.T, ws, module string) string {
	m, err := bzlmod.Load(ws)
	require.NoError(t, err)
	version, ok := m.BazelDep(module)
	require.True(t, ok, "MODULE.bazel should depend on %s", module)
	return version
}

func TestAdd_AddsLatestStableVersion(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{"MODULE.bazel": `module(name = "x")` + "\n"})

	// registry.build's latest gazelle release is a pre-release, so this checks
	// that we pick the latest stable version instead.
	out, err := runAdd(t, ws, "github/bazel-contrib/bazel-gazelle")
	require.NoError(t, err, "output: %s", out)

	version := bazelDepVersion(t, ws, "gazelle")
	require.NotEmpty(t, version)
	require.NotContains(t, version, "-", "should not pick a pre-release")
}

func TestAdd_PinsRequestedVersion(t *testing.T) {
	for _, requested := range []string{"0.50.1", "v0.50.1"} {
		t.Run(requested, func(t *testing.T) {
			ws := testfs.MakeTempDir(t)
			testfs.WriteAllFileContents(t, ws, map[string]string{"MODULE.bazel": `module(name = "x")` + "\n"})

			out, err := runAdd(t, ws, "github/bazel-contrib/rules_go@"+requested)
			require.NoError(t, err, "output: %s", out)
			require.Equal(t, "0.50.1", bazelDepVersion(t, ws, "rules_go"))
		})
	}
}

func TestAdd_NoOpWhenDepIsInIncludedFile(t *testing.T) {
	for _, module := range []string{
		"github/bazel-contrib/rules_go",
		"github/bazel-contrib/rules_go@0.50.1",
		// gazelle's bazel_dep has no version.
		"github/bazel-contrib/bazel-gazelle",
	} {
		t.Run(module, func(t *testing.T) {
			ws := testfs.MakeTempDir(t)
			testfs.WriteAllFileContents(t, ws, includedDeps)

			out, err := runAdd(t, ws, module)
			require.NoError(t, err, "output: %s", out)
			require.Contains(t, out, "nothing to do")
			require.Equal(t, includedDeps, readFiles(t, ws, includedDeps),
				"bb add must not change a workspace that already has the dep")
		})
	}
}

func TestAdd_FailsOnConflictingVersion(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, includedDeps)

	out, err := runAdd(t, ws, "github/bazel-contrib/rules_go@0.51.0")
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, "0.50.1")
	require.Equal(t, includedDeps, readFiles(t, ws, includedDeps))
}

func TestAdd_FailsOnYankedVersion(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	contents := map[string]string{"MODULE.bazel": `module(name = "x")` + "\n"}
	testfs.WriteAllFileContents(t, ws, contents)

	// gazelle 0.26.0 is yanked in the BCR.
	out, err := runAdd(t, ws, "github/bazel-contrib/bazel-gazelle@0.26.0")
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, "yanked")
	require.Equal(t, contents, readFiles(t, ws, contents))
}

func TestAdd_FailsOnUnknownModule(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	contents := map[string]string{"MODULE.bazel": `module(name = "x")` + "\n"}
	testfs.WriteAllFileContents(t, ws, contents)

	out, err := runAdd(t, ws, "github/buildbuddy-io/this-module-does-not-exist")
	require.Error(t, err, "output: %s", out)
	require.True(t, strings.Contains(out, "not found"), "output: %s", out)
	require.Equal(t, contents, readFiles(t, ws, contents))
}
