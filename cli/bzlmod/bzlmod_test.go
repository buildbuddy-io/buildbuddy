package bzlmod_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/bzlmod"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

func TestBazelDep_FollowsIncludes(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"MODULE.bazel": `
module(name = "x")
include("//deps:bazel_dep.MODULE.bazel")
`,
		"deps/BUILD": "",
		"deps/bazel_dep.MODULE.bazel": `
bazel_dep(name = "rules_go", version = "0.63.0", repo_name = "io_bazel_rules_go")
bazel_dep(name = "gazelle", repo_name = "bazel_gazelle")
include("//deps:more.MODULE.bazel")
`,
		"deps/more.MODULE.bazel": `bazel_dep(name = "rules_shell", version = "0.4.1")`,
	})

	m, err := bzlmod.Load(ws)
	require.NoError(t, err)

	version, ok := m.BazelDep("rules_go")
	require.True(t, ok)
	require.Equal(t, "0.63.0", version)

	// A bazel_dep without a version (e.g. because of an override) still counts.
	version, ok = m.BazelDep("gazelle")
	require.True(t, ok)
	require.Equal(t, "", version)

	// Includes are followed transitively.
	_, ok = m.BazelDep("rules_shell")
	require.True(t, ok)

	_, ok = m.BazelDep("rules_python")
	require.False(t, ok)

}

func TestLoad_MissingInclude(t *testing.T) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		"MODULE.bazel": `include("//deps:missing.MODULE.bazel")`,
	})
	_, err := bzlmod.Load(ws)
	require.Error(t, err)
}

func TestLoad_RejectsLabelsBazelRejects(t *testing.T) {
	for _, label := range []string{":x.MODULE.bazel", "@//:x.MODULE.bazel", "@repo//:x.MODULE.bazel", "//x.MODULE.bazel"} {
		ws := testfs.MakeTempDir(t)
		testfs.WriteAllFileContents(t, ws, map[string]string{
			"MODULE.bazel":   `include("` + label + `")`,
			"x.MODULE.bazel": "",
		})
		_, err := bzlmod.Load(ws)
		require.Error(t, err, "label %q", label)
	}
}

func TestParseAndSetBazelDepVersion(t *testing.T) {
	snippet := `bazel_dep(name = "rules_go", version = "0.64.1")

go_sdk = use_extension("@rules_go//go:extensions.bzl", "go_sdk")
`
	name, version, err := bzlmod.ParseBazelDep(snippet)
	require.NoError(t, err)
	require.Equal(t, "rules_go", name)
	require.Equal(t, "0.64.1", version)

	updated, err := bzlmod.SetBazelDepVersion(snippet, "rules_go", "0.50.1")
	require.NoError(t, err)
	_, version, err = bzlmod.ParseBazelDep(updated)
	require.NoError(t, err)
	require.Equal(t, "0.50.1", version)
	require.Contains(t, updated, "go_sdk = use_extension")
}
