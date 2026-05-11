// Package fix_test contains end-to-end integration tests for `bb fix`.
//
// These tests run the real `bb` binary as a subprocess against scratch
// repos. They cover the network-free behaviors documented in cli/fix/fix.go:
// MODULE.bazel bootstrap, buildifier formatting, hidden-directory skipping,
// --diff being non-mutating, idempotency on re-run, and --help.
//
// Tests that would exercise language detection (which goes to BCR via
// `bb add`) or repo-defined Gazelle (which would invoke real bazel) are
// intentionally omitted to keep the suite hermetic.
package fix_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

// poorlyFormatted is a BUILD file that buildifier should rewrite.
const poorlyFormatted = `load( "@rules_shell//shell:sh_binary.bzl",   "sh_binary" )
sh_binary( name="x",srcs=["x.sh"] )
`

// fixWorkspace creates a fresh temp dir for `bb fix` to operate on. Unlike
// testcli.NewWorkspace it does NOT pre-create MODULE.bazel, so we can exercise
// the bootstrap path. Files from `contents` are written verbatim.
func fixWorkspace(t *testing.T, contents map[string]string) string {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, contents)
	return ws
}

// runFix runs `bb fix <args>` in ws and returns combined output.
func runFix(t *testing.T, ws string, args ...string) (string, error) {
	cmd := testcli.Command(t, ws, append([]string{"fix"}, args...)...)
	b, err := testcli.CombinedOutput(cmd)
	return string(b), err
}

// snapshot captures relative-path -> contents for every regular file under ws.
// Hidden directories are not skipped; we want to detect any change anywhere.
func snapshot(t *testing.T, ws string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.Walk(ws, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(ws, path)
		if err != nil {
			return err
		}
		out[rel] = string(b)
		return nil
	})
	require.NoError(t, err)
	return out
}

func TestFix_BootstrapsModuleBazel(t *testing.T) {
	ws := fixWorkspace(t, nil)

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	require.FileExists(t, filepath.Join(ws, "MODULE.bazel"),
		"bb fix should create MODULE.bazel when neither MODULE.bazel nor WORKSPACE exists")
	require.NoFileExists(t, filepath.Join(ws, "WORKSPACE"),
		"bb fix should not create a WORKSPACE file when bootstrapping")

	// The bootstrapped module file should declare a module named after the dir.
	b, err := os.ReadFile(filepath.Join(ws, "MODULE.bazel"))
	require.NoError(t, err)
	require.Contains(t, string(b), "module(name = \""+filepath.Base(ws)+"\")")
}

func TestFix_PreservesExistingWorkspace(t *testing.T) {
	wsContents := "# pre-existing WORKSPACE\n"
	ws := fixWorkspace(t, map[string]string{"WORKSPACE": wsContents})

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	require.NoFileExists(t, filepath.Join(ws, "MODULE.bazel"),
		"bb fix should not create MODULE.bazel when WORKSPACE already exists")
	b, err := os.ReadFile(filepath.Join(ws, "WORKSPACE"))
	require.NoError(t, err)
	require.Equal(t, wsContents, string(b),
		"WORKSPACE contents should be preserved (it's already buildifier-clean)")
}

func TestFix_PreservesExistingModule(t *testing.T) {
	modContents := "module(name = \"already_here\")\n"
	ws := fixWorkspace(t, map[string]string{"MODULE.bazel": modContents})

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	b, err := os.ReadFile(filepath.Join(ws, "MODULE.bazel"))
	require.NoError(t, err)
	require.Equal(t, modContents, string(b),
		"existing MODULE.bazel should be preserved")
}

func TestFix_FormatsBuildFile(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n",
		"BUILD.bazel":  poorlyFormatted,
	})

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	b, err := os.ReadFile(filepath.Join(ws, "BUILD.bazel"))
	require.NoError(t, err)
	got := string(b)
	require.NotEqual(t, poorlyFormatted, got, "BUILD.bazel should have been reformatted")
	// Buildifier canonicalizes spacing inside calls.
	require.Contains(t, got, `name = "x"`)
	require.Contains(t, got, `srcs = ["x.sh"]`)
}

func TestFix_FormatsBzlFile(t *testing.T) {
	poorlyFormattedBzl := "def  foo( x,y ):\n  return x+y\n"
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n",
		"defs.bzl":     poorlyFormattedBzl,
	})

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	b, err := os.ReadFile(filepath.Join(ws, "defs.bzl"))
	require.NoError(t, err)
	require.NotEqual(t, poorlyFormattedBzl, string(b),
		".bzl files should be formatted directly by buildifier")
}

func TestFix_SkipsHiddenDirectories(t *testing.T) {
	// `bb fix`'s buildifier walk skips dot-prefixed directories. We only
	// verify `.git` here: in-process Gazelle, which runs afterwards, has
	// its own ignore list that includes `.git` but not other dot-dirs
	// like `.ijwb`, so testing those would conflate the two behaviors.
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel":     "module(name = \"x\")\n",
		".git/BUILD.bazel": poorlyFormatted,
	})

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	b, err := os.ReadFile(filepath.Join(ws, ".git/BUILD.bazel"))
	require.NoError(t, err)
	require.Equal(t, poorlyFormatted, string(b),
		".git/BUILD.bazel should not be modified (hidden directory)")
}

func TestFix_IgnoresNonBuildFiles(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel":  "module(name = \"x\")\n",
		"BUILD.txt":     poorlyFormatted, // looks like Starlark, but not a build file
		"notes.bzl.bak": poorlyFormatted,
		"README.md":     "# hello\n",
	})

	out, err := runFix(t, ws)
	require.NoError(t, err, "output: %s", out)

	for _, p := range []string{"BUILD.txt", "notes.bzl.bak"} {
		b, err := os.ReadFile(filepath.Join(ws, p))
		require.NoError(t, err)
		require.Equal(t, poorlyFormatted, string(b),
			"%s should not be reformatted (not a recognized build file)", p)
	}
}

func TestFix_DiffDoesNotMutate(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n",
		"BUILD.bazel":  poorlyFormatted,
		"defs.bzl":     "def  f(): pass\n",
	})

	before := snapshot(t, ws)
	// `buildifier -mode=diff` exits non-zero when there are differences,
	// which `bb fix --diff` propagates. We don't assert on the exit code;
	// we only care that no files were mutated.
	out, _ := runFix(t, ws, "--diff")
	after := snapshot(t, ws)

	require.Equal(t, before, after, "--diff must not modify any files (output: %s)", out)
	// `deps.bzl` (the file `update-repos` would write to) must not appear.
	require.NoFileExists(t, filepath.Join(ws, "deps.bzl"))
}

func TestFix_Idempotent(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel": "module(name = \"x\")\n",
		"BUILD.bazel":  poorlyFormatted,
	})

	out, err := runFix(t, ws)
	require.NoError(t, err, "first run output: %s", out)
	afterFirst := snapshot(t, ws)

	out, err = runFix(t, ws)
	require.NoError(t, err, "second run output: %s", out)
	afterSecond := snapshot(t, ws)

	require.Equal(t, afterFirst, afterSecond,
		"running `bb fix` twice should converge: second run must be a no-op")
}

func TestFix_RunsFromSubdirectory(t *testing.T) {
	ws := fixWorkspace(t, map[string]string{
		"MODULE.bazel":    "module(name = \"x\")\n",
		"sub/BUILD.bazel": poorlyFormatted,
	})

	// Run from a subdirectory; `bb fix` should still find the workspace root
	// and format files anywhere in the repo.
	cmd := testcli.Command(t, ws, "fix")
	cmd.Dir = filepath.Join(ws, "sub")
	b, err := testcli.CombinedOutput(cmd)
	require.NoError(t, err, "output: %s", string(b))

	contents, err := os.ReadFile(filepath.Join(ws, "sub", "BUILD.bazel"))
	require.NoError(t, err)
	require.NotEqual(t, poorlyFormatted, string(contents),
		"sub/BUILD.bazel should have been reformatted")
}

func TestFix_HelpExitsWithUsage(t *testing.T) {
	ws := fixWorkspace(t, nil)
	cmd := testcli.Command(t, ws, "fix", "--help")
	b, _ := testcli.CombinedOutput(cmd)
	output := string(b)

	require.Equal(t, 1, cmd.ProcessState.ExitCode(),
		"`bb fix --help` should exit 1 (matches HandleFix on flag.ErrHelp)")
	require.Contains(t, output, "usage: bb fix")
	require.Contains(t, output, "--diff")
	// --help should not bootstrap a MODULE.bazel.
	require.NoFileExists(t, filepath.Join(ws, "MODULE.bazel"),
		"--help should print usage and exit before bootstrap")
}
