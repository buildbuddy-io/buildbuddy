// Package fix_test contains end-to-end integration tests for `bb fix`.
//
// These tests run the real `bb` binary as a subprocess against scratch
// repos. They cover the network-free behaviors documented in cli/fix/fix.go:
// MODULE.bazel bootstrap, buildifier formatting, hidden-directory skipping,
// --diff being non-mutating, idempotency on re-run, and --help.
//
// Repo-defined Gazelle is exercised via a local Bazel stub, without launching
// Bazel. Language detection (which goes to BCR via `bb add`) is intentionally
// omitted to keep the suite hermetic.
package fix_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/quarantine"
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
	// Buildifier differences must fail the command without changing files.
	out, err := runFix(t, ws, "--diff")
	require.Error(t, err, "output: %s", out)
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
	quarantine.SkipQuarantinedTest(t)
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

// repoGazelleWorkspace selects a local executable as Bazel itself, rather than
// tools/bazel (which would still require Bazelisk to resolve/download Bazel).
// The stub and its argument log live outside the workspace so --diff snapshots
// detect only changes made by bb fix, not the test's invocation bookkeeping.
func repoGazelleWorkspace(t *testing.T, contents map[string]string, exitCode int) (ws, stub string) {
	t.Helper()
	stubDir := testfs.MakeTempDir(t)
	stub = filepath.Join(stubDir, "bazel-stub")
	testfs.WriteAllFileContents(t, stubDir, map[string]string{
		"bazel-stub": fmt.Sprintf(`#!/bin/sh
set -eu
printf '%%s\n' "$@" >> "$BB_FIX_TEST_ARGS_FILE"
printf 'repo Gazelle diagnostic\n' >&2
exit %d
`, exitCode),
	})
	testfs.MakeExecutable(t, stubDir, "bazel-stub")
	ws = fixWorkspace(t, map[string]string{
		"MODULE.bazel":  "module(name = \"x\")\n",
		"BUILD.bazel":   "gazelle(name = \"gazelle\")\n",
		".bazelversion": stub + "\n",
	})
	testfs.WriteAllFileContents(t, ws, contents)
	// Override any Bazel version inherited from the test runner as well.
	t.Setenv("BB_USE_BAZEL_VERSION", stub)
	t.Setenv("USE_BAZEL_VERSION", stub)
	// Bazelisk runs the stub via a cache symlink, so $0 is not its original
	// path. Pass the argument log's absolute path independently.
	t.Setenv("BB_FIX_TEST_ARGS_FILE", stub+".args")
	return ws, stub
}

func requireRepoGazelleRan(t *testing.T, stub, mode string) {
	t.Helper()
	b, err := os.ReadFile(stub + ".args")
	require.NoError(t, err, "repo Gazelle must run even after formatting errors")
	want := "run\n--\n//:gazelle\n"
	if mode == "diff" {
		want += "-mode=diff\n"
	}
	require.Equal(t, want, string(b), "repo Gazelle should run exactly once with the requested mode")
}

func TestFix_RepoGazelleExitStatus(t *testing.T) {
	for _, mode := range []string{"fix", "diff"} {
		for _, exitCode := range []int{0, 7} {
			t.Run(fmt.Sprintf("%s/exit_%d", mode, exitCode), func(t *testing.T) {
				ws, stub := repoGazelleWorkspace(t, nil, exitCode)
				args := []string{"fix"}
				if mode == "diff" {
					args = append(args, "--diff")
				}
				cmd := testcli.Command(t, ws, args...)
				stdout, stderr, err := testcli.SplitOutput(cmd)
				if exitCode == 0 {
					// Gazelle may print warnings to stderr without failing.
					require.NoError(t, err, "stdout: %s\nstderr: %s", stdout, stderr)
				} else {
					// Bazelisk reports subprocess failures as an exit code with
					// no Go error; bb fix must not silently discard that code.
					require.Error(t, err, "stdout: %s\nstderr: %s", stdout, stderr)
					require.NotZero(t, cmd.ProcessState.ExitCode())
				}
				require.Empty(t, string(stdout), "a failure must not depend on Gazelle emitting stdout")
				require.Contains(t, string(stderr), "repo Gazelle diagnostic")
				requireRepoGazelleRan(t, stub, mode)
			})
		}
	}
}

func TestFix_FormattingFailureSurvivesSuccessfulGazelle(t *testing.T) {
	for _, mode := range []string{"fix", "diff"} {
		for _, path := range []string{"broken.bzl", "broken/BUILD", "broken/BUILD.bazel"} {
			t.Run(mode+"/"+path, func(t *testing.T) {
				const malformed = "broken(\n"
				ws, stub := repoGazelleWorkspace(t, map[string]string{path: malformed}, 0)
				var args []string
				if mode == "diff" {
					args = append(args, "--diff")
				}
				out, err := runFix(t, ws, args...)
				require.Error(t, err, "output: %s", out)
				require.Contains(t, out, path)
				require.Contains(t, out, "syntax error")
				requireRepoGazelleRan(t, stub, mode)
				b, err := os.ReadFile(filepath.Join(ws, path))
				require.NoError(t, err)
				require.Equal(t, malformed, string(b), "invalid input must not be overwritten")
			})
		}
	}
}

func TestFix_ContinuesFormattingAfterErrors(t *testing.T) {
	for _, mode := range []string{"fix", "diff"} {
		t.Run(mode, func(t *testing.T) {
			// WalkDir visits these in lexical order: both broken files precede
			// both formattable files, and Gazelle must run after all of them.
			contents := map[string]string{
				"00-broken.bzl":        "broken(\n",
				"01-broken/BUILD":      "also_broken(\n",
				"z-first.bzl":          "def  first( x,y ):\n  return x+y\n",
				"z-second/BUILD.bazel": poorlyFormatted,
			}
			ws, stub := repoGazelleWorkspace(t, contents, 0)
			before := snapshot(t, ws)
			args := []string{"fix"}
			if mode == "diff" {
				args = append(args, "--diff")
			}
			stdout, stderr, err := testcli.SplitOutput(testcli.Command(t, ws, args...))
			require.Error(t, err, "stdout: %s\nstderr: %s", stdout, stderr)
			require.Contains(t, string(stderr), "00-broken.bzl")
			require.Contains(t, string(stderr), "01-broken/BUILD")
			requireRepoGazelleRan(t, stub, mode)
			if mode == "diff" {
				require.Equal(t, before, snapshot(t, ws))
				// A formatting error must not suppress later diffs, and the
				// first diff's nonzero exit code must not suppress the second.
				require.Contains(t, string(stdout), "z-first.bzl")
				require.Contains(t, string(stdout), "z-second/BUILD.bazel")
			} else {
				for _, path := range []string{"z-first.bzl", "z-second/BUILD.bazel"} {
					b, err := os.ReadFile(filepath.Join(ws, path))
					require.NoError(t, err)
					require.NotEqual(t, contents[path], string(b), "%s should still be formatted", path)
				}
			}
		})
	}
}

func TestFix_MissingRepoBazelExecutable(t *testing.T) {
	for _, mode := range []string{"fix", "diff"} {
		t.Run(mode, func(t *testing.T) {
			ws, _ := repoGazelleWorkspace(t, nil, 0)
			missing := filepath.Join(testfs.MakeTempDir(t), "missing-bazel")
			t.Setenv("BB_USE_BAZEL_VERSION", missing)
			t.Setenv("USE_BAZEL_VERSION", missing)
			var args []string
			if mode == "diff" {
				args = append(args, "--diff")
			}
			out, err := runFix(t, ws, args...)
			require.Error(t, err, "output: %s", out)
			// Bazelisk reports its cache alias rather than the original path.
			require.Contains(t, out, filepath.Base(missing))
			require.Contains(t, out, "no such file or directory")
		})
	}
}

func TestFix_WalkFailureSurvivesSuccessfulGazelle(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read directories regardless of permission bits")
	}
	for _, mode := range []string{"fix", "diff"} {
		t.Run(mode, func(t *testing.T) {
			ws, stub := repoGazelleWorkspace(t, nil, 0)
			unreadable := filepath.Join(ws, "00-unreadable")
			require.NoError(t, os.Mkdir(unreadable, 0700))
			t.Cleanup(func() { require.NoError(t, os.Chmod(unreadable, 0700)) })
			require.NoError(t, os.Chmod(unreadable, 0000))
			var args []string
			if mode == "diff" {
				args = append(args, "--diff")
			}
			out, err := runFix(t, ws, args...)
			require.Error(t, err, "output: %s", out)
			require.Contains(t, out, "00-unreadable")
			require.Contains(t, out, "permission denied")
			requireRepoGazelleRan(t, stub, mode)
		})
	}
}
