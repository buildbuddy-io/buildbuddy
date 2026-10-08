package version_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

// Set by x_defs in BUILD.
var branchHighestVersionRlocationpath string

func TestBranchHighestVersion(t *testing.T) {
	script := testfs.RunfilePath(t, branchHighestVersionRlocationpath)

	// Isolate git from the host's git config.
	home := testfs.MakeTempDir(t)
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")

	requireOutput := func(dir, want, msg string) {
		stdout, stderr, err := testshell.Try(t, dir, script)
		require.NoError(t, err, "%s: stderr: %s", msg, stderr)
		require.Equal(t, want, strings.TrimSpace(stdout), msg)
	}
	requireFailure := func(dir, msg string) {
		stdout, _, err := testshell.Try(t, dir, script)
		require.Error(t, err, msg)
		require.Empty(t, stdout, msg)
	}

	empty := testfs.MakeTempDir(t)
	testshell.Run(t, empty, "git init")
	requireFailure(empty, "no commits")
	repo, _ := testgit.MakeTempRepo(t, map[string]string{"README": ""})
	requireFailure(repo, "no tags")

	// Give every commit and tag a later timestamp than the last, so
	// creation-date order is well defined (and differs from version order
	// where it matters).
	now := 1700000000
	run := func(script string) {
		now += 60
		t.Setenv("GIT_AUTHOR_DATE", fmt.Sprintf("@%d +0000", now))
		t.Setenv("GIT_COMMITTER_DATE", fmt.Sprintf("@%d +0000", now))
		testshell.Run(t, repo, script)
	}
	commit := func(msg string) { run("git commit -q --allow-empty -m " + msg) }
	tag := func(name string) { run(fmt.Sprintf("git tag -a %s -m %s", name, name)) }

	tag("v2.9.0")
	commit("b")
	tag("v2.10.0")
	tag("not-a-version")
	tag("v2.11.0-rc1")
	tag("cli-v5.0.0")
	requireOutput(repo, "v2.10.0", "versions sort numerically, non-vX.Y.Z tags ignored")

	// Cut a release branch and tag it, then keep committing and tagging master.
	commit("c")
	run("git checkout -q -b bb_release_1")
	commit("cherry-pick-1")
	tag("v2.11.0")
	run("git checkout -q master")
	commit("d")
	tag("v2.12.0")
	requireOutput(repo, "v2.12.0", "master sees its own newest tag")

	// A lower version tagged later doesn't win: versions sort by number, not
	// by creation date.
	commit("e")
	tag("v2.1.99")
	requireOutput(repo, "v2.12.0", "lower version tagged later is ignored")

	// Cut a newer release branch with a higher version than anything on master.
	run("git checkout -q -b bb_release_2")
	commit("cherry-pick-3")
	tag("v2.13.0")
	run("git checkout -q master")
	requireOutput(repo, "v2.12.0", "master ignores higher tags on other branches")

	run("git checkout -q bb_release_1")
	requireOutput(repo, "v2.11.0", "release branch ignores newer master tags")
	commit("cherry-pick-2")
	requireOutput(repo, "v2.11.0", "untagged commit resolves the branch's tag")
	tag("v2.11.1")
	requireOutput(repo, "v2.11.1", "patch tag")

	// Shallow clones may be missing tagged commits, so refuse to guess.
	shallow := filepath.Join(testfs.MakeTempDir(t), "shallow")
	run(fmt.Sprintf("git clone -q --depth 1 --branch bb_release_1 file://%s %s && git -C %s fetch -q --tags", repo, shallow, shallow))
	requireFailure(shallow, "shallow clone")
}
