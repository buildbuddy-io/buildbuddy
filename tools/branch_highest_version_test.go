package tools_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
)

// Set by x_defs in BUILD.
var branchHighestVersionRlocationpath string

func TestBranchHighestVersion(t *testing.T) {
	script := testfs.RunfilePath(t, branchHighestVersionRlocationpath)
	isolateGit(t)

	// Fails without commits or tags.
	empty := testfs.MakeTempDir(t)
	testshell.Run(t, empty, "git init")
	requireFailure(t, empty, script)
	repo, _ := testgit.MakeTempRepo(t, map[string]string{"README": ""})
	requireFailure(t, repo, script)

	g := &gitRepo{t: t, dir: repo}
	g.tag("v2.9.0")
	g.commit("b")
	g.tag("v2.10.0")
	g.tag("not-a-version")
	g.tag("v2.11.0-rc1")
	g.tag("cli-v5.0.0")
	requireOutput(t, repo, script, "v2.10.0", "versions sort numerically, non-vX.Y.Z tags ignored")

	// Cut a release branch and tag it, then keep committing and tagging master.
	g.commit("c")
	g.run("git checkout -q -b bb_release_1")
	g.commit("cherry-pick-1")
	g.tag("v2.11.0")
	g.run("git checkout -q master")
	g.commit("d")
	g.tag("v2.12.0")
	requireOutput(t, repo, script, "v2.12.0", "master sees its own newest tag")

	// A lower version tagged later doesn't win: versions sort by number, not
	// by creation date.
	g.commit("e")
	g.tag("v2.1.99")
	requireOutput(t, repo, script, "v2.12.0", "lower version tagged later is ignored")

	// Cut a newer release branch with a higher version than anything on master.
	g.run("git checkout -q -b bb_release_2")
	g.commit("cherry-pick-3")
	g.tag("v2.13.0")
	g.run("git checkout -q master")
	requireOutput(t, repo, script, "v2.12.0", "master ignores higher tags on other branches")

	g.run("git checkout -q bb_release_1")
	requireOutput(t, repo, script, "v2.11.0", "release branch ignores newer master tags")
	g.commit("cherry-pick-2")
	requireOutput(t, repo, script, "v2.11.0", "untagged commit resolves the branch's tag")
	g.tag("v2.11.1")
	requireOutput(t, repo, script, "v2.11.1", "patch tag")

	// Shallow clones may be missing tagged commits, so refuse to guess.
	shallow := filepath.Join(testfs.MakeTempDir(t), "shallow")
	testshell.Run(t, repo, fmt.Sprintf("git clone -q --depth 1 --branch bb_release_1 file://%s %s && git -C %s fetch -q --tags", repo, shallow, shallow))
	requireFailure(t, shallow, script)
}
