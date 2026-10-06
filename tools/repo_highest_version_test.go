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
var repoHighestVersionRlocationpath string

func TestRepoHighestVersion(t *testing.T) {
	script := testfs.RunfilePath(t, repoHighestVersionRlocationpath)
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

	// Cut and tag two release branches.
	g.commit("c")
	g.run("git checkout -q -b bb_release_1")
	g.commit("cherry-pick-1")
	g.tag("v2.11.0")
	g.run("git checkout -q master")
	g.commit("d")
	g.run("git checkout -q -b bb_release_2")
	g.commit("cherry-pick-2")
	g.tag("v2.12.0")
	requireOutput(t, repo, script, "v2.12.0", "newest release")

	// Patching the older branch creates the newest tag by creation date, but
	// the next minor release must still bump from v2.12.0.
	g.run("git checkout -q bb_release_1")
	g.commit("cherry-pick-3")
	g.tag("v2.11.1")
	requireOutput(t, repo, script, "v2.12.0", "patch to an older branch doesn't win")

	// Tags on other branches count, even when they aren't in HEAD's history.
	g.run("git checkout -q master")
	requireOutput(t, repo, script, "v2.12.0", "tags on other branches count")

	// Only tags are needed, not history.
	shallow := filepath.Join(testfs.MakeTempDir(t), "shallow")
	testshell.Run(t, repo, fmt.Sprintf("git clone -q --depth 1 --branch master file://%s %s && git -C %s fetch -q --tags", repo, shallow, shallow))
	requireOutput(t, shallow, script, "v2.12.0", "shallow clone with fetched tags")
}
