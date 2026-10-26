package version_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

// Set by x_defs in BUILD.
var buildLabelRlocationpath string

func TestBuildLabel(t *testing.T) {
	script := testfs.RunfilePath(t, buildLabelRlocationpath)

	// Isolate git from the host's git config, and use a non-UTC local
	// timezone so local-time dates would be caught.
	home := testfs.MakeTempDir(t)
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("TZ", "America/Los_Angeles")
	t.Setenv("CI", "true")

	empty := testfs.MakeTempDir(t)
	testshell.Run(t, empty, "git init")
	stdout, _, err := testshell.Try(t, empty, script)
	require.Error(t, err, "no commits")
	require.Empty(t, stdout, "no commits")

	repo, _ := testgit.MakeTempRepo(t, map[string]string{"README": ""})
	labelWithArgs := func(args string) string {
		stdout, stderr, err := testshell.Try(t, repo, script+args)
		require.NoError(t, err, "stderr: %s", stderr)
		return strings.TrimSpace(stdout)
	}
	label := func() string { return labelWithArgs(" --ignore-tags") }
	headSHA := func() string {
		return strings.TrimSpace(testshell.Run(t, repo, "git rev-parse HEAD"))
	}
	commit := func(authorDate, committerDate string) {
		t.Setenv("GIT_AUTHOR_DATE", authorDate)
		t.Setenv("GIT_COMMITTER_DATE", committerDate)
		testshell.Run(t, repo, "git commit -q --allow-empty -m c")
	}

	commit("2026-03-04T12:00:00 +0000", "2026-03-04T12:00:00 +0000")
	require.Equal(t, "ci-20260304-"+headSHA()[:7], label(), "date and first 7 chars of the SHA")
	require.Regexp(t, `^ci-\d{8}-[0-9a-f]{7}$`, label())

	// 2026-10-21 20:00 -0800 is 2026-10-22 04:00 in UTC, but still 2026-10-21
	// both at the commit's own offset and in America/Los_Angeles (PDT).
	commit("2026-10-21T20:00:00 -0800", "2026-10-21T20:00:00 -0800")
	require.Equal(t, "ci-20261022-"+headSHA()[:7], label(), "date is UTC")

	commit("2020-01-02T12:00:00 +0000", "2026-03-04T12:00:00 +0000")
	require.Equal(t, "ci-20260304-"+headSHA()[:7], label(), "committer date, not author date")

	testshell.Run(t, repo, "git tag v2.10.0")
	require.Equal(t, "ci-20260304-"+headSHA()[:7], label(), "tags are ignored with --ignore-tags")

	sha := headSHA()[:7]
	t.Setenv("CI", "")
	require.Equal(t, "local-20260304-"+sha, label(), "CI unset")
	t.Setenv("CI", "true")

	testfs.WriteAllFileContents(t, repo, map[string]string{"untracked": "x"})
	require.Equal(t, "ci-20260304-"+sha, label(), "untracked files are ignored")

	// Touching a file changes its mtime but not its content.
	testshell.Run(t, repo, "touch README")
	require.Equal(t, "ci-20260304-"+sha, label(), "touched file with same content")

	testfs.WriteAllFileContents(t, repo, map[string]string{"README": "modified"})
	require.Equal(t, "local-20260304-"+sha, label(), "modified tracked file")

	// Default mode: tags at HEAD win.
	testshell.Run(t, repo, "git checkout -q README")
	require.Equal(t, "v2.10.0", labelWithArgs(""), "HEAD tagged")
	require.Equal(t, "ci-20260304-"+sha, label(), "--ignore-tags still ignores the tag")

	t.Setenv("CI", "")
	testfs.WriteAllFileContents(t, repo, map[string]string{"README": "modified"})
	require.Equal(t, "v2.10.0", labelWithArgs(""), "tags win over CI unset and a modified tree")
	testshell.Run(t, repo, "git checkout -q README")
	t.Setenv("CI", "true")

	testshell.Run(t, repo, "git tag v2.9.0")
	require.Equal(t, "v2.10.0", labelWithArgs(""), "highest by version, not lexically")

	commit("2026-03-05T12:00:00 +0000", "2026-03-05T12:00:00 +0000")
	require.Equal(t, "ci-20260305-"+headSHA()[:7], labelWithArgs(""), "older commit tagged, HEAD untagged")

	testshell.Run(t, repo, "git tag v2.10.0-rc1 && git tag not-a-version && git tag cli-v5.0.0")
	require.Equal(t, "ci-20260305-"+headSHA()[:7], labelWithArgs(""), "non-vX.Y.Z tags are ignored")

	for _, args := range []string{" --bogus", " --ignore-tags extra"} {
		stdout, _, err := testshell.Try(t, repo, script+args)
		require.Error(t, err, args)
		require.Empty(t, stdout, args)
	}
}
