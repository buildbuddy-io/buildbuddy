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
var hostedLabelRlocationpath string

func TestHostedLabel(t *testing.T) {
	script := testfs.RunfilePath(t, hostedLabelRlocationpath)

	// Isolate git from the host's git config, and use a non-UTC local
	// timezone so local-time dates would be caught.
	home := testfs.MakeTempDir(t)
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("TZ", "America/Los_Angeles")

	empty := testfs.MakeTempDir(t)
	testshell.Run(t, empty, "git init")
	stdout, _, err := testshell.Try(t, empty, script)
	require.Error(t, err, "no commits")
	require.Empty(t, stdout, "no commits")

	repo, _ := testgit.MakeTempRepo(t, map[string]string{"README": ""})
	label := func() string {
		stdout, stderr, err := testshell.Try(t, repo, script)
		require.NoError(t, err, "stderr: %s", stderr)
		return strings.TrimSpace(stdout)
	}
	headSHA := func() string {
		return strings.TrimSpace(testshell.Run(t, repo, "git rev-parse HEAD"))
	}
	commit := func(authorDate, committerDate string) {
		t.Setenv("GIT_AUTHOR_DATE", authorDate)
		t.Setenv("GIT_COMMITTER_DATE", committerDate)
		testshell.Run(t, repo, "git commit -q --allow-empty -m c")
	}

	commit("2026-03-04T12:00:00 +0000", "2026-03-04T12:00:00 +0000")
	require.Equal(t, "hosted-20260304-"+headSHA()[:7], label(), "date and first 7 chars of the SHA")
	require.Regexp(t, `^hosted-\d{8}-[0-9a-f]{7}$`, label())

	// 2026-10-21 23:30 -0800 is 2026-10-22 in UTC.
	commit("2026-10-21T23:30:00 -0800", "2026-10-21T23:30:00 -0800")
	require.Equal(t, "hosted-20261022-"+headSHA()[:7], label(), "date is UTC")

	commit("2020-01-02T12:00:00 +0000", "2026-03-04T12:00:00 +0000")
	require.Equal(t, "hosted-20260304-"+headSHA()[:7], label(), "committer date, not author date")

	testshell.Run(t, repo, "git tag v2.10.0")
	require.Equal(t, "hosted-20260304-"+headSHA()[:7], label(), "tags are ignored")
}
