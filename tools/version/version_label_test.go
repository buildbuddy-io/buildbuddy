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
var versionLabelRlocationpath string

func TestVersionLabel(t *testing.T) {
	script := testfs.RunfilePath(t, versionLabelRlocationpath)

	// Isolate git from the host's git config.
	home := testfs.MakeTempDir(t)
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")

	empty := testfs.MakeTempDir(t)
	testshell.Run(t, empty, "git init")
	stdout, _, err := testshell.Try(t, empty, script)
	require.Error(t, err, "no commits")
	require.Empty(t, stdout, "no commits")

	repo, _ := testgit.MakeTempRepo(t, map[string]string{"README": ""})
	label := func() string {
		// Run from a different cwd than the script's directory.
		stdout, stderr, err := testshell.Try(t, repo, script)
		require.NoError(t, err, "stderr: %s", stderr)
		return strings.TrimSpace(stdout)
	}
	hosted := func() string {
		date := strings.TrimSpace(testshell.Run(t, repo, "TZ=UTC git show -s --date=format-local:%Y%m%d --format=%cd HEAD"))
		sha := strings.TrimSpace(testshell.Run(t, repo, "git rev-parse HEAD"))
		return "hosted-" + date + "-" + sha[:7]
	}
	commit := func() {
		t.Setenv("GIT_AUTHOR_DATE", "2026-03-04T12:00:00 +0000")
		t.Setenv("GIT_COMMITTER_DATE", "2026-03-04T12:00:00 +0000")
		testshell.Run(t, repo, "git commit -q --allow-empty -m c")
	}
	tag := func(names ...string) {
		for _, name := range names {
			testshell.Run(t, repo, "git tag "+name)
		}
	}

	require.Equal(t, hosted(), label(), "untagged commit")

	tag("v2.8.0")
	commit()
	require.Equal(t, hosted(), label(), "tag on an older commit doesn't count")

	tag("v2.10.0-rc1", "not-a-version", "cli-v5.0.0")
	require.Equal(t, hosted(), label(), "non-release tags ignored")

	tag("v2.10.0")
	require.Equal(t, "v2.10.0", label(), "release tag at HEAD")

	tag("v2.9.0")
	require.Equal(t, "v2.10.0", label(), "highest tag by version sort, not lexically")
}
