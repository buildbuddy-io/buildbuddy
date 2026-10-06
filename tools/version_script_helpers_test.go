package tools_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/stretchr/testify/require"
)

// isolateGit keeps git commands in the test isolated from the host's git
// config.
func isolateGit(t *testing.T) {
	home := testfs.MakeTempDir(t)
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
}

// gitRepo runs git commands in a test repo, giving every commit and tag a
// later timestamp than the last, so creation-date order is well defined (and
// differs from version order where it matters).
type gitRepo struct {
	t   *testing.T
	dir string
	now int64
}

func (g *gitRepo) run(script string) {
	if g.now == 0 {
		g.now = 1700000000
	}
	g.now += 60
	date := fmt.Sprintf("@%d +0000", g.now)
	g.t.Setenv("GIT_AUTHOR_DATE", date)
	g.t.Setenv("GIT_COMMITTER_DATE", date)
	testshell.Run(g.t, g.dir, script)
}

func (g *gitRepo) commit(msg string) {
	g.run(fmt.Sprintf("git commit -q --allow-empty -m %q", msg))
}

func (g *gitRepo) tag(name string) {
	g.run(fmt.Sprintf("git tag -a %q -m %q", name, name))
}

func requireOutput(t *testing.T, dir, script, want, msg string) {
	stdout, stderr, err := testshell.Try(t, dir, script)
	require.NoError(t, err, "%s: stderr: %s", msg, stderr)
	require.Equal(t, want, strings.TrimSpace(stdout), msg)
}

func requireFailure(t *testing.T, dir, script string) {
	stdout, _, err := testshell.Try(t, dir, script)
	require.Error(t, err, "stdout: %s", stdout)
	require.Empty(t, stdout)
}
