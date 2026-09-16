package git_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/util/git"
	"github.com/stretchr/testify/require"
)

func TestRun_RedactsCredentialsInError(t *testing.T) {
	binDir := t.TempDir()
	gitPath := filepath.Join(binDir, "git")
	script := "#!/bin/sh\nprintf 'fatal: https://user:%s@example.com/repo token=%s\\n' \"$REPO_TOKEN\" \"$REPO_TOKEN\" >&2\nexit 7\n"
	require.NoError(t, os.WriteFile(gitPath, []byte(script), 0700))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("REPO_TOKEN", "environment-secret-123")

	err := git.Run(context.Background(), "", io.Discard, "fetch", "https://user:argument-secret-456@example.com/repo", "environment-secret-123")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "environment-secret-123")
	require.NotContains(t, err.Error(), "argument-secret-456")
	require.True(t, strings.Contains(err.Error(), "<REDACTED>"))
	var exitErr *exec.ExitError
	require.True(t, errors.As(err, &exitErr))
	require.Equal(t, 7, exitErr.ExitCode())
}
