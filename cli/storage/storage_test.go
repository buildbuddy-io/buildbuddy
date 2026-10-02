package storage

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/stretchr/testify/require"
)

func TestReadRepoConfig(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		existingConfig map[string]string
		key            string
		expected       string
	}{
		{
			name: "key exists",
			existingConfig: map[string]string{
				"api-key": "test-api-key",
			},
			key:      "api-key",
			expected: "test-api-key",
		},
		{
			name:           "key does not exist",
			existingConfig: map[string]string{},
			key:            "api-key",
			expected:       "",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repoRoot := setUpTempRepo(t)

			for key, value := range testCase.existingConfig {
				cmd := exec.Command("git", "config", "--local", gitConfigSection+"."+key, value)
				cmd.Dir = repoRoot
				err := cmd.Run()
				require.NoError(t, err)
			}

			value, err := ReadRepoConfig(testCase.key)
			require.NoError(t, err)
			require.Equal(t, testCase.expected, value)
		})
	}
}

func TestUnsetRepoConfig(t *testing.T) {
	repoRoot := setUpTempRepo(t)

	require.NoError(t, WriteRepoConfig("api-key", "test-api-key"))
	require.True(t, gitConfigHasKey(t, repoRoot, "api-key"))

	removed, err := UnsetRepoConfig("api-key")
	require.NoError(t, err)
	require.True(t, removed)

	require.False(t, gitConfigHasKey(t, repoRoot, "api-key"))
	value, err := ReadRepoConfig("api-key")
	require.NoError(t, err)
	require.Empty(t, value)
}

func TestUnsetRepoConfigIsNotAnErrorWhenUnset(t *testing.T) {
	setUpTempRepo(t)

	removed, err := UnsetRepoConfig("api-key")
	require.NoError(t, err)
	require.False(t, removed)
}

func TestUnsetRepoConfigLeavesOtherSettingsAlone(t *testing.T) {
	repoRoot := setUpTempRepo(t)

	require.NoError(t, WriteRepoConfig("api-key", "test-api-key"))
	require.NoError(t, WriteRepoConfig("remote-bazel-remote", "origin"))

	_, err := UnsetRepoConfig("api-key")
	require.NoError(t, err)

	require.False(t, gitConfigHasKey(t, repoRoot, "api-key"))
	value, err := ReadRepoConfig("remote-bazel-remote")
	require.NoError(t, err)
	require.Equal(t, "origin", value)
}

func TestWriteEmptyRepoConfigLeavesKeyPresent(t *testing.T) {
	repoRoot := setUpTempRepo(t)

	require.NoError(t, WriteRepoConfig("api-key", "test-api-key"))
	require.NoError(t, WriteRepoConfig("api-key", ""))

	require.True(t, gitConfigHasKey(t, repoRoot, "api-key"))
}

// Callers match on ErrNotInRepo, so every config helper must propagate it.
func TestRepoConfigReportsNotInRepo(t *testing.T) {
	previousRepoRootPath := RepoRootPath
	RepoRootPath = func() (string, error) {
		return "", ErrNotInRepo
	}
	t.Cleanup(func() {
		RepoRootPath = previousRepoRootPath
	})

	_, err := ReadRepoConfig("api-key")
	require.ErrorIs(t, err, ErrNotInRepo)
	require.ErrorIs(t, WriteRepoConfig("api-key", "test-api-key"), ErrNotInRepo)
	_, err = UnsetRepoConfig("api-key")
	require.ErrorIs(t, err, ErrNotInRepo)
}

func TestRepoRootFromGitOutput(t *testing.T) {
	exitErr := &exec.ExitError{ProcessState: &os.ProcessState{}}

	t.Run("success", func(t *testing.T) {
		root, err := repoRootFromGitOutput([]byte("/path/to/repo\n"), nil, nil)
		require.NoError(t, err)
		require.Equal(t, "/path/to/repo", root)
	})

	// GIT_TRACE and warnings write to stderr on success.
	t.Run("stderr on success is not part of the path", func(t *testing.T) {
		root, err := repoRootFromGitOutput(
			[]byte("/path/to/repo\n"), []byte("12:00:00.000000 git.c:463 trace: built-in: git rev-parse --show-toplevel\n"), nil)
		require.NoError(t, err)
		require.Equal(t, "/path/to/repo", root)
	})

	t.Run("not in a repo", func(t *testing.T) {
		_, err := repoRootFromGitOutput(
			nil, []byte("fatal: not a git repository (or any of the parent directories): .git\n"), exitErr)
		require.ErrorIs(t, err, ErrNotInRepo)
	})

	t.Run("not in a repo, stopped at a filesystem boundary", func(t *testing.T) {
		_, err := repoRootFromGitOutput(
			nil, []byte("fatal: not a git repository (or any parent up to mount point /mnt)\nStopping at filesystem boundary (GIT_DISCOVERY_ACROSS_FILESYSTEM not set).\n"), exitErr)
		require.ErrorIs(t, err, ErrNotInRepo)
	})

	t.Run("broken worktree is not reported as no repo", func(t *testing.T) {
		_, err := repoRootFromGitOutput(
			nil, []byte("fatal: not a git repository: /moved/repo/.git/worktrees/wt\n"), exitErr)
		require.NotErrorIs(t, err, ErrNotInRepo)
		require.Contains(t, err.Error(), "/moved/repo/.git/worktrees/wt")
	})

	t.Run("other failures keep git's message", func(t *testing.T) {
		_, err := repoRootFromGitOutput(
			nil, []byte("fatal: detected dubious ownership in repository at '/repo'\n"), exitErr)
		require.NotErrorIs(t, err, ErrNotInRepo)
		require.Contains(t, err.Error(), "dubious ownership")
	})

	t.Run("silent failures still report the cause", func(t *testing.T) {
		_, err := repoRootFromGitOutput(nil, nil, errors.New("exec: \"git\": executable file not found in $PATH"))
		require.NotErrorIs(t, err, ErrNotInRepo)
		require.Contains(t, err.Error(), "executable file not found")
	})
}

func setUpTempRepo(t *testing.T) string {
	t.Helper()

	repoRoot, _ := testgit.MakeTempRepo(t, map[string]string{
		"README.md": "# test repo",
	})
	previousRepoRootPath := RepoRootPath
	RepoRootPath = func() (string, error) {
		return repoRoot, nil
	}
	t.Cleanup(func() {
		RepoRootPath = previousRepoRootPath
	})
	return repoRoot
}

// gitConfigHasKey reports whether the setting exists in .git/config at all,
// which ReadRepoConfig cannot distinguish from an empty value.
func gitConfigHasKey(t *testing.T, repoRoot, key string) bool {
	t.Helper()

	cmd := exec.Command("git", "config", "--local", "--get", gitConfigSection+"."+key)
	cmd.Dir = repoRoot
	err := cmd.Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "unexpected failure running git config")
	require.Equal(t, 1, exitErr.ExitCode(), "unexpected git config exit code")
	return false
}
