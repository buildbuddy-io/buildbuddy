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

	require.NoError(t, UnsetRepoConfig("api-key"))

	require.False(t, gitConfigHasKey(t, repoRoot, "api-key"))
	value, err := ReadRepoConfig("api-key")
	require.NoError(t, err)
	require.Empty(t, value)
}

func TestUnsetRepoConfigIsNotAnErrorWhenUnset(t *testing.T) {
	setUpTempRepo(t)

	require.NoError(t, UnsetRepoConfig("api-key"))
}

func TestUnsetRepoConfigLeavesOtherSettingsAlone(t *testing.T) {
	repoRoot := setUpTempRepo(t)

	require.NoError(t, WriteRepoConfig("api-key", "test-api-key"))
	require.NoError(t, WriteRepoConfig("remote-bazel-remote", "origin"))

	require.NoError(t, UnsetRepoConfig("api-key"))

	require.False(t, gitConfigHasKey(t, repoRoot, "api-key"))
	value, err := ReadRepoConfig("remote-bazel-remote")
	require.NoError(t, err)
	require.Equal(t, "origin", value)
}

// This is the behavior that makes UnsetRepoConfig necessary.
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
	require.ErrorIs(t, UnsetRepoConfig("api-key"), ErrNotInRepo)
}

// Each failure mode is pinned separately: the sentinel, and the messages that
// would otherwise be reduced to a bare exit status.
func TestRepoRootFromGitOutput(t *testing.T) {
	exitErr := &exec.ExitError{ProcessState: &os.ProcessState{}}

	t.Run("success", func(t *testing.T) {
		root, err := repoRootFromGitOutput([]byte("/path/to/repo\n"), nil)
		require.NoError(t, err)
		require.Equal(t, "/path/to/repo", root)
	})

	t.Run("not in a repo", func(t *testing.T) {
		_, err := repoRootFromGitOutput(
			[]byte("fatal: not a git repository (or any of the parent directories): .git\n"), exitErr)
		require.ErrorIs(t, err, ErrNotInRepo)
	})

	t.Run("other failures keep git's message", func(t *testing.T) {
		_, err := repoRootFromGitOutput(
			[]byte("fatal: detected dubious ownership in repository at '/repo'\n"), exitErr)
		require.NotErrorIs(t, err, ErrNotInRepo)
		require.Contains(t, err.Error(), "dubious ownership")
	})

	t.Run("silent failures still report the cause", func(t *testing.T) {
		_, err := repoRootFromGitOutput(nil, errors.New("exec: \"git\": executable file not found in $PATH"))
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
