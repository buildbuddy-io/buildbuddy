package storage

import (
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
