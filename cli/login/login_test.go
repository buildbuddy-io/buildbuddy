package login

import (
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

func TestAPIKeyDiscovery(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		envAPIKey         string
		repoAPIKey        string
		expectedAPIKey    string
		expectedArgs      []string
		expectedGetAPIErr bool
	}{
		{
			name:           "env defined",
			envAPIKey:      "env-api-key",
			expectedAPIKey: "env-api-key",
			expectedArgs: []string{
				"build",
				"//foo:bar",
				"--remote_header=x-buildbuddy-api-key=env-api-key",
			},
		},
		{
			name:           ".git/config value defined",
			repoAPIKey:     "repo-api-key",
			expectedAPIKey: "repo-api-key",
			expectedArgs: []string{
				"build",
				"//foo:bar",
				"--remote_header=x-buildbuddy-api-key=repo-api-key",
			},
		},
		{
			name:           "both env and .git/config defined",
			envAPIKey:      "env-api-key",
			repoAPIKey:     "repo-api-key",
			expectedAPIKey: "env-api-key",
			expectedArgs: []string{
				"build",
				"//foo:bar",
				"--remote_header=x-buildbuddy-api-key=env-api-key",
			},
		},
		{
			name:              "neither env nor .git/config defined",
			expectedArgs:      []string{"build", "//foo:bar"},
			expectedGetAPIErr: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repoRoot, _ := testgit.MakeTempRepo(t, map[string]string{
				"README.md": "# test repo",
			})
			previousRepoRootPath := storage.RepoRootPath
			storage.RepoRootPath = func() (string, error) {
				return repoRoot, nil
			}
			t.Cleanup(func() {
				storage.RepoRootPath = previousRepoRootPath
			})

			// Simulate either CI-provided credentials, repo-local config, or both.
			setNonTTYStdin(t)
			t.Setenv("BUILDBUDDY_API_KEY", testCase.envAPIKey)
			if testCase.repoAPIKey != "" {
				require.NoError(t, storage.WriteRepoConfig(apiKeyRepoSetting, testCase.repoAPIKey))
			}

			// GetAPIKey should return the same key that ConfigureAPIKey will use.
			apiKey, err := GetAPIKey()
			if testCase.expectedGetAPIErr {
				require.True(t, status.IsNotFoundError(err))
				require.Empty(t, apiKey)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedAPIKey, apiKey)
			}

			// Supported Bazel commands should receive the discovered API key.
			args, err := ConfigureAPIKey([]string{"build", "//foo:bar"})

			require.NoError(t, err)
			require.Equal(t, testCase.expectedArgs, args)
		})
	}
}

func setNonTTYStdin(t *testing.T) {
	t.Helper()

	stdin, err := os.CreateTemp(t.TempDir(), "stdin")
	require.NoError(t, err)
	previousStdin := os.Stdin
	os.Stdin = stdin
	t.Cleanup(func() {
		os.Stdin = previousStdin
		require.NoError(t, stdin.Close())
	})
}
