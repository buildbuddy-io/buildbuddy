package login

import (
	"errors"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/cli/storage"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

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
				"--ignore_all_rc_files",
				"build",
				"--remote_header=x-buildbuddy-api-key=env-api-key",
				"//foo:bar",
			},
		},
		{
			name:           ".git/config value defined",
			repoAPIKey:     "repo-api-key",
			expectedAPIKey: "repo-api-key",
			expectedArgs: []string{
				"--ignore_all_rc_files",
				"build",
				"--remote_header=x-buildbuddy-api-key=repo-api-key",
				"//foo:bar",
			},
		},
		{
			name:           "both env and .git/config defined",
			envAPIKey:      "env-api-key",
			repoAPIKey:     "repo-api-key",
			expectedAPIKey: "env-api-key",
			expectedArgs: []string{
				"--ignore_all_rc_files",
				"build",
				"--remote_header=x-buildbuddy-api-key=env-api-key",
				"//foo:bar",
			},
		},
		{
			name:              "neither env nor .git/config defined",
			expectedArgs:      []string{"--ignore_all_rc_files", "build", "//foo:bar"},
			expectedGetAPIErr: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)
			t.Setenv("USERPROFILE", homeDir)

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
			args, err := arg.NewBazelArgs([]string{"build", "//foo:bar"})
			require.NoError(t, err)
			err = ConfigureAPIKey(args)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedArgs, args.Resolved())
		})
	}
}

func TestHandleLoginCheck(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		envAPIKey  string
		repoAPIKey string
		notInRepo  bool
		authErr    error
		// expectedAuthKeys are the keys passed to authenticate.
		expectedAuthKeys []string
		expectedExitCode int
	}{
		{
			name:             "env only",
			envAPIKey:        "env-api-key",
			expectedAuthKeys: []string{"env-api-key"},
			expectedExitCode: 0,
		},
		{
			name:             ".git/config only",
			repoAPIKey:       "repo-api-key",
			expectedAuthKeys: []string{"repo-api-key"},
			expectedExitCode: 0,
		},
		{
			name:             "env takes precedence over .git/config",
			envAPIKey:        "env-api-key",
			repoAPIKey:       "repo-api-key",
			expectedAuthKeys: []string{"env-api-key"},
			expectedExitCode: 0,
		},
		{
			name:             "env only, outside a git repo",
			envAPIKey:        "env-api-key",
			notInRepo:        true,
			expectedAuthKeys: []string{"env-api-key"},
			expectedExitCode: 0,
		},
		{
			name:             "invalid key",
			envAPIKey:        "env-api-key",
			authErr:          status.UnauthenticatedError("invalid API key"),
			expectedAuthKeys: []string{"env-api-key"},
			expectedExitCode: 1,
		},
		{
			name:             "authentication error",
			envAPIKey:        "env-api-key",
			authErr:          status.UnavailableError("connection refused"),
			expectedAuthKeys: []string{"env-api-key"},
			expectedExitCode: 2,
		},
		{
			name:             "no key",
			expectedExitCode: 1,
		},
		{
			name:             "no key, outside a git repo",
			notInRepo:        true,
			expectedExitCode: 1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)
			t.Setenv("USERPROFILE", homeDir)

			repoRoot, _ := testgit.MakeTempRepo(t, map[string]string{
				"README.md": "# test repo",
			})
			previousRepoRootPath := storage.RepoRootPath
			storage.RepoRootPath = func() (string, error) {
				if testCase.notInRepo {
					return "", errors.New("fatal: not a git repository")
				}
				return repoRoot, nil
			}
			t.Cleanup(func() {
				storage.RepoRootPath = previousRepoRootPath
			})

			var authKeys []string
			previousAuthenticateFn := authenticateFn
			authenticateFn = func(apiKey string) error {
				authKeys = append(authKeys, apiKey)
				return testCase.authErr
			}
			t.Cleanup(func() {
				authenticateFn = previousAuthenticateFn
				*check = false
			})

			t.Setenv("BUILDBUDDY_API_KEY", testCase.envAPIKey)
			if testCase.repoAPIKey != "" {
				require.NoError(t, storage.WriteRepoConfig(apiKeyRepoSetting, testCase.repoAPIKey))
			}

			exitCode, err := HandleLogin([]string{"--check"})

			require.NoError(t, err)
			require.Equal(t, testCase.expectedExitCode, exitCode)
			require.Equal(t, testCase.expectedAuthKeys, authKeys)
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
