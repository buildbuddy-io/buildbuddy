package login

import (
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

func TestConfigureBESAPIKey(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		args               []string
		expectedBESHeaders []string
	}{
		{
			name:               "remote_header only",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "bes_header already set",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_header=X-BuildBuddy-API-Key=", "--bes_backend=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: []string{"X-BuildBuddy-API-Key="},
		},
		{
			name:               "non-BuildBuddy bes_backend",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://bes.example.com", "--remote_cache=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "self-hosted bes_backend matching remote_cache",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=bb.example.com:1985", "--remote_cache=grpc://bb.example.com"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "unix socket bes_backend",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=unix:/a.sock", "--remote_cache=unix:/b.sock"},
			expectedBESHeaders: nil,
		},
		{
			name:               "no bes_backend",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--remote_cache=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			homeDir := t.TempDir()
			t.Setenv("HOME", homeDir)
			t.Setenv("USERPROFILE", homeDir)

			args, err := arg.NewBazelArgs(append([]string{"build", "//foo:bar"}, testCase.args...))
			require.NoError(t, err)
			err = ConfigureBESAPIKey(args)

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBESHeaders, args.GetAllFlagsWithName("bes_header"))
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
