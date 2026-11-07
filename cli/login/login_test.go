package login

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/credentialhelper"
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
		credentialHelper  string
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
		{
			name:             "credential helper and no API key",
			credentialHelper: "/path/to/helper",
			expectedArgs:     []string{"--ignore_all_rc_files", "build", "//foo:bar"},
		},
		{
			name:             "credential helper and env API key",
			envAPIKey:        "env-api-key",
			credentialHelper: "/path/to/helper",
			expectedAPIKey:   "env-api-key",
			expectedArgs: []string{
				"--ignore_all_rc_files",
				"build",
				"--remote_header=x-buildbuddy-api-key=env-api-key",
				"//foo:bar",
			},
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
			t.Setenv(credentialhelper.EnvVar, testCase.credentialHelper)
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

func TestConfigureCredentialHelper(t *testing.T) {
	helper := filepath.Join(t.TempDir(), "helper")
	require.NoError(t, os.WriteFile(helper, []byte("#!/bin/sh\n"), 0o755))

	for _, testCase := range []struct {
		name             string
		credentialHelper string
		args             []string
		expectedArgs     []string
		expectedErr      string
	}{
		{
			name:         "no helper configured",
			args:         []string{"build", "--bes_backend=grpcs://bes.example.com", "//foo:bar"},
			expectedArgs: []string{"--ignore_all_rc_files", "build", "--bes_backend=grpcs://bes.example.com", "//foo:bar"},
		},
		{
			name:             "unsupported command",
			credentialHelper: helper,
			args:             []string{"version"},
			expectedArgs:     []string{"--ignore_all_rc_files", "version"},
		},
		{
			name:             "one helper per host",
			credentialHelper: helper,
			args:             []string{"build", "--bes_backend=grpcs://remote.example.com", "--remote_cache=remote.example.com", "//foo:bar"},
			expectedArgs: []string{
				"--ignore_all_rc_files",
				"build",
				"--bes_backend=grpcs://remote.example.com",
				"--remote_cache=remote.example.com",
				"--credential_helper=remote.example.com=" + helper,
				"//foo:bar",
			},
		},
		{
			name:             "skips hostless targets",
			credentialHelper: helper,
			args:             []string{"build", "--bes_backend=grpcs://bes.example.com:443", "--remote_executor=unix:///tmp/executor.sock", "//foo:bar"},
			expectedArgs: []string{
				"--ignore_all_rc_files",
				"build",
				"--bes_backend=grpcs://bes.example.com:443",
				"--remote_executor=unix:///tmp/executor.sock",
				"--credential_helper=bes.example.com=" + helper,
				"//foo:bar",
			},
		},
		{
			name:             "keeps a scope the user configured",
			credentialHelper: helper,
			args:             []string{"build", "--bes_backend=grpcs://bes.example.com", "--remote_cache=grpcs://cache.example.com", "--credential_helper=bes.example.com=/usr/bin/mine", "//foo:bar"},
			expectedArgs: []string{
				"--ignore_all_rc_files",
				"build",
				"--bes_backend=grpcs://bes.example.com",
				"--remote_cache=grpcs://cache.example.com",
				"--credential_helper=bes.example.com=/usr/bin/mine",
				"--credential_helper=cache.example.com=" + helper,
				"//foo:bar",
			},
		},
		{
			name:             "helper not found",
			credentialHelper: filepath.Join(t.TempDir(), "missing"),
			args:             []string{"build", "--bes_backend=grpcs://bes.example.com", "//foo:bar"},
			expectedErr:      "failed to find credential helper",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv(credentialhelper.EnvVar, testCase.credentialHelper)
			args, err := arg.NewBazelArgs(testCase.args)
			require.NoError(t, err)

			err = ConfigureCredentialHelper(args)
			if testCase.expectedErr != "" {
				require.ErrorContains(t, err, testCase.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expectedArgs, args.Resolved())
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
