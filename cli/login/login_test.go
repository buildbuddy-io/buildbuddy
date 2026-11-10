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

func TestConfigureBESAPIKey(t *testing.T) {
	besHeaders := func(t *testing.T, flags []string) []string {
		t.Helper()
		homeDir := t.TempDir()
		t.Setenv("HOME", homeDir)
		t.Setenv("USERPROFILE", homeDir)

		args, err := arg.NewBazelArgs(append([]string{"build", "//foo:bar"}, flags...))
		require.NoError(t, err)
		require.NoError(t, ConfigureBESAPIKey(args))
		return args.GetAllFlagsWithName("bes_header")
	}

	// Cases exercising how the remote_header and bes_header flags are handled.
	for _, testCase := range []struct {
		name               string
		args               []string
		expectedBESHeaders []string
	}{
		{
			name:               "bes_header already set",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_header=X-BuildBuddy-API-Key=", "--bes_backend=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: []string{"X-BuildBuddy-API-Key="},
		},
		{
			name:               "later empty remote_header key clears across case",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--remote_header=X-BuildBuddy-API-Key=", "--bes_backend=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "other remote_header after key is ignored",
			args:               []string{"--remote_header=X-BuildBuddy-API-Key=KEY", "--remote_header=x-other=OTHER", "--bes_backend=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expectedBESHeaders, besHeaders(t, testCase.args))
		})
	}

	// Cases exercising which BES backends may receive the key. An empty string
	// leaves the corresponding flag unset.
	for _, testCase := range []struct {
		name, bes, cache, executor string
		want                       bool
	}{
		{"on-prem cache and Cloud BES", "grpcs://remote.buildbuddy.io", "grpcs://bb.corp", "", false},
		{"no cache or executor and Cloud BES", "grpcs://remote.buildbuddy.io", "", "", true},
		{"Cloud cache, on-prem executor and other Cloud BES", "grpcs://remote.buildbuddy.io", "grpcs://cache.buildbuddy.io", "grpcs://bb.corp", false},
		{"Cloud cache, on-prem executor and BES identical to cache", "grpcs://remote.buildbuddy.io", "grpcs://remote.buildbuddy.io", "grpcs://bb.corp", true},
		{"unparseable cache and Cloud BES", "grpcs://remote.buildbuddy.io", "unix:/a.sock", "", false},
		{"plaintext Cloud executor and Cloud BES", "grpcs://remote.buildbuddy.io", "", "grpc://remote.buildbuddy.io", false},

		{"non-BuildBuddy bes_backend", "grpcs://bes.example.com", "grpcs://remote.buildbuddy.io", "", false},
		{"self-hosted bes_backend matching remote_cache", "bb.example.com:1985", "grpcs://bb.example.com:1985", "", true},
		{"self-hosted bes_backend matching remote_executor", "grpc://bb.example.com:1985", "", "grpc://bb.example.com:1985", true},
		{"no bes_backend", "", "grpcs://remote.buildbuddy.io", "", false},

		{"comma list with attacker host last", "grpcs://remote.buildbuddy.io,grpcs://attacker.example", "", "", false},
		{"comma list of BuildBuddy hosts", "grpcs://remote.buildbuddy.io,grpcs://other.buildbuddy.io", "", "", true},
		{"comma list of BuildBuddy and on-prem remote_cache hosts", "remote.buildbuddy.io,grpcs://bb.example.com", "bb.example.com", "", false},
		{"comma list with trailing empty entry", "remote.buildbuddy.io,", "", "", false},

		{"scheme mismatch with omitted scheme", "bb.example.com:1985", "grpc://bb.example.com:1985", "", false},
		{"port mismatch", "grpcs://bb.example.com:1985", "grpcs://bb.example.com:443", "", false},
		{"grpcs default port matches explicit 443", "grpcs://bb.example.com", "bb.example.com:443", "", true},
		{"grpc default port matches explicit 80", "grpc://bb.example.com", "grpc://bb.example.com:80", "", true},

		{"plaintext BuildBuddy bes_backend", "grpc://remote.buildbuddy.io", "", "", false},
		{"non-gRPC scheme BuildBuddy bes_backend", "https://remote.buildbuddy.io", "", "", false},
		{"lookalike domain with prefix", "grpcs://notbuildbuddy.io", "", "", false},
		{"lookalike domain with suffix", "grpcs://buildbuddy.io.evil.com", "", "", false},
		{"apex domain", "grpcs://buildbuddy.io", "", "", true},
		{"unix socket bes_backend matching remote_cache", "unix:/a.sock", "unix:/a.sock", "", false},

		{"multi-colon host", "grpcs://x.buildbuddy.io:attacker.example:443", "", "", false},
		{"userinfo on BuildBuddy host", "grpcs://user:pw@remote.buildbuddy.io", "", "", false},
		{"path", "grpcs://remote.buildbuddy.io/foo", "", "", false},
		{"query", "grpcs://remote.buildbuddy.io?x=1", "", "", false},
		{"trailing slash", "grpcs://remote.buildbuddy.io/", "", "", true},

		{"port zero", "grpcs://remote.buildbuddy.io:0", "", "", false},
		{"port out of range", "grpcs://remote.buildbuddy.io:65536", "", "", false},
		{"huge port", "grpcs://remote.buildbuddy.io:99999999999999999999", "", "", false},
		{"non-numeric port", "grpcs://remote.buildbuddy.io:foo", "", "", false},
		{"highest port", "grpcs://remote.buildbuddy.io:65535", "", "", true},

		{"non-ASCII host", "grpcs://bücher.buildbuddy.io", "", "", false},
		{"IPv6 identical to remote_cache", "grpc://[::1]:1985", "grpc://[::1]:1985", "", false},
		{"IPv4 identical to remote_cache", "grpc://10.0.0.1:1985", "grpc://10.0.0.1:1985", "", true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			flags := []string{"--remote_header=x-buildbuddy-api-key=KEY"}
			for _, flag := range [][2]string{{"bes_backend", testCase.bes}, {"remote_cache", testCase.cache}, {"remote_executor", testCase.executor}} {
				if flag[1] != "" {
					flags = append(flags, "--"+flag[0]+"="+flag[1])
				}
			}
			var expected []string
			if testCase.want {
				expected = []string{"x-buildbuddy-api-key=KEY"}
			}
			require.Equal(t, expected, besHeaders(t, flags))
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
