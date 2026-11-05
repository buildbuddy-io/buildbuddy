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
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=bb.example.com:1985", "--remote_cache=grpcs://bb.example.com:1985"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "self-hosted bes_backend matching remote_executor",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://bb.example.com:1985", "--remote_executor=grpc://bb.example.com:1985"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "comma list with attacker host first",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://attacker.example,remote.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "comma list with attacker host last",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io,grpcs://attacker.example"},
			expectedBESHeaders: nil,
		},
		{
			name:               "comma list of BuildBuddy hosts",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io,grpcs://other.buildbuddy.io"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "comma list of BuildBuddy and remote_cache hosts",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=remote.buildbuddy.io,grpcs://bb.example.com", "--remote_cache=bb.example.com"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "comma list with leading empty entry",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=,remote.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "comma list with trailing empty entry",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=remote.buildbuddy.io,"},
			expectedBESHeaders: nil,
		},
		{
			name:               "comma list with unparseable entry",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=remote.buildbuddy.io,grpcs://bad host"},
			expectedBESHeaders: nil,
		},
		{
			name:               "comma list with unix socket entry",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=remote.buildbuddy.io,unix:/a.sock"},
			expectedBESHeaders: nil,
		},
		{
			name:               "scheme mismatch",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://bb.example.com:443", "--remote_cache=grpcs://bb.example.com:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "scheme mismatch with omitted scheme",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=bb.example.com:1985", "--remote_cache=grpc://bb.example.com:1985"},
			expectedBESHeaders: nil,
		},
		{
			name:               "port mismatch",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://bb.example.com:1985", "--remote_cache=grpcs://bb.example.com:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "scheme and port mismatch",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://bb.example.com:1985", "--remote_cache=grpcs://bb.example.com:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "grpcs default port matches explicit 443",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://bb.example.com", "--remote_cache=bb.example.com:443"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "explicit 443 matches grpcs default port",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=bb.example.com:443", "--remote_executor=grpcs://bb.example.com"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "grpc default port matches explicit 80",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://bb.example.com", "--remote_cache=grpc://bb.example.com:80"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "grpc default port does not match 443",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://bb.example.com", "--remote_cache=grpc://bb.example.com:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "plaintext BuildBuddy bes_backend",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://remote.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "plaintext BuildBuddy bes_backend with port",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://remote.buildbuddy.io:1985"},
			expectedBESHeaders: nil,
		},
		{
			name:               "non-gRPC scheme BuildBuddy bes_backend",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=https://remote.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "lookalike domain with prefix",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://notbuildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "lookalike domain with suffix",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://buildbuddy.io.evil.com"},
			expectedBESHeaders: nil,
		},
		{
			name:               "unix socket bes_backend matching remote_cache",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=unix:/a.sock", "--remote_cache=unix:/a.sock"},
			expectedBESHeaders: nil,
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
		{
			name:               "multi-colon host with inner grpc scheme",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://x.buildbuddy.io:grpc://@attacker.example:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "multi-colon host with inner grpcs scheme",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://x.buildbuddy.io:grpcs://@attacker.example:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "multi-colon host",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://x.buildbuddy.io:attacker.example:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "multi-colon host without scheme",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=x.buildbuddy.io:attacker.example:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "multi-colon host in comma list",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=remote.buildbuddy.io,grpcs://x.buildbuddy.io:grpc://@attacker.example:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "multi-colon host ending in colon",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://x.buildbuddy.io:grpc:"},
			expectedBESHeaders: nil,
		},
		{
			name:               "multi-colon target identical to remote_cache",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://x.buildbuddy.io:grpc://@attacker.example:443", "--remote_cache=grpcs://x.buildbuddy.io:grpc://@attacker.example:443"},
			expectedBESHeaders: nil,
		},
		{
			name:               "multi-colon target identical to remote_executor",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://bb.example.com:grpc:", "--remote_executor=grpcs://bb.example.com:grpc:"},
			expectedBESHeaders: nil,
		},
		{
			name:               "userinfo on BuildBuddy host",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://user:pw@remote.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "BuildBuddy host as userinfo",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io@attacker.example"},
			expectedBESHeaders: nil,
		},
		{
			name:               "userinfo identical to remote_cache",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://user@bb.example.com", "--remote_cache=grpcs://user@bb.example.com"},
			expectedBESHeaders: nil,
		},
		{
			name:               "path",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io/foo"},
			expectedBESHeaders: nil,
		},
		{
			name:               "path identical to remote_cache",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://bb.example.com/foo", "--remote_cache=grpcs://bb.example.com/foo"},
			expectedBESHeaders: nil,
		},
		{
			name:               "query",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io?x=1"},
			expectedBESHeaders: nil,
		},
		{
			name:               "empty query",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io?"},
			expectedBESHeaders: nil,
		},
		{
			name:               "fragment",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io#x"},
			expectedBESHeaders: nil,
		},
		{
			name:               "empty fragment",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io#"},
			expectedBESHeaders: nil,
		},
		{
			name:               "port zero",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io:0"},
			expectedBESHeaders: nil,
		},
		{
			name:               "port out of range",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io:65536"},
			expectedBESHeaders: nil,
		},
		{
			name:               "huge port",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io:99999999999999999999"},
			expectedBESHeaders: nil,
		},
		{
			name:               "non-numeric port",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io:foo"},
			expectedBESHeaders: nil,
		},
		{
			name:               "non-ASCII host",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://bücher.buildbuddy.io"},
			expectedBESHeaders: nil,
		},
		{
			name:               "IPv6 identical to remote_cache",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://[::1]:1985", "--remote_cache=grpc://[::1]:1985"},
			expectedBESHeaders: nil,
		},
		{
			name:               "trailing slash",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io/"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "highest port",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io:65535"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "apex domain",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpcs://buildbuddy.io"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "IPv4 identical to remote_cache",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://10.0.0.1:1985", "--remote_cache=grpc://10.0.0.1:1985"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "IPv4 differs from remote_cache",
			args:               []string{"--remote_header=x-buildbuddy-api-key=KEY", "--bes_backend=grpc://10.0.0.1:1985", "--remote_cache=grpc://10.0.0.2:1985"},
			expectedBESHeaders: nil,
		},
		{
			name:               "mixed-case remote_header key",
			args:               []string{"--remote_header=X-BuildBuddy-API-Key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
		},
		{
			name:               "mixed-case remote_header key with bes_header already set",
			args:               []string{"--remote_header=X-BuildBuddy-API-Key=KEY", "--bes_header=x-buildbuddy-api-key=OTHER", "--bes_backend=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=OTHER"},
		},
		{
			name:               "last remote_header key wins across case",
			args:               []string{"--remote_header=x-buildbuddy-api-key=FIRST", "--remote_header=X-BuildBuddy-API-Key=KEY", "--bes_backend=grpcs://remote.buildbuddy.io"},
			expectedBESHeaders: []string{"x-buildbuddy-api-key=KEY"},
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
