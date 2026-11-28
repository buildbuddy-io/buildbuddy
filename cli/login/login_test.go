package login

import (
	"bytes"
	"errors"
	"flag"
	stdlog "log"
	"os"
	"os/exec"
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
			setUpRepo(t)

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

// TestResolveAPIKey covers the credential resolution used by `bb login --check`
// and `--allow_existing`: a user whose only credential is in the environment is
// logged in.
func TestResolveAPIKey(t *testing.T) {
	for _, testCase := range []struct {
		name string
		// envAPIKey is the value of BUILDBUDDY_API_KEY, empty when the
		// environment has no key.
		envAPIKey string
		// repoAPIKey is written to .git/config when non-nil. It is a pointer so
		// that "absent" and "present but empty" are distinguishable; an empty
		// value is a state real repos are in, and must be treated as unset.
		repoAPIKey     *string
		expected       string
		expectedSource apiKeySource
	}{
		{
			name:           "env only",
			envAPIKey:      "env-api-key",
			expected:       "env-api-key",
			expectedSource: apiKeySourceEnv,
		},
		{
			name:           ".git/config only",
			repoAPIKey:     new("repo-api-key"),
			expected:       "repo-api-key",
			expectedSource: apiKeySourceRepo,
		},
		{
			name:           "env takes precedence over .git/config",
			envAPIKey:      "env-api-key",
			repoAPIKey:     new("repo-api-key"),
			expected:       "env-api-key",
			expectedSource: apiKeySourceEnv,
		},
		{
			name:           "neither set",
			expected:       "",
			expectedSource: apiKeySourceNone,
		},
		{
			name:           "empty .git/config value is treated as unset",
			repoAPIKey:     new(""),
			expected:       "",
			expectedSource: apiKeySourceNone,
		},
		{
			name:           "whitespace-only env value is treated as unset",
			envAPIKey:      "   ",
			expected:       "",
			expectedSource: apiKeySourceNone,
		},
		{
			name:           "surrounding whitespace is trimmed",
			envAPIKey:      "  env-api-key\n",
			expected:       "env-api-key",
			expectedSource: apiKeySourceEnv,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repoRoot := setUpRepo(t)
			t.Setenv("BUILDBUDDY_API_KEY", testCase.envAPIKey)
			if testCase.repoAPIKey != nil {
				require.NoError(t, storage.WriteRepoConfig(apiKeyRepoSetting, *testCase.repoAPIKey))
				// Guard the setup itself: writing "" must leave the key present
				// with an empty value, not remove it, or this case silently
				// duplicates "neither set".
				require.True(t, gitConfigHasKey(t, repoRoot, gitConfigAPIKeyName),
					"expected %s to be present in .git/config", gitConfigAPIKeyName)
			}

			apiKey, source, err := resolveAPIKey()

			require.NoError(t, err)
			require.Equal(t, testCase.expected, apiKey)
			require.Equal(t, testCase.expectedSource, source)
		})
	}
}

// --check must report on the credentials a build would use, not just on
// .git/config. This covers the command's exit codes; TestResolveAPIKey covers
// resolution on its own.
func TestHandleLoginCheck(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		envAPIKey        string
		repoAPIKey       *string
		authErr          error
		expectedExitCode int
		// expectedAuthKey is the key passed to the authenticator.
		expectedAuthKey string
	}{
		{
			name:             "valid key in env only",
			envAPIKey:        "env-api-key",
			expectedExitCode: 0,
			expectedAuthKey:  "env-api-key",
		},
		{
			name:             "valid key in .git/config only",
			repoAPIKey:       new("repo-api-key"),
			expectedExitCode: 0,
			expectedAuthKey:  "repo-api-key",
		},
		{
			name:             "env key takes precedence",
			envAPIKey:        "env-api-key",
			repoAPIKey:       new("repo-api-key"),
			expectedExitCode: 0,
			expectedAuthKey:  "env-api-key",
		},
		{
			name:             "rejected key reports invalid",
			envAPIKey:        "env-api-key",
			authErr:          status.UnauthenticatedError("invalid API key"),
			expectedExitCode: 1,
			expectedAuthKey:  "env-api-key",
		},
		{
			name:             "unreachable server reports an error, not an answer",
			envAPIKey:        "env-api-key",
			authErr:          status.UnavailableError("connection refused"),
			expectedExitCode: 2,
			expectedAuthKey:  "env-api-key",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repoRoot := setUpRepo(t)
			resetLoginFlags(t)
			t.Setenv("BUILDBUDDY_API_KEY", testCase.envAPIKey)
			if testCase.repoAPIKey != nil {
				require.NoError(t, storage.WriteRepoConfig(apiKeyRepoSetting, *testCase.repoAPIKey))
			}
			authKeys := stubAuthenticate(t, testCase.authErr)

			exitCode, err := HandleLogin([]string{"--check"})

			require.NoError(t, err)
			require.Equal(t, testCase.expectedExitCode, exitCode)
			require.Equal(t, []string{testCase.expectedAuthKey}, *authKeys)
			// --check never writes anything.
			requireRepoAPIKey(t, repoRoot, testCase.repoAPIKey)
		})
	}
}

// --allow_existing skips login when the credentials a build would use are
// valid, and stops without logging in when they can't be checked or when a
// rejected key came from the environment. None of these cases may write to
// .git/config.
func TestHandleLoginAllowExisting(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		authErr          error
		expectedExitCode int
	}{
		{
			name:             "valid key in env only skips login",
			expectedExitCode: 0,
		},
		{
			name:             "unreachable server exits without logging in",
			authErr:          status.UnavailableError("connection refused"),
			expectedExitCode: 2,
		},
		{
			// A new key in .git/config would be shadowed by the env key.
			name:             "rejected env key exits without logging in",
			authErr:          status.UnauthenticatedError("invalid API key"),
			expectedExitCode: 1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			repoRoot := setUpRepo(t)
			resetLoginFlags(t)
			t.Setenv("BUILDBUDDY_API_KEY", "env-api-key")
			authKeys := stubAuthenticate(t, testCase.authErr)

			exitCode, err := HandleLogin([]string{"--allow_existing"})

			require.NoError(t, err)
			require.Equal(t, testCase.expectedExitCode, exitCode)
			require.Equal(t, []string{"env-api-key"}, *authKeys)
			requireRepoAPIKey(t, repoRoot, nil)
		})
	}
}

func TestLogSavedKeyStatus(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		envAPIKey string
		warns     bool
	}{
		{name: "no env key", envAPIKey: "", warns: false},
		{name: "env key matches the saved key", envAPIKey: "saved-api-key", warns: false},
		{name: "env key matches after trimming", envAPIKey: " saved-api-key\n", warns: false},
		{name: "different env key", envAPIKey: "other-api-key", warns: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("BUILDBUDDY_API_KEY", testCase.envAPIKey)
			logs := captureLogs(t)

			logSavedKeyStatus("saved-api-key")

			if testCase.warns {
				require.Contains(t, logs.String(), "builds will keep using that key")
				require.NotContains(t, logs.String(), "You are now building with BuildBuddy!")
			} else {
				require.Contains(t, logs.String(), "You are now building with BuildBuddy!")
				require.NotContains(t, logs.String(), "BUILDBUDDY_API_KEY")
			}
		})
	}
}

func setUpRepo(t *testing.T) string {
	t.Helper()

	isolateHomeDir(t)

	repoRoot, _ := testgit.MakeTempRepo(t, map[string]string{
		"README.md": "# test repo",
	})
	stubRepoRootPath(t, func() (string, error) {
		return repoRoot, nil
	})
	return repoRoot
}

// isolateHomeDir points the user config and cache dirs at a temp dir so that
// nothing under test can read or write the real user's files.
func isolateHomeDir(t *testing.T) {
	t.Helper()

	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
}

func stubRepoRootPath(t *testing.T, fn func() (string, error)) {
	t.Helper()

	previous := storage.RepoRootPath
	storage.RepoRootPath = fn
	t.Cleanup(func() {
		storage.RepoRootPath = previous
	})
}

// stubAuthenticate replaces the credential check with one that records the keys
// it was given and returns err, so the --check paths can be driven without a
// network round-trip.
func stubAuthenticate(t *testing.T, err error) *[]string {
	t.Helper()

	var keys []string
	previous := authenticateFn
	authenticateFn = func(apiKey string) error {
		keys = append(keys, apiKey)
		return err
	}
	t.Cleanup(func() {
		authenticateFn = previous
	})
	return &keys
}

// HandleLogin parses into a package-global flag set, so a flag set by one test
// would otherwise leak into every test that runs after it.
func resetLoginFlags(t *testing.T) {
	t.Helper()

	t.Cleanup(func() {
		flags.VisitAll(func(f *flag.Flag) {
			f.Value.Set(f.DefValue)
		})
	})
}

// requireRepoAPIKey asserts that .git/config holds exactly the given API key,
// or no key at all when it is nil.
func requireRepoAPIKey(t *testing.T, repoRoot string, expected *string) {
	t.Helper()

	require.Equal(t, expected != nil, gitConfigHasKey(t, repoRoot, gitConfigAPIKeyName))
	if expected != nil {
		apiKey, err := storage.ReadRepoConfig(apiKeyRepoSetting)
		require.NoError(t, err)
		require.Equal(t, *expected, apiKey)
	}
}

// captureLogs redirects the CLI's log output, which goes through the stdlib log
// package, into a buffer for the duration of the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	previous := stdlog.Writer()
	stdlog.SetOutput(&buf)
	t.Cleanup(func() {
		stdlog.SetOutput(previous)
	})
	return &buf
}

// gitConfigHasKey reports whether key is present in the repo-local git config,
// which reading the config file as text cannot distinguish from an empty value.
func gitConfigHasKey(t *testing.T, repoRoot, key string) bool {
	t.Helper()

	cmd := exec.Command("git", "config", "--local", "--get", key)
	cmd.Dir = repoRoot
	err := cmd.Run()
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	require.True(t, errors.As(err, &exitErr), "unexpected error running git config: %s", err)
	require.Equal(t, 1, exitErr.ExitCode(), "unexpected git config exit code")
	return false
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
