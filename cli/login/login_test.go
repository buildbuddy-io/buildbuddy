package login

import (
	"bytes"
	"errors"
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

// TestResolveAPIKey covers the credential resolution used by `bb login --check`
// and `--allow_existing`: a user whose only credential is in the environment is
// logged in.
func TestResolveAPIKey(t *testing.T) {
	for _, testCase := range []struct {
		name string
		// envAPIKey is the value of BUILDBUDDY_API_KEY, unset when empty.
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

// Outside a git repo there is no repo-local key to read, but an API key in the
// environment is still usable, so this must not be an error.
func TestResolveAPIKeyOutsideGitRepo(t *testing.T) {
	stubRepoRootPath(t, func() (string, error) {
		return "", storage.ErrNotInRepo
	})

	t.Run("env set", func(t *testing.T) {
		isolateHomeDir(t)
		t.Setenv("BUILDBUDDY_API_KEY", "env-api-key")

		apiKey, source, err := resolveAPIKey()

		require.NoError(t, err)
		require.Equal(t, "env-api-key", apiKey)
		require.Equal(t, apiKeySourceEnv, source)
	})

	t.Run("env unset", func(t *testing.T) {
		isolateHomeDir(t)
		t.Setenv("BUILDBUDDY_API_KEY", "")

		apiKey, source, err := resolveAPIKey()

		require.NoError(t, err)
		require.Empty(t, apiKey)
		require.Equal(t, apiKeySourceNone, source)
	})
}

// A failure to reach .git/config that is not "there is no repo" must not be
// reported as "there is no key": a broken environment would look like an
// unconfigured one.
func TestResolveAPIKeyReturnsUnexpectedRepoErrors(t *testing.T) {
	isolateHomeDir(t)
	t.Setenv("BUILDBUDDY_API_KEY", "")
	stubRepoRootPath(t, func() (string, error) {
		return "", errors.New("detected dubious ownership in repository")
	})

	_, source, err := resolveAPIKey()

	require.Error(t, err)
	require.Contains(t, err.Error(), "dubious ownership")
	require.Equal(t, apiKeySourceNone, source)
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
		// expectedAuthKey is the key passed to the authenticator, or "" if it
		// must not be called at all.
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
		{
			// The answer is knowable locally, so --check must not dial, and must
			// not turn an offline transport failure into exit 2.
			name:             "no credential anywhere reports invalid without dialing",
			expectedExitCode: 1,
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
			if testCase.expectedAuthKey == "" {
				require.Empty(t, *authKeys, "authenticator should not have been called")
			} else {
				require.Equal(t, []string{testCase.expectedAuthKey}, *authKeys)
			}
			// --check never writes anything.
			require.Equal(t, testCase.repoAPIKey != nil, gitConfigHasKey(t, repoRoot, gitConfigAPIKeyName))
		})
	}
}

// Asking for both must not silently do one of them.
func TestHandleLoginRejectsCheckWithAllowExisting(t *testing.T) {
	setUpRepo(t)
	resetLoginFlags(t)
	t.Setenv("BUILDBUDDY_API_KEY", "env-api-key")
	authKeys := stubAuthenticate(t, nil)

	exitCode, err := HandleLogin([]string{"--check", "--allow_existing"})

	require.NoError(t, err)
	require.Equal(t, 2, exitCode)
	require.Empty(t, *authKeys, "no credential check should have been attempted")
}

// Skipping login is right when the existing credentials work, but a silent skip
// gives no hint that no key was written.
func TestHandleLoginAllowExistingReportsWhichCredentialItUsed(t *testing.T) {
	setUpRepo(t)
	resetLoginFlags(t)
	t.Setenv("BUILDBUDDY_API_KEY", "env-api-key")
	stubAuthenticate(t, nil)
	logs := captureLogs(t)

	exitCode, err := HandleLogin([]string{"--allow_existing"})

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	require.Contains(t, logs.String(), "Already logged in using $BUILDBUDDY_API_KEY")
}

func TestHandleLogoutRemovesAPIKey(t *testing.T) {
	repoRoot := setUpRepo(t)
	t.Setenv("BUILDBUDDY_API_KEY", "")
	require.NoError(t, storage.WriteRepoConfig(apiKeyRepoSetting, "repo-api-key"))

	exitCode, err := HandleLogout(nil)

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	// `api-key =` contains the substring "api-key", so a text search cannot tell
	// "removed" from "present with an empty value".
	require.False(t, gitConfigHasKey(t, repoRoot, gitConfigAPIKeyName))
}

// Logout can only clear what bb wrote, so whether it may claim to have logged
// the user out depends on the environment.
func TestHandleLogoutWarnsAboutEnvironmentKey(t *testing.T) {
	t.Run("env key still set", func(t *testing.T) {
		setUpRepo(t)
		t.Setenv("BUILDBUDDY_API_KEY", "env-api-key")
		logs := captureLogs(t)

		exitCode, err := HandleLogout(nil)

		require.NoError(t, err)
		require.Equal(t, 0, exitCode)
		require.Contains(t, logs.String(), "BUILDBUDDY_API_KEY is still set")
		require.NotContains(t, logs.String(), "You are now logged out!")
	})

	t.Run("no env key", func(t *testing.T) {
		setUpRepo(t)
		t.Setenv("BUILDBUDDY_API_KEY", "")
		logs := captureLogs(t)

		exitCode, err := HandleLogout(nil)

		require.NoError(t, err)
		require.Equal(t, 0, exitCode)
		require.NotContains(t, logs.String(), "BUILDBUDDY_API_KEY")
		require.Contains(t, logs.String(), "You are now logged out!")
	})

	// A whitespace-only value is not a usable credential. Warning here would
	// tell the user they are still logged in when they are not.
	t.Run("whitespace-only env key", func(t *testing.T) {
		setUpRepo(t)
		t.Setenv("BUILDBUDDY_API_KEY", "   ")
		logs := captureLogs(t)

		exitCode, err := HandleLogout(nil)

		require.NoError(t, err)
		require.Equal(t, 0, exitCode)
		require.NotContains(t, logs.String(), "BUILDBUDDY_API_KEY")
		require.Contains(t, logs.String(), "You are now logged out!")
	})
}

// Outside a git repo there is nothing repo-local to clear, and the environment
// warning must still run.
func TestHandleLogoutOutsideGitRepo(t *testing.T) {
	isolateHomeDir(t)
	stubRepoRootPath(t, func() (string, error) {
		return "", storage.ErrNotInRepo
	})
	t.Setenv("BUILDBUDDY_API_KEY", "env-api-key")
	logs := captureLogs(t)

	exitCode, err := HandleLogout(nil)

	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	require.Contains(t, logs.String(), "BUILDBUDDY_API_KEY is still set")
}

// `bb logout --help` must print usage, not log the user out.
func TestHandleLogoutHelpDoesNotLogOut(t *testing.T) {
	repoRoot := setUpRepo(t)
	require.NoError(t, storage.WriteRepoConfig(apiKeyRepoSetting, "repo-api-key"))
	logs := captureLogs(t)

	exitCode, err := HandleLogout([]string{"--help"})

	require.NoError(t, err)
	require.Equal(t, 1, exitCode)
	require.Contains(t, logs.String(), "bb logout")
	require.True(t, gitConfigHasKey(t, repoRoot, gitConfigAPIKeyName),
		"the saved API key should still be present")
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

// HandleLogin and HandleLogout parse into package-global flag sets, so a flag
// set by one test would otherwise leak into every test that runs after it.
func resetLoginFlags(t *testing.T) {
	t.Helper()

	t.Cleanup(func() {
		*check = false
		*allowExisting = false
	})
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
