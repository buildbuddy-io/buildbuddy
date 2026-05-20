package ci_runner_util

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_env"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	mockGithubAppID = 1234567890

	owner   = "acme-inc"
	repoURL = "https://github.com/acme-inc/repo"
)

func TestRunnerTimeout(t *testing.T) {
	tests := []struct {
		name                    string
		experimentTimeout       string
		requestedTimeout        time.Duration
		requestedTimeoutPresent bool
		defaultTimeout          time.Duration
		expectedTimeout         time.Duration
	}{
		{
			name:              "experiment set no requested timeout",
			experimentTimeout: "24h",
			expectedTimeout:   24 * time.Hour,
		},
		{
			name:                    "experiment set requested timeout lower",
			experimentTimeout:       "24h",
			requestedTimeout:        1 * time.Hour,
			requestedTimeoutPresent: true,
			expectedTimeout:         1 * time.Hour,
		},
		{
			name:            "no experiment",
			defaultTimeout:  4 * time.Hour,
			expectedTimeout: 4 * time.Hour,
		},
		{
			name:                    "invalid experiment fallback to requested timeout",
			experimentTimeout:       "not-a-duration",
			requestedTimeout:        2 * time.Hour,
			requestedTimeoutPresent: true,
			expectedTimeout:         2 * time.Hour,
		},
		{
			name:              "invalid experiment fallback to default timeout",
			experimentTimeout: "not-a-duration",
			defaultTimeout:    3 * time.Hour,
			expectedTimeout:   3 * time.Hour,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.defaultTimeout != 0 {
				flags.Set(t, "remote_execution.ci_runner_default_timeout", tc.defaultTimeout)
			}

			var efp interfaces.ExperimentFlagProvider
			if tc.experimentTimeout != "" {
				env := enterprise_testenv.New(t)
				configureDefaultTimeoutExperiment(t, env, tc.experimentTimeout)
				efp = env.GetExperimentFlagProvider()
			}

			var requestedTimeout *time.Duration
			if tc.requestedTimeoutPresent {
				requestedTimeout = &tc.requestedTimeout
			}
			timeout, err := RunnerTimeout(ctx, efp, requestedTimeout, "test-action")
			require.NoError(t, err)
			require.Equal(t, tc.expectedTimeout, timeout)
		})
	}
}

func configureDefaultTimeoutExperiment(t *testing.T, env *testenv.TestEnv, timeout string) {
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		DefaultTimeoutExperimentName: {
			State:          memprovider.Enabled,
			DefaultVariant: "on",
			Variants: map[string]any{
				"on": timeout,
			},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))

	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
}

func TestSetTaskRepositoryToken(t *testing.T) {
	env := getEnv(t, "fresh-repo-token")
	ctx := context.Background()

	createLinkedRepo(t, ctx, env)

	task := newRemoteRunnerTask(repoURL, "workflow-api-key", "stale-repo-token")
	err := SetTaskRepositoryToken(ctx, env, task, "group1")
	require.NoError(t, err)

	envOverrides := envOverridesHeader(task)
	require.Equal(t, "fresh-repo-token", envOverrides["REPO_TOKEN"])
	require.Equal(t, "workflow-api-key", envOverrides[ci_runner_env.BuildBuddyAPIKeyEnvVarName])
}

func TestSetTaskRepositoryToken_Failure(t *testing.T) {
	env := getEnv(t, "fresh-repo-token")
	ctx := context.Background()

	// Don't create a repo for the group ID, so the token refresh fails.

	task := newRemoteRunnerTask("https://github.com/acme-inc/repo", "workflow-api-key", "stale-repo-token")
	err := SetTaskRepositoryToken(ctx, env, task, "group1")
	require.Error(t, err)

	envOverrides := envOverridesHeader(task)
	// If token refresh fails, the existing token should be left unchanged.
	require.Equal(t, "stale-repo-token", envOverrides["REPO_TOKEN"])
}

func getEnv(t *testing.T, mockToken string) *testenv.TestEnv {
	flags.Set(t, "github.read_only_app.enabled", true)
	env := enterprise_testenv.New(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"user1":            testauth.User("user1", "group1"),
		"workflow-api-key": testauth.User("user1", "group1"),
	}))
	gh, err := githubapp.NewAppService(env, nil, &testgit.FakeGitHubApp{Token: mockToken, MockAppID: mockGithubAppID, DBHandle: env.GetDBHandle()})
	require.NoError(t, err)
	env.SetGitHubAppService(gh)
	return env
}

func createLinkedRepo(t *testing.T, ctx context.Context, env environment.Env) {
	err := env.GetDBHandle().NewQuery(ctx, "ci_runner_util_test_create_linked_repo").Create(&tables.GitRepository{
		RepoURL: repoURL,
		UserID:  "user1",
		GroupID: "group1",
		Perms:   1,
	})
	require.NoError(t, err)
	err = env.GetDBHandle().NewQuery(ctx, "ci_runner_util_test_create_app_installation").Create(&tables.GitHubAppInstallation{
		GroupID: "group1",
		AppID:   mockGithubAppID,
		Owner:   owner,
	})
	require.NoError(t, err)
}

func newRemoteRunnerTask(repoURL, apiKey, repoToken string) *repb.ExecutionTask {
	overrides := ci_runner_env.BuildBuddyAPIKeyEnvVarName + "=" + apiKey
	if repoToken != "" {
		overrides += ",REPO_TOKEN=" + repoToken
	}
	return &repb.ExecutionTask{
		Command: &repb.Command{
			Arguments: []string{
				"./" + ExecutableName,
				"--pushed_repo_url=" + repoURL,
			},
		},
		PlatformOverrides: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{
					Name:  platform.EnvOverridesPropertyName,
					Value: overrides,
				},
			},
		},
	}
}
