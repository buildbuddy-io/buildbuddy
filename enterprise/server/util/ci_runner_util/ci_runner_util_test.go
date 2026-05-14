package ci_runner_util

import (
	"context"
	"testing"

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
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	mockGithubAppID = 1234567890

	owner   = "acme-inc"
	repoURL = "https://github.com/acme-inc/repo"
)

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
	gh, err := githubapp.NewAppService(env, nil, &testgit.FakeGitHubApp{Token: mockToken, MockAppID: mockGithubAppID})
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
