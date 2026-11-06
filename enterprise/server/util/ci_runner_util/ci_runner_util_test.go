package ci_runner_util

import (
	"context"
	"testing"
	"time"

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

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
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
		groupStatus             grpb.Group_GroupStatus
		expectedTimeout         time.Duration
		expectedReason          string
	}{
		{
			name:              "free tier experiment set no requested timeout",
			experimentTimeout: "24h",
			groupStatus:       grpb.Group_FREE_TIER_GROUP_STATUS,
			expectedTimeout:   24 * time.Hour,
			expectedReason:    FreeTierTimeoutReason,
		},
		{
			name:                    "experiment set requested timeout lower",
			experimentTimeout:       "24h",
			requestedTimeout:        1 * time.Hour,
			requestedTimeoutPresent: true,
			groupStatus:             grpb.Group_FREE_TIER_GROUP_STATUS,
			expectedTimeout:         1 * time.Hour,
		},
		{
			name:              "enterprise experiment set no requested timeout",
			experimentTimeout: "24h",
			groupStatus:       grpb.Group_ENTERPRISE_GROUP_STATUS,
			expectedTimeout:   24 * time.Hour,
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
			groupStatus:             grpb.Group_FREE_TIER_GROUP_STATUS,
			expectedTimeout:         2 * time.Hour,
		},
		{
			name:              "invalid experiment fallback to default timeout",
			experimentTimeout: "not-a-duration",
			defaultTimeout:    3 * time.Hour,
			groupStatus:       grpb.Group_FREE_TIER_GROUP_STATUS,
			expectedTimeout:   3 * time.Hour,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			if tc.defaultTimeout != 0 {
				flags.Set(t, "remote_execution.ci_runner_default_timeout", tc.defaultTimeout)
			}

			flags.Set(t, "remote_execution.remote_runner_default_timeout", tc.experimentTimeout)

			var requestedTimeout *time.Duration
			if tc.requestedTimeoutPresent {
				requestedTimeout = &tc.requestedTimeout
			}
			timeout, err := RunnerTimeout(ctx, requestedTimeout, "test-action", tc.groupStatus)
			require.NoError(t, err)
			require.Equal(t, tc.expectedTimeout, timeout.Duration)
			require.Equal(t, tc.expectedReason, timeout.Reason)
		})
	}
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

func TestGitFetchLowSpeedRetryFlags(t *testing.T) {
	tests := []struct {
		name          string
		experiment    map[string]any
		expectedFlags []string
	}{
		{
			// With no experiment configured, no flags should be returned, so
			// the ci_runner falls back to its flag defaults (retries disabled).
			name:          "not configured",
			experiment:    nil,
			expectedFlags: nil,
		},
		{
			// A fully specified config should produce all three fetch flags.
			name:          "all fields set",
			experiment:    map[string]any{"retries": 2, "duration": "45s", "rate": 2048},
			expectedFlags: []string{"--git_fetch_low_speed_retries=2", "--git_fetch_low_speed_limit=2048", "--git_fetch_low_speed_time=45s"},
		},
		{
			// If only retries is set, the rate and duration should be left to
			// the ci_runner's flag defaults.
			name:          "only retries set",
			experiment:    map[string]any{"retries": 1},
			expectedFlags: []string{"--git_fetch_low_speed_retries=1"},
		},
		{
			// A duration the ci_runner would fail to parse should disable the
			// experiment entirely rather than produce partial flags.
			name:          "invalid duration",
			experiment:    map[string]any{"retries": 1, "duration": "not-a-duration"},
			expectedFlags: nil,
		},
		{
			// A zero duration disables Git's low-speed check, so it should be
			// omitted to let the ci_runner use its default duration.
			name:          "zero duration",
			experiment:    map[string]any{"retries": 1, "duration": "0s"},
			expectedFlags: []string{"--git_fetch_low_speed_retries=1"},
		},
		{
			// A negative duration is not meaningful, so it should be omitted to
			// let the ci_runner use its default duration.
			name:          "negative duration",
			experiment:    map[string]any{"retries": 1, "duration": "-1s"},
			expectedFlags: []string{"--git_fetch_low_speed_retries=1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "remote_execution.ci_runner.low_speed_retry_config", tc.experiment)
			flags := GitFetchLowSpeedRetryFlags(t.Context())
			require.Equal(t, tc.expectedFlags, flags)
		})
	}
}

func TestApplyDefaultImageOverride(t *testing.T) {
	const (
		defaultImage  = "docker://gcr.io/example/default:latest"
		overrideImage = "docker://gcr.io/example/override:latest"
		sharedPool    = "Shared-Pool"
	)
	prop := func(name, value string) *repb.Platform_Property {
		return &repb.Platform_Property{Name: name, Value: value}
	}
	for _, tc := range []struct {
		name       string
		override   string
		props      []*repb.Platform_Property
		headers    []*repb.Platform_Property
		userProps  []*repb.Platform_Property
		groupOwned bool
		want       string
	}{
		{name: "no override", want: defaultImage},
		{name: "override applies with mixed-case shared pool", override: overrideImage, want: overrideImage},
		{name: "docker prefix is added", override: "gcr.io/example/override:latest", want: overrideImage},
		{name: "default pool", override: overrideImage, props: []*repb.Platform_Property{prop("Pool", "default")}, want: overrideImage},
		{name: "custom pool", override: overrideImage, props: []*repb.Platform_Property{prop("Pool", "custom")}, want: defaultImage},
		{name: "non-linux OS", override: overrideImage, props: []*repb.Platform_Property{prop("osfamily", "windows")}, want: defaultImage},
		{name: "image is not the default", override: overrideImage, props: []*repb.Platform_Property{prop("container-image", "docker://other")}, want: "docker://other"},
		{name: "user props set the image", override: overrideImage, userProps: []*repb.Platform_Property{prop("Container-Image", defaultImage)}, want: defaultImage},
		{name: "self-hosted", override: overrideImage, props: []*repb.Platform_Property{prop("use-self-hosted-executors", "true")}, want: defaultImage},
		{name: "self-hosted header", override: overrideImage, headers: []*repb.Platform_Property{prop("use-self-hosted-executors", "true")}, want: defaultImage},
		{name: "custom pool header", override: overrideImage, headers: []*repb.Platform_Property{prop("pool", "custom")}, want: defaultImage},
		{name: "group-owned executors", override: overrideImage, groupOwned: true, want: defaultImage},
		{name: "group-owned executors with explicit shared pool", override: overrideImage, groupOwned: true, props: []*repb.Platform_Property{prop("use-self-hosted-executors", "false")}, want: overrideImage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "remote_execution.ci_runner_default_image", tc.override)
			env := enterprise_testenv.New(t)
			ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
				"user1": &testauth.TestUser{UserID: "user1", GroupID: "group1", UseGroupOwnedExecutors: tc.groupOwned},
			})
			env.SetAuthenticator(ta)
			ctx, err := ta.WithAuthenticatedUser(context.Background(), "user1")
			require.NoError(t, err)

			cmd := &repb.Command{Platform: &repb.Platform{Properties: append([]*repb.Platform_Property{
				prop("Pool", sharedPool),
				prop("container-image", defaultImage),
			}, tc.props...)}}
			task := &repb.ExecutionTask{Command: cmd, PlatformOverrides: &repb.Platform{Properties: tc.headers}}
			ApplyDefaultImageOverride(ctx, env, task, tc.userProps, defaultImage, sharedPool, "test")

			props, err := platform.ParseProperties(task)
			require.NoError(t, err)
			require.Equal(t, tc.want, props.ContainerImage)
		})
	}
}
