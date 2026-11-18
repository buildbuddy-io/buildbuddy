package hostedrunner

import (
	"context"
	"encoding/base64"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_util"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"

	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	bspb "github.com/buildbuddy-io/buildbuddy/proto/bytestream"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
)

func getEnv(t *testing.T) (*testenv.TestEnv, context.Context) {
	// Avoid uploading embedded CI runner binaries to the in-memory cache in
	// each test because it's very slow. The executor will add these binaries locally instead.
	flags.Set(t, "remote_execution.init_ci_runner_from_cache", false)

	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	ctx := context.Background()

	execClient := NewFakeExecutionClient()
	te.SetRemoteExecutionClient(execClient)

	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(te /*sslService=*/, nil)
	require.NoError(t, err)
	bsServer, err := byte_stream_server.NewByteStreamServer(te)
	require.NoError(t, err)

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, te)
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)
	bspb.RegisterByteStreamServer(grpcServer, bsServer)

	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(t, ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { clientConn.Close() })

	te.SetByteStreamClient(bspb.NewByteStreamClient(clientConn))
	te.SetWorkflowService(workflow.NewWorkflowService(te))

	tu := &tables.User{
		UserID: "US1",
		Email:  "US1@org1.io",
		SubID:  "US1-SubID",
	}
	err = te.GetUserDB().InsertUser(context.Background(), tu)
	require.NoError(t, err)

	auth := te.GetAuthenticator().(*testauth.TestAuthenticator)
	authCtx, err := auth.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	jwt, err := auth.TestJWTForUserID("US1")
	require.NoError(t, err)
	authCtx = metadata.AppendToOutgoingContext(authCtx, authutil.ContextTokenStringKey, jwt)

	return te, authCtx
}

type fakeExecutionClient struct {
	repb.ExecutionClient
	executeRequests []*executeRequest
}

type executeRequest struct {
	Metadata metadata.MD
	Payload  *repb.ExecuteRequest
}

func NewFakeExecutionClient() *fakeExecutionClient {
	return &fakeExecutionClient{
		executeRequests: make([]*executeRequest, 0),
	}
}

func (c *fakeExecutionClient) Execute(ctx context.Context, req *repb.ExecuteRequest, opts ...grpc.CallOption) (repb.Execution_ExecuteClient, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	c.executeRequests = append(c.executeRequests, &executeRequest{
		Metadata: md,
		Payload:  req,
	})
	return &fakeExecuteStream{}, nil
}

func (c *fakeExecutionClient) WaitExecution(ctx context.Context, req *repb.WaitExecutionRequest, opts ...grpc.CallOption) (repb.Execution_WaitExecutionClient, error) {
	return &fakeExecuteStream{}, nil
}

type fakeExecuteStream struct{ grpc.ClientStream }

func (*fakeExecuteStream) Recv() (*longrunningpb.Operation, error) {
	metadata, err := anypb.New(&repb.ExecuteOperationMetadata{
		Stage: repb.ExecutionStage_COMPLETED,
	})
	if err != nil {
		return nil, err
	}
	return &longrunningpb.Operation{Name: "fake-operation-name", Metadata: metadata}, nil
}

type execution struct {
	Action  *repb.Action
	Command *repb.Command
}

func getExecution(t *testing.T, ctx context.Context, te *testenv.TestEnv, executeRequest *repb.ExecuteRequest) *execution {
	instanceName := executeRequest.GetInstanceName()
	ar := digest.NewCASResourceName(executeRequest.GetActionDigest(), instanceName, repb.DigestFunction_BLAKE3)
	action := &repb.Action{}
	err := cachetools.GetBlobAsProto(ctx, te.GetByteStreamClient(), ar, action)
	require.NoError(t, err)
	cr := digest.NewCASResourceName(action.GetCommandDigest(), instanceName, repb.DigestFunction_BLAKE3)
	command := &repb.Command{}
	err = cachetools.GetBlobAsProto(ctx, te.GetByteStreamClient(), cr, command)
	require.NoError(t, err)
	return &execution{
		Action:  action,
		Command: command,
	}
}

func setGroupStatus(t *testing.T, te *testenv.TestEnv, ctx context.Context, groupStatus grpb.Group_GroupStatus) context.Context {
	tu, err := te.GetUserDB().GetUser(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, tu.Groups)
	gid := tu.Groups[0].Group.GroupID
	err = te.GetDBHandle().NewQuery(context.Background(), "update_group_status_for_test").Raw(`
		UPDATE "Groups" SET status = ? WHERE group_id = ?`,
		int32(groupStatus), gid).Exec().Error
	require.NoError(t, err)

	auth := te.GetAuthenticator().(*testauth.TestAuthenticator)
	authCtx, err := auth.WithAuthenticatedUser(context.Background(), tu.UserID)
	require.NoError(t, err)
	jwt, err := auth.TestJWTForUserID(tu.UserID)
	require.NoError(t, err)
	return metadata.AppendToOutgoingContext(authCtx, authutil.ContextTokenStringKey, jwt)
}

func TestRun_WithoutRepoURL(t *testing.T) {
	te, ctx := getEnv(t)

	r, err := New(te)
	require.NoError(t, err)

	_, err = r.Run(ctx, &rnpb.RunRequest{
		Steps: []*rnpb.Step{{Run: "echo hello"}},
	})
	require.NoError(t, err)

	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	require.Equal(t, 1, len(execClient.executeRequests))
}

func TestTimeout(t *testing.T) {
	tests := []struct {
		name                  string
		groupStatus           grpb.Group_GroupStatus
		experimentTimeout     string
		expectedTimeout       time.Duration
		expectedTimeoutReason string
	}{
		{name: "timeout configured in experiment", groupStatus: grpb.Group_FREE_TIER_GROUP_STATUS, experimentTimeout: "1h", expectedTimeout: 1 * time.Hour, expectedTimeoutReason: ci_runner_util.FreeTierTimeoutReason},
		{name: "timeout not configured in experiment", groupStatus: grpb.Group_ENTERPRISE_GROUP_STATUS, expectedTimeout: 24 * time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "remote_execution.remote_runner_default_timeout", tc.experimentTimeout)
			te, ctx := getEnv(t)

			ctx = setGroupStatus(t, te, ctx, tc.groupStatus)

			r, err := New(te)
			require.NoError(t, err)

			_, err = r.Run(ctx, &rnpb.RunRequest{
				Steps:   []*rnpb.Step{{Run: "echo hello"}},
				Timeout: "24h",
			})
			require.NoError(t, err)

			execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
			require.Equal(t, 1, len(execClient.executeRequests))
			exec := getExecution(t, ctx, te, execClient.executeRequests[0].Payload)
			expectedTimeout := tc.expectedTimeout.String()
			require.Contains(t, exec.Command.GetArguments(), "--timeout="+expectedTimeout)
			if tc.expectedTimeoutReason != "" {
				require.Contains(t, exec.Command.GetArguments(), "--timeout_reason="+tc.expectedTimeoutReason)
			} else {
				require.NotContains(t, exec.Command.GetArguments(), "--timeout_reason="+ci_runner_util.FreeTierTimeoutReason)
			}
			require.Equal(t, tc.expectedTimeout+TimeoutGracePeriod, exec.Action.GetTimeout().AsDuration())
		})
	}
}

func TestRemoteHeaders_EnvOverrides(t *testing.T) {
	encode := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	for _, tc := range []struct {
		name          string
		headers       []string
		wantOverrides []string
	}{
		{
			name:          "plain overrides",
			headers:       []string{"x-buildbuddy-platform.env-overrides=PWD=supersecret,USERNAME=bb"},
			wantOverrides: []string{"PWD=supersecret", "USERNAME=bb"},
		},
		{
			name: "repeated plain overrides",
			headers: []string{
				"x-buildbuddy-platform.env-overrides=K1=first",
				"x-buildbuddy-platform.env-overrides=K2=other,K1=last",
			},
			wantOverrides: []string{"K1=first", "K2=other", "K1=last"},
		},
		{
			name: "repeated base64 overrides",
			headers: []string{
				"x-buildbuddy-platform.env-overrides-base64=" + encode("K1=V1,V2"),
				"x-buildbuddy-platform.env-overrides-base64=" + encode("K2=V3=V4"),
				"x-buildbuddy-platform.env-overrides-base64=" + encode("K1=last"),
			},
			wantOverrides: []string{"K1=V1,V2", "K2=V3=V4", "K1=last"},
		},
		{
			name: "base64 overrides follow plain overrides",
			headers: []string{
				"x-buildbuddy-platform.env-overrides-base64=" + encode("K1=encoded-first"),
				"x-buildbuddy-platform.env-overrides=K1=plain,K2=other",
				"x-buildbuddy-platform.env-overrides-base64=" + encode("K1=encoded-last"),
			},
			wantOverrides: []string{"K1=plain", "K2=other", "K1=encoded-first", "K1=encoded-last"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te, ctx := getEnv(t)
			r, err := New(te)
			require.NoError(t, err)
			_, err = r.Run(ctx, &rnpb.RunRequest{
				GitRepo:       &gitpb.GitRepo{RepoUrl: "sample"},
				RepoState:     &gitpb.RepoState{Branch: "test"},
				RemoteHeaders: tc.headers,
				Steps:         []*rnpb.Step{{Run: "test-val"}},
			})
			require.NoError(t, err)

			execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
			require.Len(t, execClient.executeRequests, 1)
			md := execClient.executeRequests[0].Metadata
			// Platform parsing only applies the last header per property, so each must be merged.
			plainOverrides := md.Get(platform.OverrideHeaderPrefix + platform.EnvOverridesPropertyName)
			require.Len(t, plainOverrides, 1)
			require.LessOrEqual(t, len(md.Get(platform.OverrideHeaderPrefix+platform.EnvOverridesBase64PropertyName)), 1)
			props, err := platform.ParseProperties(&repb.ExecutionTask{
				PlatformOverrides: &repb.Platform{Properties: platform.RemoteHeaderOverrides(metadata.NewIncomingContext(ctx, md))},
			})
			require.NoError(t, err)
			// User overrides follow the runner's credential overrides and retain their order.
			require.Greater(t, len(props.EnvOverrides), len(tc.wantOverrides))
			require.Equal(t, tc.wantOverrides, props.EnvOverrides[len(props.EnvOverrides)-len(tc.wantOverrides):])
			for _, expectedCredential := range []string{"BUILDBUDDY_API_KEY", "REPO_TOKEN", "REPO_USER"} {
				require.Contains(t, plainOverrides[0], expectedCredential+"=")
			}
		})
	}
}

func TestRemoteHeaders_SecretEnvOverrides(t *testing.T) {
	encode := func(value string) string { return base64.StdEncoding.EncodeToString([]byte(value)) }
	for _, tc := range []struct {
		name          string
		headers       []string
		wantOverrides []string
	}{
		{
			name: "repeated secrets",
			headers: []string{
				"x-buildbuddy-platform.secret-env-overrides=K1=V1",
				"x-buildbuddy-platform.secret-env-overrides=K2=V2",
			},
			wantOverrides: []string{"K1=V1", "K2=V2"},
		},
		{
			name: "repeated base64 secrets",
			headers: []string{
				"x-buildbuddy-platform.secret-env-overrides-base64=" + encode("K1=V1,V2"),
				"x-buildbuddy-platform.secret-env-overrides-base64=" + encode("K2=V3=V4"),
			},
			wantOverrides: []string{"K1=V1,V2", "K2=V3=V4"},
		},
		{
			name: "base64 overrides follow plain overrides",
			headers: []string{
				"x-buildbuddy-platform.secret-env-overrides-base64=" + encode("K1=encoded-first"),
				"x-buildbuddy-platform.secret-env-overrides=K1=plain,K2=other",
				"x-buildbuddy-platform.secret-env-overrides-base64=" + encode("K1=encoded-last"),
			},
			wantOverrides: []string{"K1=plain", "K2=other", "K1=encoded-first", "K1=encoded-last"},
		},
		{
			name: "case insensitive header names",
			headers: []string{
				"X-BuildBuddy-Platform.Secret-Env-Overrides=K1=V1",
				"x-buildbuddy-platform.secret-env-overrides=K2=V2",
			},
			wantOverrides: []string{"K1=V1", "K2=V2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te, ctx := getEnv(t)
			r, err := New(te)
			require.NoError(t, err)
			_, err = r.Run(ctx, &rnpb.RunRequest{
				GitRepo:       &gitpb.GitRepo{RepoUrl: "sample"},
				RepoState:     &gitpb.RepoState{Branch: "test"},
				RemoteHeaders: tc.headers,
				Steps:         []*rnpb.Step{{Run: "test-val"}},
			})
			require.NoError(t, err)

			execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
			require.Len(t, execClient.executeRequests, 1)
			md := execClient.executeRequests[0].Metadata
			for _, property := range []string{platform.SecretEnvOverridesPropertyName, platform.SecretEnvOverridesBase64PropertyName} {
				require.LessOrEqual(t, len(md.Get(platform.OverrideHeaderPrefix+property)), 1)
			}
			props, err := platform.ParseProperties(&repb.ExecutionTask{
				PlatformOverrides: &repb.Platform{Properties: platform.RemoteHeaderOverrides(metadata.NewIncomingContext(ctx, md))},
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantOverrides, props.SecretEnvOverrides)
			// Env overrides set by the runner, like REPO_TOKEN and BUILDBUDDY_API_KEY, should continue to be set.
			require.NotEmpty(t, props.EnvOverrides)
		})
	}
}

func TestRun_BuildUser(t *testing.T) {
	for _, tc := range []struct {
		name     string
		subID    string
		fullName string
		groupKey bool
		env      map[string]string
		want     string
	}{
		{name: "GitHub user", subID: "https://github.com/alice", fullName: "Alice Example", want: "alice"},
		{name: "user name", subID: "sso/alice", fullName: "Alice Example", want: "Alice Example"},
		{name: "unnamed user", subID: "sso/alice", want: "US1"},
		{name: "organization key", subID: "sso/alice", groupKey: true, want: "Acme, Inc."},
		{name: "explicit user", subID: "sso/alice", env: map[string]string{"BUILD_USER": "custom-user"}, want: "custom-user"},
		{name: "explicit empty user", subID: "sso/alice", env: map[string]string{"BUILD_USER": ""}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "app.user_owned_keys_enabled", true)
			te, ctx := getEnv(t)
			u, err := te.GetUserDB().GetUser(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, u.Groups)
			groupID := u.Groups[0].Group.GroupID
			err = te.GetDBHandle().NewQuery(ctx, "set_attribution_profile").Raw(
				`UPDATE "Users" SET sub_id = ?, first_name = ? WHERE user_id = ?`,
				tc.subID, tc.fullName, u.UserID).Exec().Error
			require.NoError(t, err)
			enterprise_testauth.SetUserOwnedKeysEnabled(t, ctx, te, groupID, true)
			err = te.GetDBHandle().NewQuery(ctx, "set_attribution_organization").Raw(
				`UPDATE "Groups" SET name = ? WHERE group_id = ?`, "Acme, Inc.", groupID).Exec().Error
			require.NoError(t, err)
			var key *tables.APIKey
			caps := []cappb.Capability{cappb.Capability_CACHE_WRITE}
			if tc.groupKey {
				key, err = te.GetAuthDB().CreateAPIKey(ctx, groupID, "test", caps, 0, false)
			} else {
				key, err = te.GetAuthDB().CreateUserAPIKey(ctx, groupID, u.UserID, "test", caps, 0)
			}
			require.NoError(t, err)
			ctx = te.GetAuthenticator().AuthContextFromAPIKey(t.Context(), key.Value)
			ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, key.Value)
			r, err := New(te)
			require.NoError(t, err)
			_, err = r.Run(ctx, &rnpb.RunRequest{
				Steps: []*rnpb.Step{{Run: "echo hello"}},
				Env:   tc.env,
			})
			require.NoError(t, err)
			execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
			require.Len(t, execClient.executeRequests, 1)
			exec := getExecution(t, ctx, te, execClient.executeRequests[0].Payload)
			var users []string
			for _, env := range exec.Command.GetEnvironmentVariables() {
				if env.GetName() == "BUILD_USER" {
					users = append(users, env.GetValue())
				}
			}
			require.Equal(t, []string{tc.want}, users)
		})
	}
}

func TestNormalizeWorkingDirectory(t *testing.T) {
	for _, tc := range []struct {
		name      string
		input     string
		expected  string
		expectErr bool
	}{
		{"empty", "", "", false},
		{"simple subdir", "subdir", "subdir", false},
		{"nested", filepath.Join("subdir", "nested"), filepath.Join("subdir", "nested"), false},
		{"dot cleaned to empty", ".", "", false},
		{"absolute path rejected", "/tmp/workspace", "", true},
		{"parent traversal rejected", filepath.Join("..", "subdir"), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := normalizeWorkingDirectory(tc.input)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestActionFromRunRequest(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		req                       *rnpb.RunRequest
		expectedName              string
		expectedBazelWorkspaceDir string
		expectErr                 bool
	}{
		{
			name: "all fields set",
			req: &rnpb.RunRequest{
				Name:             "Test run",
				Steps:            []*rnpb.Step{{Run: "bazel build //:target"}},
				WorkingDirectory: filepath.Join("subdir", "nested"),
			},
			expectedName:              "Test run",
			expectedBazelWorkspaceDir: filepath.Join("subdir", "nested"),
		},
		{
			name: "default name",
			req: &rnpb.RunRequest{
				Steps: []*rnpb.Step{{Run: "bazel test //..."}},
			},
			expectedName:              "remote run",
			expectedBazelWorkspaceDir: "",
		},
		{
			name: "invalid working directory",
			req: &rnpb.RunRequest{
				Steps:            []*rnpb.Step{{Run: "bazel build //..."}},
				WorkingDirectory: "/absolute/path",
			},
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			action, err := actionFromRunRequest(tc.req)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedName, action.Name)
			require.Equal(t, tc.req.GetSteps(), action.Steps)
			require.Equal(t, tc.expectedBazelWorkspaceDir, action.BazelWorkspaceDir)
		})
	}
}

func TestCredentialEnvOverrides_PrivateRepo(t *testing.T) {
	const (
		mockAppID = int64(1234)
		fakeToken = "fake-github-token"
		// repoURL is the raw input; normalizedURL is what git.NormalizeRepoURL produces
		// (they are the same here, but keeping them distinct documents the expectation).
		repoURL        = "https://github.com/test-org/test-repo"
		normalizedURL  = "https://github.com/test-org/test-repo"
		repoOwner      = "test-org"
		installationID = int64(1)
	)

	// Enable the read-write GitHub app flag so that IsReadWriteAppEnabled() returns true.
	flags.Set(t, "github.app.enabled", true)

	te, ctx := getEnv(t)

	// Derive groupID from the authenticated user context rather than hard-coding it.
	u, err := te.GetAuthenticator().AuthenticatedUser(ctx)
	require.NoError(t, err)
	groupID := u.GetGroupID()
	require.NotEmpty(t, groupID)

	dbh := te.GetDBHandle()
	require.NotNil(t, dbh)

	// Insert a GitHubAppInstallation row (required by production GetRepositoryInstallationToken).
	err = dbh.NewQuery(ctx, "create_github_app_install_for_test").Create(&tables.GitHubAppInstallation{
		UserID:         "US1",
		GroupID:        groupID,
		AppID:          mockAppID,
		Owner:          repoOwner,
		InstallationID: installationID,
		Perms:          1,
	})
	require.NoError(t, err)

	// Insert a GitRepositories row (required by production GetRepositoryInstallationToken).
	err = dbh.NewQuery(ctx, "create_git_repo_for_test").Create(&tables.GitRepository{
		UserID:  "US1",
		GroupID: groupID,
		RepoURL: normalizedURL,
		AppID:   mockAppID,
		Perms:   1,
	})
	require.NoError(t, err)

	// Set up a fake GitHub app that asserts the correct groupID, repoURL, and
	// owner are passed through, and that both the GitRepositories and
	// GitHubAppInstallations rows required by production are present.
	vApp := &testgit.FakeGitHubApp{
		Token:           fakeToken,
		MockAppID:       mockAppID,
		DBHandle:        dbh,
		ExpectedGroupID: groupID,
		ExpectedRepoURL: normalizedURL,
		ExpectedOwner:   repoOwner,
	}
	gh, err := githubapp.NewAppService(te, vApp, nil)
	require.NoError(t, err)
	te.SetGitHubAppService(gh)

	// Build the runner service.
	r, err := New(te)
	require.NoError(t, err)

	// Call credentialEnvOverrides with a RunRequest that has a repo URL but no explicit access token.
	req := &rnpb.RunRequest{
		GitRepo: &gitpb.GitRepo{
			RepoUrl: repoURL,
		},
		Steps: []*rnpb.Step{{Run: "echo hello"}},
	}
	envOverrides, err := r.credentialEnvOverrides(ctx, req)
	require.NoError(t, err)

	// Assert that REPO_TOKEN is set to the fake token.
	require.Contains(t, envOverrides, "REPO_TOKEN="+fakeToken)
}

func TestRun_GitCredentialsFromEnv(t *testing.T) {
	te, ctx := getEnv(t)
	r, err := New(te)
	require.NoError(t, err)

	_, err = r.Run(ctx, &rnpb.RunRequest{
		GitRepo:   &gitpb.GitRepo{RepoUrl: "https://bitbucket.org/org/repo"},
		RepoState: &gitpb.RepoState{Branch: "main"},
		Env:       map[string]string{"REPO_USER": "user", "REPO_TOKEN": "token"},
		Steps:     []*rnpb.Step{{Run: "echo hello"}},
	})
	require.NoError(t, err)

	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	require.Equal(t, 1, len(execClient.executeRequests))
	execReq := execClient.executeRequests[0]
	overrides := execReq.Metadata.Get(platform.OverrideHeaderPrefix + platform.EnvOverridesPropertyName)
	require.Greater(t, len(overrides), 0)
	require.Contains(t, overrides[len(overrides)-1], "REPO_USER=user,REPO_TOKEN=token")
	for _, ev := range getExecution(t, ctx, te, execReq.Payload).Command.GetEnvironmentVariables() {
		require.NotContains(t, []string{"REPO_USER", "REPO_TOKEN"}, ev.GetName())
	}
}
