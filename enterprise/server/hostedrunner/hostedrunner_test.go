package hostedrunner

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
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
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"

	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	bspb "google.golang.org/genproto/googleapis/bytestream"
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

	clientConn, err := testenv.LocalGRPCConn(ctx, lis)
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

func configureTimeout(t *testing.T, env *testenv.TestEnv, groupStatus grpb.Group_GroupStatus, timeout string) {
	evaluator := func(_ memprovider.InMemoryFlag, flatCtx openfeature.FlattenedContext) (any, openfeature.ProviderResolutionDetail) {
		if flatCtx["group_status"] == grpb.Group_GroupStatus_name[int32(groupStatus)] {
			return timeout, openfeature.ProviderResolutionDetail{Reason: openfeature.TargetingMatchReason}
		}
		return "", openfeature.ProviderResolutionDetail{Reason: openfeature.DefaultReason}
	}
	testProvider := memprovider.NewInMemoryProvider(map[string]memprovider.InMemoryFlag{
		ci_runner_util.DefaultTimeoutExperimentName: {
			State:            memprovider.Enabled,
			ContextEvaluator: &evaluator,
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))

	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
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
		name            string
		groupStatus     grpb.Group_GroupStatus
		expectedTimeout time.Duration
	}{
		{name: "timeout configured in experiment", groupStatus: grpb.Group_FREE_TIER_GROUP_STATUS, expectedTimeout: 1 * time.Hour},
		{name: "timeout not configured in experiment", groupStatus: grpb.Group_ENTERPRISE_GROUP_STATUS, expectedTimeout: 24 * time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			te, ctx := getEnv(t)
			configureTimeout(t, te, grpb.Group_FREE_TIER_GROUP_STATUS, "1h")

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
			require.Equal(t, tc.expectedTimeout+TimeoutGracePeriod, exec.Action.GetTimeout().AsDuration())
		})
	}
}

func TestRemoteHeaders_EnvOverrides(t *testing.T) {
	te, ctx := getEnv(t)

	r, err := New(te)
	require.NoError(t, err)

	_, err = r.Run(ctx, &rnpb.RunRequest{
		GitRepo:       &gitpb.GitRepo{RepoUrl: "sample"},
		RepoState:     &gitpb.RepoState{Branch: "test"},
		RemoteHeaders: []string{"x-buildbuddy-platform.env-overrides=PWD=supersecret,USERNAME=bb"},
		Steps:         []*rnpb.Step{{Run: "test-val"}},
	})
	require.NoError(t, err)

	// Check that the specified env overrides are set on the execution context
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	require.Equal(t, 1, len(execClient.executeRequests))
	execReq := execClient.executeRequests[0]
	envOverridesMetadata := execReq.Metadata.Get(platform.OverrideHeaderPrefix + platform.EnvOverridesPropertyName)
	require.Greater(t, len(envOverridesMetadata), 0)
	// If a platform property is specified multiple times, the latest value is applied.
	// Check that value to simulate what would actually be applied.
	appliedEnvOverrides := envOverridesMetadata[len(envOverridesMetadata)-1]
	require.Contains(t, appliedEnvOverrides, "PWD=supersecret")
	require.Contains(t, appliedEnvOverrides, "USERNAME=bb")

	// Check that credential-related overrides were not overwritten
	for _, expectedCredential := range []string{"BUILDBUDDY_API_KEY", "REPO_TOKEN", "REPO_USER"} {
		require.Contains(t, appliedEnvOverrides, expectedCredential)
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
