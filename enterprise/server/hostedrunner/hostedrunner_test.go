package hostedrunner

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"

	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	gitpb "github.com/buildbuddy-io/buildbuddy/proto/git"
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

func getCommand(t *testing.T, ctx context.Context, te *testenv.TestEnv, executeRequest *repb.ExecuteRequest) *repb.Command {
	instanceName := executeRequest.GetInstanceName()
	actionResourceName := digest.NewCASResourceName(executeRequest.GetActionDigest(), instanceName, executeRequest.GetDigestFunction())
	action := &repb.Action{}
	err := cachetools.GetBlobAsProto(ctx, te.GetByteStreamClient(), actionResourceName, action)
	require.NoError(t, err)

	commandResourceName := digest.NewCASResourceName(action.GetCommandDigest(), instanceName, executeRequest.GetDigestFunction())
	command := &repb.Command{}
	err = cachetools.GetBlobAsProto(ctx, te.GetByteStreamClient(), commandResourceName, command)
	require.NoError(t, err)
	return command
}

func createGitRepo(t *testing.T, te *testenv.TestEnv, ctx context.Context, repoURL string, useCLIInRemoteRunners bool) {
	u, err := te.GetAuthenticator().AuthenticatedUser(ctx)
	require.NoError(t, err)
	err = te.GetDBHandle().NewQuery(ctx, "create_git_repo_for_test").Create(&tables.GitRepository{
		RepoURL:                  repoURL,
		GroupID:                  u.GetGroupID(),
		UserID:                   u.GetUserID(),
		Perms:                    perms.GROUP_READ | perms.GROUP_WRITE,
		UseCLIInRemoteRunners:    useCLIInRemoteRunners,
		UseDefaultWorkflowConfig: false,
	})
	require.NoError(t, err)

	// In `Create`, GORM omits fields that are set to false. Because `use_cli_in_remote_runners`
	// defaults to true, that field is set to true when the value is omitted.
	// We must explicitly set it to false if requested.
	if !useCLIInRemoteRunners {
		err = te.GetDBHandle().NewQuery(ctx, "disable_cli_for_repo_for_test").Raw(`
			UPDATE "GitRepositories"
			SET use_cli_in_remote_runners = ?
			WHERE group_id = ?
			AND repo_url = ?
		`, false, u.GetGroupID(), repoURL).Exec().Error
		require.NoError(t, err)
	}
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

func TestRemoteHeaders_EnvOverrides(t *testing.T) {
	te, ctx := getEnv(t)

	r, err := New(te)
	require.NoError(t, err)
	repoURL := "https://github.com/fake/sample"
	createGitRepo(t, te, ctx, repoURL, false)

	_, err = r.Run(ctx, &rnpb.RunRequest{
		GitRepo:       &gitpb.GitRepo{RepoUrl: repoURL},
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

func TestBazelUseCLI(t *testing.T) {
	for _, useCLI := range []bool{true, false} {
		te, ctx := getEnv(t)

		repoURL := fmt.Sprintf("https://github.com/fake/%v", useCLI)
		createGitRepo(t, te, ctx, repoURL, useCLI)

		r, err := New(te)
		require.NoError(t, err)
		_, err = r.Run(ctx, &rnpb.RunRequest{
			GitRepo:   &gitpb.GitRepo{RepoUrl: repoURL},
			RepoState: &gitpb.RepoState{Branch: "main"},
			Steps:     []*rnpb.Step{{Run: "bazel version"}},
		})
		require.NoError(t, err)

		execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
		require.Equal(t, 1, len(execClient.executeRequests))
		command := getCommand(t, ctx, te, execClient.executeRequests[0].Payload)

		if useCLI {
			require.Contains(t, command.GetArguments(), "--bazel_command=bb")
		} else {
			require.NotContains(t, command.GetArguments(), "--bazel_command=bb")
		}
	}
}
