package service_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"

	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	mockGithubAppID = int64(98765)

	configWithLinuxWorkflow = `
actions:
  - name: "Test (linux_amd64)"
    triggers: { pull_request: { branches: [ "*" ] }, push: { branches: [ "*" ] } }
    bazel_commands: [ "test //..." ]
`
)

func newTestEnv(t *testing.T) *testenv.TestEnv {
	flags.Set(t, "github.app.enabled", true)
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	tu := &tables.User{
		UserID: "US1",
		Email:  "US1@org1.io",
		SubID:  "US1-SubID",
	}
	err := te.GetUserDB().InsertUser(context.Background(), tu)
	require.NoError(t, err)
	tu = &tables.User{
		UserID: "US2",
		Email:  "US2@org2.io",
		SubID:  "US2-SubID",
	}
	err = te.GetUserDB().InsertUser(context.Background(), tu)
	require.NoError(t, err)
	te.SetRepoDownloader(repo_downloader.NewRepoDownloader())
	te.SetWorkflowService(workflow.NewWorkflowService(te))
	gh, err := githubapp.NewAppService(te, &testgit.FakeGitHubApp{MockAppID: mockGithubAppID}, nil)
	require.NoError(t, err)
	te.SetGitHubAppService(gh)

	execClient := NewFakeExecutionClient()
	te.SetRemoteExecutionClient(execClient)
	t.Cleanup(func() {
		// Shut down to make sure the workflow task queue is drained.
		te.GetHealthChecker().Shutdown()
		te.GetHealthChecker().WaitForGracefulShutdown()

		// Once the shut down completes, the workflow background workers should
		// have all exited and no more workflows can be started. At this point,
		// assert that we have called NextExecuteRequest() exactly once for each
		// started execution.
		close(execClient.executeRequests)
		for r := range execClient.executeRequests {
			require.FailNowf(t,
				"got unexpected ExecuteRequest. If this is actually expected, make sure to call NextExecuteRequest() in the test to inspect all started executions.",
				"request: %+v", r)
		}
	})

	return te
}

func setupFakeGitProvider(t *testing.T, te *testenv.TestEnv) *testgit.FakeProvider {
	provider := testgit.NewFakeProvider()
	te.SetGitProviders([]interfaces.GitProvider{provider})
	return provider
}

func runBBServer(ctx context.Context, t *testing.T, env *testenv.TestEnv) *grpc.ClientConn {
	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(env /*sslService=*/, nil)
	require.NoError(t, err)
	bsServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)
	bspb.RegisterByteStreamServer(grpcServer, bsServer)

	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(ctx, lis)
	if err != nil {
		t.Error(err)
	}

	env.SetByteStreamClient(bspb.NewByteStreamClient(clientConn))

	return clientConn
}

// Makes a local temp repo and returns the repo URL.
func makeTempRepo(t *testing.T) string {
	path, _ := testgit.MakeTempRepo(t, map[string]string{"README": "# Test repo"})
	return fmt.Sprintf("file://%s", path)
}

func createWorkflow(t *testing.T, env *testenv.TestEnv, repoURL, groupID string, useDefaultWorkflowConfig bool) *tables.GitRepository {
	dbh := env.GetDBHandle()
	require.NotNil(t, dbh)
	repo := &tables.GitRepository{
		RepoURL:                  repoURL,
		GroupID:                  groupID,
		AppID:                    mockGithubAppID,
		Perms:                    perms.GROUP_READ | perms.GROUP_WRITE,
		UseDefaultWorkflowConfig: useDefaultWorkflowConfig,
	}
	err := dbh.NewQuery(context.Background(), "create_git_repo_for_test").Create(repo)
	require.NoError(t, err)
	appInstallation := &tables.GitHubAppInstallation{
		GroupID: groupID,
		AppID:   mockGithubAppID,
	}
	err = dbh.NewQuery(context.Background(), "create_app_installation_for_test").Create(appInstallation)
	require.NoError(t, err)
	return repo
}

// pingLegacyWorkflowWebhook makes an empty request to the given webhook URL. The fake git
// provider's WebhookData field determines the webhook data parsed from the
// request.
func pingLegacyWorkflowWebhook(t *testing.T, env *testenv.TestEnv, url string) {
	req, err := http.NewRequest("POST", url, nil /*=body*/)
	require.NoError(t, err)
	env.GetWorkflowService().ServeHTTP(testhttp.NewResponseWriter(t), req)
}

type execution struct {
	Metadata metadata.MD
	Action   *repb.Action
	Command  *repb.Command
}

// getExecution fetches digest contents of an ExecuteRequest.
func getExecution(t *testing.T, ctx context.Context, te *testenv.TestEnv, executeRequest *repb.ExecuteRequest) *execution {
	instanceName := executeRequest.GetInstanceName()
	ar := digest.NewCASResourceName(executeRequest.GetActionDigest(), instanceName, repb.DigestFunction_BLAKE3)
	action := &repb.Action{}
	err := cachetools.GetBlobAsProto(ctx, te.GetByteStreamClient(), ar, action)
	require.NoError(t, err)
	command := &repb.Command{}
	cr := digest.NewCASResourceName(action.GetCommandDigest(), instanceName, repb.DigestFunction_BLAKE3)
	err = cachetools.GetBlobAsProto(ctx, te.GetByteStreamClient(), cr, command)
	require.NoError(t, err)
	return &execution{
		Action:  action,
		Command: command,
	}
}

func envVars(cmd *repb.Command) map[string]string {
	vars := make(map[string]string, len(cmd.EnvironmentVariables))
	for _, ev := range cmd.EnvironmentVariables {
		vars[ev.Name] = ev.Value
	}
	return vars
}

type fakeExecutionClient struct {
	repb.ExecutionClient
	executeRequests chan *executeRequest
}

type executeRequest struct {
	Metadata metadata.MD
	Payload  *repb.ExecuteRequest
}

func NewFakeExecutionClient() *fakeExecutionClient {
	return &fakeExecutionClient{
		executeRequests: make(chan *executeRequest, 100),
	}
}

func (c *fakeExecutionClient) Execute(ctx context.Context, req *repb.ExecuteRequest, opts ...grpc.CallOption) (repb.Execution_ExecuteClient, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	c.executeRequests <- &executeRequest{
		Metadata: md,
		Payload:  req,
	}
	return &fakeExecuteStream{}, nil
}

func (c *fakeExecutionClient) NextExecuteRequest() *executeRequest {
	return <-c.executeRequests
}

func (c *fakeExecutionClient) WaitExecution(ctx context.Context, req *repb.WaitExecutionRequest, opts ...grpc.CallOption) (repb.Execution_WaitExecutionClient, error) {
	return &fakeExecuteStream{}, nil
}

type fakeExecuteStream struct{ grpc.ClientStream }

func (*fakeExecuteStream) Recv() (*longrunning.Operation, error) {
	metadata, err := anypb.New(&repb.ExecuteOperationMetadata{
		Stage: repb.ExecutionStage_COMPLETED,
	})
	if err != nil {
		return nil, err
	}
	return &longrunning.Operation{Name: "fake-operation-name", Metadata: metadata}, nil
}

func authenticate(t *testing.T, ctx context.Context, env environment.Env) (authCtx context.Context, uid, gid string) {
	return authenticateAsUser(t, ctx, env, "US1")
}

func authenticateAsUser(t *testing.T, ctx context.Context, env environment.Env, userID string) (authCtx context.Context, uid, gid string) {
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	authCtx, err := auth.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	tu, err := env.GetUserDB().GetUser(authCtx)
	require.NoError(t, err)
	gid = tu.Groups[0].Group.GroupID
	jwt, err := auth.TestJWTForUserID(userID)
	require.NoError(t, err)
	authCtx = metadata.AppendToOutgoingContext(authCtx, "x-buildbuddy-jwt", jwt)
	return authCtx, tu.UserID, gid
}

func TestDeleteLegacyWorkflow(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	provider := setupFakeGitProvider(t, te)

	clientConn := runBBServer(ctx, t, te)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	// Create a single workflow row.
	row := &tables.Workflow{
		WorkflowID:           "WF1",
		UserID:               gid,
		GroupID:              gid,
		Perms:                48,
		RepoURL:              "file:///ANY",
		GitProviderWebhookID: testgit.FakeWebhookID,
	}
	err := te.GetDBHandle().NewQuery(ctx, "create_workflow").Create(&row)
	assert.NoError(t, err)

	req := &wfpb.DeleteWorkflowRequest{Id: "WF1"}
	_, err = bbClient.DeleteWorkflow(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, testgit.FakeWebhookID, provider.UnregisteredWebhookID, "should unregister webhook upon deletion")

	err = te.GetDBHandle().NewQuery(ctx, "get_worflow").Raw(`SELECT * FROM "Workflows"`).Take(&row)
	assert.True(t, db.IsRecordNotFound(err))
}

func TestListLegacyWorkflows(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)

	ctx1, uid1, gid1 := authenticate(t, ctx, te)

	clientConn := runBBServer(ctx, t, te)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	// Write 2 workflows for US1
	row := &tables.Workflow{
		WorkflowID: "WF1",
		UserID:     gid1,
		GroupID:    gid1,
		Perms:      48,
		RepoURL:    "file:///ANY",
		WebhookID:  "WHID1",
	}
	err := te.GetDBHandle().NewQuery(ctx, "create_workflow").Create(&row)
	require.NoError(t, err)

	row2 := &tables.Workflow{
		WorkflowID: "WF2",
		UserID:     gid1,
		GroupID:    gid1,
		Perms:      48,
		RepoURL:    "file:///ANY",
		WebhookID:  "WHID2",
	}
	err = te.GetDBHandle().NewQuery(ctx, "create_workflow").Create(&row2)
	require.NoError(t, err)

	ctx2, _, gid2 := authenticateAsUser(t, ctx, te, "US2")

	// Write 1 workflow for US2
	row3 := &tables.Workflow{
		WorkflowID: "WF3",
		UserID:     gid2,
		GroupID:    gid2,
		Perms:      48,
		RepoURL:    "file:///ANY",
		WebhookID:  "WHID3",
	}
	err = te.GetDBHandle().NewQuery(ctx, "create_workflow").Create(&row3)
	require.NoError(t, err)

	user1Req := &wfpb.GetWorkflowsRequest{
		RequestContext: testauth.RequestContext(uid1, gid1),
	}
	rsp, err := bbClient.GetWorkflows(ctx1, user1Req)
	require.NoError(t, err)
	assert.Equal(t, 2, len(rsp.GetWorkflow()), "Two workflows owned by US1 should be returned")

	user2Req := &wfpb.GetWorkflowsRequest{
		RequestContext: testauth.RequestContext("US2", gid2),
	}
	rsp, err = bbClient.GetWorkflows(ctx2, user2Req)
	assert.NoError(t, err)
	assert.Empty(t, cmp.Diff([]*wfpb.GetWorkflowsResponse_Workflow{{
		Id:         "WF3",
		RepoUrl:    "file:///ANY",
		WebhookUrl: "http://localhost:8080/webhooks/workflow/WHID3",
	}}, rsp.GetWorkflow(), protocmp.Transform()))
}

func TestWebhook_UntrustedPullRequest_StartsUntrustedWorkflow(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	runBBServer(ctx, t, te)

	repo := createWorkflow(t, te, repoURL, gid, false)

	provider.TrustedUsers = []string{"acme-inc-user-1"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:          "pull_request",
		TargetRepoURL:      "https://github.com/acme-inc/acme",
		TargetBranch:       "main",
		PushedRepoURL:      "https://github.com/untrusteduser/acme",
		PushedBranch:       "feature",
		SHA:                "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic: true,
		PullRequestAuthor:  "external-user-1",
	}
	provider.FileContents = map[string]string{"buildbuddy.yaml": configWithLinuxWorkflow}

	err := te.GetWorkflowService().HandleRepositoryEvent(ctx, repo, provider.WebhookData, "faketoken")
	require.NoError(t, err)

	execReq := execClient.NextExecuteRequest()
	exec := getExecution(t, ctx, te, execReq.Payload)
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
	env := envVars(exec.Command)
	assert.NotContains(t,
		env, "BUILDBUDDY_API_KEY",
		"action env should not contain BUILDBUDDY_API_KEY env var")
	assert.NotContains(t,
		execReq.Metadata,
		"x-buildbuddy-platform.env-overrides",
		"untrusted workflow should not have remote_header env vars")
}

func TestWebhook_TrustedPullRequest_StartsTrustedWorkflow(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	runBBServer(ctx, t, te)
	repo := createWorkflow(t, te, repoURL, gid, false)
	provider.TrustedUsers = []string{"acme-inc-user-1"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:          "pull_request",
		TargetRepoURL:      "https://github.com/acme-inc/acme",
		TargetBranch:       "main",
		PushedRepoURL:      "https://github.com/untrusteduser/acme",
		PushedBranch:       "feature",
		SHA:                "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic: true,
		PullRequestAuthor:  "acme-inc-user-1",
	}
	provider.FileContents = map[string]string{"buildbuddy.yaml": configWithLinuxWorkflow}

	err := te.GetWorkflowService().HandleRepositoryEvent(ctx, repo, provider.WebhookData, "faketoken")
	require.NoError(t, err)

	execReq := execClient.NextExecuteRequest()
	exec := getExecution(t, ctx, te, execReq.Payload)
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
	env := envVars(exec.Command)
	assert.NotContains(t,
		env, "BUILDBUDDY_API_KEY",
		"action env should not contain BUILDBUDDY_API_KEY env var")
	assert.Regexp(t,
		`BUILDBUDDY_API_KEY=[\w]+,REPO_USER=,REPO_TOKEN=`,
		execReq.Metadata["x-buildbuddy-platform.env-overrides"],
		"API key should be set via env-overrides")
}

func TestWebhook_TrustedApprovalOnUntrustedPullRequest_StartsTrustedWorkflow(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, t, te)
	bbspb.NewBuildBuddyServiceClient(clientConn)
	repo := createWorkflow(t, te, repoURL, gid, false)
	provider.TrustedUsers = []string{"acme-inc-user-1"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:           "pull_request",
		TargetRepoURL:       "https://github.com/acme-inc/acme",
		TargetBranch:        "main",
		PushedRepoURL:       "https://github.com/untrusteduser/acme",
		PushedBranch:        "feature",
		SHA:                 "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic:  true,
		PullRequestAuthor:   "external-user-1",
		PullRequestApprover: "acme-inc-user-1",
	}
	provider.FileContents = map[string]string{"buildbuddy.yaml": configWithLinuxWorkflow}

	err := te.GetWorkflowService().HandleRepositoryEvent(ctx, repo, provider.WebhookData, "faketoken")
	require.NoError(t, err)

	execReq := execClient.NextExecuteRequest()
	exec := getExecution(t, ctx, te, execReq.Payload)
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
	env := envVars(exec.Command)
	assert.NotContains(t,
		env, "BUILDBUDDY_API_KEY",
		"action env should not contain BUILDBUDDY_API_KEY env var")
	assert.Regexp(t,
		`BUILDBUDDY_API_KEY=[\w]+,REPO_USER=,REPO_TOKEN=`,
		execReq.Metadata["x-buildbuddy-platform.env-overrides"],
		"API key should be set via env-overrides")
}

func TestWebhook_TrustedApprovalOnAlreadyTrustedPullRequest_NOP(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, t, te)
	bbspb.NewBuildBuddyServiceClient(clientConn)
	repo := createWorkflow(t, te, repoURL, gid, false)
	provider.TrustedUsers = []string{"acme-inc-user-1", "acme-inc-user-2"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:           "pull_request",
		TargetRepoURL:       "https://github.com/acme-inc/acme",
		TargetBranch:        "main",
		PushedRepoURL:       "https://github.com/untrusteduser/acme",
		PushedBranch:        "feature",
		SHA:                 "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic:  true,
		PullRequestAuthor:   "acme-inc-user-1",
		PullRequestApprover: "acme-inc-user-2",
	}
	provider.FileContents = map[string]string{"buildbuddy.yaml": configWithLinuxWorkflow}

	err := te.GetWorkflowService().HandleRepositoryEvent(ctx, repo, provider.WebhookData, "faketoken")
	require.NoError(t, err)
}

func TestWebhook_UntrustedApprovalOnUntrustedPullRequest_NOP(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, t, te)
	bbspb.NewBuildBuddyServiceClient(clientConn)
	repo := createWorkflow(t, te, repoURL, gid, false)
	provider.TrustedUsers = []string{"acme-inc-user-1"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:           "pull_request",
		TargetRepoURL:       "https://github.com/acme-inc/acme",
		TargetBranch:        "main",
		PushedRepoURL:       "https://github.com/untrusteduser/acme",
		PushedBranch:        "feature",
		SHA:                 "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic:  true,
		PullRequestAuthor:   "external-user-1",
		PullRequestApprover: "external-user-2",
	}
	provider.FileContents = map[string]string{"buildbuddy.yaml": configWithLinuxWorkflow}

	err := te.GetWorkflowService().HandleRepositoryEvent(ctx, repo, provider.WebhookData, "faketoken")
	require.NoError(t, err)
}

func TestWebhook_TrustedPush_StartsTrustedWorkflow(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, t, te)
	bbspb.NewBuildBuddyServiceClient(clientConn)
	repo := createWorkflow(t, te, repoURL, gid, false)
	provider.TrustedUsers = []string{"acme-inc-user-1"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:          "push",
		TargetRepoURL:      "https://github.com/acme-inc/acme",
		TargetBranch:       "main",
		PushedRepoURL:      "https://github.com/acme-inc/acme",
		PushedBranch:       "main",
		SHA:                "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic: true,
	}
	provider.FileContents = map[string]string{"buildbuddy.yaml": configWithLinuxWorkflow}

	err := te.GetWorkflowService().HandleRepositoryEvent(ctx, repo, provider.WebhookData, "faketoken")
	require.NoError(t, err)

	execReq := execClient.NextExecuteRequest()
	exec := getExecution(t, ctx, te, execReq.Payload)
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
	env := envVars(exec.Command)
	assert.NotContains(t,
		env, "BUILDBUDDY_API_KEY",
		"action env should not contain BUILDBUDDY_API_KEY env var")
	assert.Regexp(t,
		`BUILDBUDDY_API_KEY=[\w]+,REPO_USER=,REPO_TOKEN=`,
		execReq.Metadata["x-buildbuddy-platform.env-overrides"],
		"API key should be set via env-overrides")
}

func TestWebhook_NoWorkflowConfig_NOP(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, t, te)
	bbspb.NewBuildBuddyServiceClient(clientConn)

	// When `useDefaultWorkflowConfig` is false, if there is not a config file
	// in the repo, we expect webhooks to trigger a no-op.
	repo := createWorkflow(t, te, repoURL, gid, false /*useDefaultWorkflowConfig*/)

	provider.TrustedUsers = []string{"acme-inc-user-1"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:               "push",
		TargetRepoURL:           "https://github.com/acme-inc/acme",
		TargetBranch:            "main",
		TargetRepoDefaultBranch: "main",
		PushedRepoURL:           "https://github.com/acme-inc/acme",
		PushedBranch:            "main",
		SHA:                     "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic:      true,
	}
	// Do not return a buildbuddy.yaml config file in the repo contents.
	provider.FileContents = map[string]string{}

	err := te.GetWorkflowService().HandleRepositoryEvent(ctx, repo, provider.WebhookData, "faketoken")
	require.NoError(t, err)

	// Give the workflow service a moment to asynchronously handle the request.
	time.Sleep(100 * time.Millisecond)

	// Verify that no execution requests were triggered.
	require.Zero(t, len(execClient.executeRequests))
}

func TestWebhook_UseDefaultWorkflowConfig(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, t, te)
	bbspb.NewBuildBuddyServiceClient(clientConn)

	// When `useDefaultWorkflowConfig` is true, even though the repo doesn't have
	// a buildbuddy.yaml, we should use the default config and still trigger an execution.
	repo := createWorkflow(t, te, repoURL, gid, true /*useDefaultWorkflowConfig*/)

	provider.TrustedUsers = []string{"acme-inc-user-1"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:               "push",
		TargetRepoURL:           "https://github.com/acme-inc/acme",
		TargetBranch:            "main",
		TargetRepoDefaultBranch: "main",
		PushedRepoURL:           "https://github.com/acme-inc/acme",
		PushedBranch:            "main",
		SHA:                     "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic:      true,
	}
	// Do not return a buildbuddy.yaml config file in the repo contents.
	provider.FileContents = map[string]string{}

	err := te.GetWorkflowService().HandleRepositoryEvent(ctx, repo, provider.WebhookData, "faketoken")
	require.NoError(t, err)

	execReq := execClient.NextExecuteRequest()
	exec := getExecution(t, ctx, te, execReq.Payload)
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
}

func TestWebhook_LegacyWorkflow_UseDefaultWorkflowConfig(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, t, te)
	bbspb.NewBuildBuddyServiceClient(clientConn)

	// Create a legacy workflow and register it for webhooks
	webhookID := "webhook1"
	legacyWF := &tables.Workflow{
		WorkflowID:           "WF1",
		WebhookID:            webhookID,
		UserID:               gid,
		GroupID:              gid,
		Perms:                48,
		RepoURL:              repoURL,
		GitProviderWebhookID: testgit.FakeWebhookID,
	}
	err := te.GetDBHandle().NewQuery(context.Background(), "create_legacy_wf_for_test").Create(legacyWF)
	require.NoError(t, err)
	webhookURL := fmt.Sprintf("%s/webhooks/workflow/%s", build_buddy_url.WithPath("").String(), webhookID)

	provider.TrustedUsers = []string{"acme-inc-user-1"}
	provider.WebhookData = &interfaces.WebhookData{
		EventName:               "push",
		TargetRepoURL:           "https://github.com/acme-inc/acme",
		TargetBranch:            "main",
		TargetRepoDefaultBranch: "main",
		PushedRepoURL:           "https://github.com/acme-inc/acme",
		PushedBranch:            "main",
		SHA:                     "c04d68571cb519e095772c865847007ed3e7fea9",
		IsTargetRepoPublic:      true,
	}
	// Do not return a buildbuddy.yaml config file in the repo contents.
	provider.FileContents = map[string]string{}
	provider.RegisteredWebhookURL = webhookURL

	pingLegacyWorkflowWebhook(t, te, webhookURL)

	execReq := execClient.NextExecuteRequest()
	exec := getExecution(t, ctx, te, execReq.Payload)
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
}

func TestAPIDispatch_ActionFiltering(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	flags.Set(t, "remote_execution.enable_kythe_indexing", true)

	te := newTestEnv(t)
	ctx, uid, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	_ = setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, t, te)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	reqCtx := testauth.RequestContext(uid, gid)
	createWorkflow(t, te, repoURL, gid, true /*useDefaultWorkflowConfig*/)

	testCases := []struct {
		name            string
		actionFilter    []string
		kytheEnabled    bool
		expectedActions []string
	}{
		{
			name:            "no action filter, kythe disabled",
			actionFilter:    nil,
			kytheEnabled:    false,
			expectedActions: []string{"Test all targets"},
		},
		{
			name:            "no action filter, kythe enabled",
			actionFilter:    nil,
			kytheEnabled:    true,
			expectedActions: []string{"Test all targets", config.KytheActionName},
		},
		{
			name:            "action filter, kythe disabled",
			actionFilter:    []string{"Test all targets"},
			kytheEnabled:    false,
			expectedActions: []string{"Test all targets"},
		},
		{
			name:            "action filter, kythe enabled",
			actionFilter:    []string{"Test all targets"},
			kytheEnabled:    true,
			expectedActions: []string{"Test all targets"},
		},
		{
			name:            "kythe action filter",
			actionFilter:    []string{config.KytheActionName},
			kytheEnabled:    true,
			expectedActions: []string{config.KytheActionName},
		},
	}

	for _, tc := range testCases {
		g, err := te.GetUserDB().GetGroupByID(ctx, gid)
		require.NoError(t, err)
		g.URLIdentifier = "mustbeset"
		g.CodeSearchEnabled = tc.kytheEnabled
		_, err = te.GetUserDB().UpdateGroup(ctx, g)
		require.NoError(t, err)

		workflowID := te.GetWorkflowService().GetLegacyWorkflowIDForGitRepository(gid, repoURL)
		rsp, err := bbClient.ExecuteWorkflow(ctx, &wfpb.ExecuteWorkflowRequest{
			RequestContext: reqCtx,
			WorkflowId:     workflowID,
			CommitSha:      "c04d68571cb519e095772c865847007ed3e7fea9",
			PushedRepoUrl:  "https://github.com/acme-inc/acme",
			PushedBranch:   "main",
			ActionNames:    tc.actionFilter,
		})
		require.NoError(t, err, tc.name)
		executedActionNames := make([]string, 0, len(rsp.ActionStatuses))
		for _, a := range rsp.ActionStatuses {
			executedActionNames = append(executedActionNames, a.ActionName)
		}
		require.ElementsMatch(t, tc.expectedActions, executedActionNames, tc.name)

		for range tc.expectedActions {
			_ = execClient.NextExecuteRequest()
		}
	}
}
