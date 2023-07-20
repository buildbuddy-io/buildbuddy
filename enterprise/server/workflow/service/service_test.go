package service_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"

	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	configWithLinuxWorkflow = `
actions:
  - name: "Test (linux_amd64)"
    triggers: { pull_request: { branches: [ "*" ] }, push: { branches: [ "*" ] } }
    bazel_commands: [ "test //..." ]
`
)

func newTestEnv(t *testing.T) *testenv.TestEnv {
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

func runBBServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(env /*sslService=*/, nil)
	require.NoError(t, err)
	bsServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)

	grpcServer, runFunc := env.LocalGRPCServer()
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)
	bspb.RegisterByteStreamServer(grpcServer, bsServer)

	go runFunc()
	t.Cleanup(func() { grpcServer.GracefulStop() })

	clientConn, err := env.LocalGRPCConn(ctx)
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

// pingWebhook makes an empty request to the given webhook URL. The fake git
// provider's WebhookData field determines the webhook data parsed from the
// request.
func pingWebhook(t *testing.T, url string) {
	res, err := http.Post(url, "", bytes.NewReader([]byte{}))
	require.NoError(t, err)
	// Log the response body for debug purposes.
	body, _ := io.ReadAll(res.Body)
	t.Log(string(body))
	require.Equal(t, 200, res.StatusCode)
}

type execution struct {
	Metadata metadata.MD
	Action   *repb.Action
	Command  *repb.Command
}

// getExecution fetches digest contents of an ExecuteRequest.
func getExecution(t *testing.T, ctx context.Context, te *testenv.TestEnv, executeRequest *repb.ExecuteRequest) *execution {
	instanceName := executeRequest.GetInstanceName()
	ar := digest.NewResourceName(executeRequest.GetActionDigest(), instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	action := &repb.Action{}
	err := cachetools.GetBlobAsProto(ctx, te.GetByteStreamClient(), ar, action)
	require.NoError(t, err)
	command := &repb.Command{}
	cr := digest.NewResourceName(action.GetCommandDigest(), instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
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

type fakeExecuteStream struct{ grpc.ClientStream }

func (*fakeExecuteStream) Recv() (*longrunning.Operation, error) {
	return &longrunning.Operation{Name: "fake-operation-name"}, nil
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

func TestCreate_SuccessfullyRegisterWebhook(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	ctx, uid, gid := authenticate(t, ctx, te)
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		Name:           "BuildBuddy OS Workflow",
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	rsp, err := bbClient.CreateWorkflow(ctx, req)

	require.NoError(t, err)
	assert.Regexp(t, "^WF.*", rsp.GetId(), "workflow ID should exist and match WF.*")
	assert.Regexp(t, "^.*/webhooks/workflow/.*", rsp.GetWebhookUrl(), "workflow webhook URL should exist and match /webhooks/workflow/.*")
	assert.True(t, rsp.GetWebhookRegistered())

	var row tables.Workflow
	err = te.GetDBHandle().DB(ctx).First(&row).Error
	require.NoError(t, err)
	assert.Equal(t, rsp.GetId(), row.WorkflowID, "inserted table workflow ID should match create response")
	assert.Equal(t, gid, row.UserID, "inserted table workflow user should match auth")
	assert.Equal(t, gid, row.GroupID, "inserted table workflow group should match auth")
	assert.Equal(
		t, testgit.FakeWebhookID, row.GitProviderWebhookID,
		"inserted table workflow git provider webhook ID should be set based on git provider response",
	)
	assert.NotEmpty(t, row.WebhookID, "webhook ID in DB should be nonempty")
	assert.Contains(t, rsp.GetWebhookUrl(), row.WebhookID, "webhook ID in DB should match the URL")
	assert.Equal(t, rsp.GetWebhookUrl(), provider.RegisteredWebhookURL, "returned webhook URL should be registered to the provider")
}

func TestCreate_NoWebhookPermissions(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	ctx, uid, gid := authenticate(t, ctx, te)
	provider := setupFakeGitProvider(t, te)
	provider.RegisterWebhookError = fmt.Errorf("(fake error) You do not have permissions to register webhooks!")
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		Name:           "BuildBuddy OS Workflow",
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	rsp, err := bbClient.CreateWorkflow(ctx, req)

	require.NoError(t, err)
	assert.Regexp(t, "^WF.*", rsp.GetId(), "workflow ID should exist and match WF.*")
	assert.Regexp(t, "^.*/webhooks/workflow/.*", rsp.GetWebhookUrl(), "workflow webhook URL should exist and match /webhooks/workflow/.*")
	assert.False(t, rsp.GetWebhookRegistered(), "webhook should have failed to register")

	var row tables.Workflow
	err = te.GetDBHandle().DB(ctx).First(&row).Error
	require.NoError(t, err)
	assert.Equal(t, rsp.GetId(), row.WorkflowID, "inserted table workflow ID should match create response")
	assert.Equal(t, repoURL, row.RepoURL)
	assert.Equal(t, gid, row.UserID, "inserted table workflow user should match auth")
	assert.Equal(t, gid, row.GroupID, "inserted table workflow group should match auth")
	assert.NotEmpty(t, row.WebhookID, "webhook ID in DB should be nonempty")
	assert.Contains(t, rsp.GetWebhookUrl(), row.WebhookID, "webhook ID in DB should match the URL")
	assert.Equal(t, "", row.GitProviderWebhookID, "git provider should not have returned a webhook ID")
}

func TestCreate_NonNormalizedRepoURL(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	ctx, uid, gid := authenticate(t, ctx, te)
	// We are using a github.com URL to test normalization, so disable the repo
	// downloader just for this test so that it doesn't depend on GitHub.
	te.SetRepoDownloader(nil)
	setupFakeGitProvider(t, te)
	repoURL := "git@github.com:/foo/bar"
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		GitRepo: &wfpb.CreateWorkflowRequest_GitRepo{
			// Access token is required for GitHub URLs
			AccessToken: "test-access-token",
			RepoUrl:     repoURL,
		},
	}
	_, err := bbClient.CreateWorkflow(ctx, req)

	require.NoError(t, err)

	var row tables.Workflow
	err = te.GetDBHandle().DB(ctx).First(&row).Error
	assert.NoError(t, err)
	assert.Equal(t, "https://github.com/foo/bar", row.RepoURL, "repo URL stored in DB should be normalized")
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	ctx, _, gid := authenticate(t, ctx, te)
	provider := setupFakeGitProvider(t, te)

	clientConn := runBBServer(ctx, te, t)
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
	err := te.GetDBHandle().DB(ctx).Create(&row).Error
	assert.NoError(t, err)

	req := &wfpb.DeleteWorkflowRequest{Id: "WF1"}
	_, err = bbClient.DeleteWorkflow(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, testgit.FakeWebhookID, provider.UnregisteredWebhookID, "should unregister webhook upon deletion")

	err = te.GetDBHandle().DB(ctx).First(&row).Error
	assert.True(t, db.IsRecordNotFound(err))
}

func TestList(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)

	ctx1, uid1, gid1 := authenticate(t, ctx, te)

	clientConn := runBBServer(ctx, te, t)
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
	err := te.GetDBHandle().DB(ctx).Create(&row).Error
	require.NoError(t, err)

	row2 := &tables.Workflow{
		WorkflowID: "WF2",
		UserID:     gid1,
		GroupID:    gid1,
		Perms:      48,
		RepoURL:    "file:///ANY",
		WebhookID:  "WHID2",
	}
	err = te.GetDBHandle().DB(ctx).Create(&row2).Error
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
	err = te.GetDBHandle().DB(ctx).Create(&row3).Error
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
	ctx, uid, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	wfRes, err := bbClient.CreateWorkflow(ctx, req)
	require.NoError(t, err)
	webhookURL := wfRes.GetWebhookUrl()
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

	pingWebhook(t, webhookURL)

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
	ctx, uid, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	wfRes, err := bbClient.CreateWorkflow(ctx, req)
	require.NoError(t, err)
	webhookURL := wfRes.GetWebhookUrl()
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

	pingWebhook(t, webhookURL)

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
	ctx, uid, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	wfRes, err := bbClient.CreateWorkflow(ctx, req)
	require.NoError(t, err)
	webhookURL := wfRes.GetWebhookUrl()
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

	pingWebhook(t, webhookURL)

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
	ctx, uid, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	wfRes, err := bbClient.CreateWorkflow(ctx, req)
	require.NoError(t, err)
	webhookURL := wfRes.GetWebhookUrl()
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

	pingWebhook(t, webhookURL)
}

func TestWebhook_UntrustedApprovalOnUntrustedPullRequest_NOP(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, uid, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	wfRes, err := bbClient.CreateWorkflow(ctx, req)
	require.NoError(t, err)
	webhookURL := wfRes.GetWebhookUrl()
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

	pingWebhook(t, webhookURL)
}

func TestWebhook_TrustedPush_StartsTrustedWorkflow(t *testing.T) {
	ctx := context.Background()
	u, lis := testhttp.NewServer(t)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	te := newTestEnv(t)
	ctx, uid, gid := authenticate(t, ctx, te)
	execClient := te.GetRemoteExecutionClient().(*fakeExecutionClient)
	te.SetRemoteExecutionClient(execClient)
	go http.Serve(lis, te.GetWorkflowService())
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext(uid, gid),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	wfRes, err := bbClient.CreateWorkflow(ctx, req)
	require.NoError(t, err)
	webhookURL := wfRes.GetWebhookUrl()
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

	pingWebhook(t, webhookURL)

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
