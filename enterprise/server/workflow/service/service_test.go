package service_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
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
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	te := testenv.GetTestEnv(t)
	te.SetRepoDownloader(repo_downloader.NewRepoDownloader())
	te.SetWorkflowService(workflow.NewWorkflowService(te))
	udb, err := userdb.NewUserDB(te, te.GetDBHandle())
	require.NoError(t, err)
	te.SetUserDB(udb)
	// Create an API key since we need one for executing workflows.
	ak, err := udb.CreateAPIKey(
		context.Background(),
		"GROUP1",
		"Default",
		[]akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY},
		true /*=visibleToDevelopers*/)
	require.NoError(t, err)
	tu := testauth.TestUsers(
		"USER1", "GROUP1",
		"USER2", "GROUP2",
	)
	// Test authenticator treats user IDs and API keys the same, so add a user
	// mapping for the API key
	tu[ak.Value] = tu["USER1"]
	te.SetAuthenticator(testauth.NewTestAuthenticator(tu))
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
	// Log the response body for debug purposes.
	body, _ := io.ReadAll(res.Body)
	t.Log(string(body))
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}

type execution struct {
	Action  *repb.Action
	Command *repb.Command
}

// getExecution fetches digest contents of an ExecuteRequest.
func getExecution(t *testing.T, ctx context.Context, te *testenv.TestEnv, executeRequest *repb.ExecuteRequest) *execution {
	instanceName := executeRequest.GetInstanceName()
	ar := digest.NewResourceName(executeRequest.GetActionDigest(), instanceName)
	action := &repb.Action{}
	err := cachetools.GetBlobAsProto(ctx, te.GetByteStreamClient(), ar, action)
	require.NoError(t, err)
	command := &repb.Command{}
	cr := digest.NewResourceName(action.GetCommandDigest(), instanceName)
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
	ExecuteRequests []*repb.ExecuteRequest
}

func (c *fakeExecutionClient) Execute(ctx context.Context, req *repb.ExecuteRequest, opts ...grpc.CallOption) (repb.Execution_ExecuteClient, error) {
	c.ExecuteRequests = append(c.ExecuteRequests, req)
	return &fakeExecuteStream{}, nil
}

type fakeExecuteStream struct{ grpc.ClientStream }

func (*fakeExecuteStream) Recv() (*longrunning.Operation, error) {
	return &longrunning.Operation{Name: "fake-operation-name"}, nil
}

func TestCreate_SuccessfullyRegisterWebhook(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		Name:           "BuildBuddy OS Workflow",
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
	rsp, err := bbClient.CreateWorkflow(ctx, req)

	require.NoError(t, err)
	assert.Regexp(t, "^WF.*", rsp.GetId(), "workflow ID should exist and match WF.*")
	assert.Regexp(t, "^.*/webhooks/workflow/.*", rsp.GetWebhookUrl(), "workflow webhook URL should exist and match /webhooks/workflow/.*")
	assert.True(t, rsp.GetWebhookRegistered())

	var row tables.Workflow
	err = te.GetDBHandle().DB(ctx).First(&row).Error
	require.NoError(t, err)
	assert.Equal(t, rsp.GetId(), row.WorkflowID, "inserted table workflow ID should match create response")
	assert.Equal(t, "GROUP1", row.UserID, "inserted table workflow user should match auth")
	assert.Equal(t, "GROUP1", row.GroupID, "inserted table workflow group should match auth")
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
	provider := setupFakeGitProvider(t, te)
	provider.RegisterWebhookError = fmt.Errorf("(fake error) You do not have permissions to register webhooks!")
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		Name:           "BuildBuddy OS Workflow",
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
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
	assert.Equal(t, "GROUP1", row.UserID, "inserted table workflow user should match auth")
	assert.Equal(t, "GROUP1", row.GroupID, "inserted table workflow group should match auth")
	assert.NotEmpty(t, row.WebhookID, "webhook ID in DB should be nonempty")
	assert.Contains(t, rsp.GetWebhookUrl(), row.WebhookID, "webhook ID in DB should match the URL")
	assert.Equal(t, "", row.GitProviderWebhookID, "git provider should not have returned a webhook ID")
}

func TestCreate_NonNormalizedRepoURL(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	// We are using a github.com URL to test normalization, so disable the repo
	// downloader just for this test so that it doesn't depend on GitHub.
	te.SetRepoDownloader(nil)
	setupFakeGitProvider(t, te)
	repoURL := "git@github.com:/foo/bar"
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		GitRepo: &wfpb.CreateWorkflowRequest_GitRepo{
			// Access token is required for GitHub URLs
			AccessToken: "test-access-token",
			RepoUrl:     repoURL,
		},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
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
	provider := setupFakeGitProvider(t, te)

	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	// Create a single workflow row.
	row := &tables.Workflow{
		WorkflowID:           "WF1",
		UserID:               "GROUP1",
		GroupID:              "GROUP1",
		Perms:                48,
		RepoURL:              "file:///ANY",
		GitProviderWebhookID: testgit.FakeWebhookID,
	}
	err := te.GetDBHandle().DB(ctx).Create(&row).Error
	assert.NoError(t, err)

	req := &wfpb.DeleteWorkflowRequest{Id: "WF1"}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
	_, err = bbClient.DeleteWorkflow(ctx, req)

	require.NoError(t, err)
	assert.Equal(t, testgit.FakeWebhookID, provider.UnregisteredWebhookID, "should unregister webhook upon deletion")

	err = te.GetDBHandle().DB(ctx).First(&row).Error
	assert.True(t, db.IsRecordNotFound(err))
}

func TestList(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)

	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	// Write some workflows.
	row := &tables.Workflow{
		WorkflowID: "WF1",
		UserID:     "GROUP1",
		GroupID:    "GROUP1",
		Perms:      48,
		RepoURL:    "file:///ANY",
		WebhookID:  "WHID1",
	}
	err := te.GetDBHandle().DB(ctx).Create(&row).Error
	require.NoError(t, err)

	row2 := &tables.Workflow{
		WorkflowID: "WF2",
		UserID:     "GROUP1",
		GroupID:    "GROUP1",
		Perms:      48,
		RepoURL:    "file:///ANY",
		WebhookID:  "WHID2",
	}
	err = te.GetDBHandle().DB(ctx).Create(&row2).Error
	require.NoError(t, err)

	row3 := &tables.Workflow{
		WorkflowID: "WF3",
		UserID:     "GROUP2",
		GroupID:    "GROUP2",
		Perms:      48,
		RepoURL:    "file:///ANY",
		WebhookID:  "WHID3",
	}
	err = te.GetDBHandle().DB(ctx).Create(&row3).Error
	require.NoError(t, err)

	user1Req := &wfpb.GetWorkflowsRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
	}
	ctx1 := metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
	rsp, err := bbClient.GetWorkflows(ctx1, user1Req)
	require.NoError(t, err)
	assert.Equal(t, 2, len(rsp.GetWorkflow()), "Two workflows owned by USER1 should be returned")

	user2Req := &wfpb.GetWorkflowsRequest{
		RequestContext: testauth.RequestContext("USER2", "GROUP2"),
	}
	ctx2 := metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER2")
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
	te := newTestEnv(t)
	execClient := &fakeExecutionClient{}
	te.SetRemoteExecutionClient(execClient)
	ws := te.GetWorkflowService()
	flags.Set(t, "app.build_buddy_url", *testhttp.StartServer(t, ws))
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
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

	require.Len(t, execClient.ExecuteRequests, 1, "expected one workflow execution to be started")
	exec := getExecution(t, ctx, te, execClient.ExecuteRequests[0])
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
	env := envVars(exec.Command)
	assert.NotContains(t, env, "BUILDBUDDY_API_KEY", "untrusted workflow should not have BUILDBUDDY_API_KEY env var")
}

func TestWebhook_TrustedPullRequest_StartsTrustedWorkflow(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	execClient := &fakeExecutionClient{}
	te.SetRemoteExecutionClient(execClient)
	ws := te.GetWorkflowService()
	flags.Set(t, "app.build_buddy_url", *testhttp.StartServer(t, ws))
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
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

	require.Len(t, execClient.ExecuteRequests, 1, "expected one workflow execution to be started")
	exec := getExecution(t, ctx, te, execClient.ExecuteRequests[0])
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
	env := envVars(exec.Command)
	assert.Contains(t, env, "BUILDBUDDY_API_KEY", "trusted workflow should have BUILDBUDDY_API_KEY env var")
}

func TestWebhook_TrustedApprovalOnUntrustedPullRequest_StartsTrustedWorkflow(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	execClient := &fakeExecutionClient{}
	te.SetRemoteExecutionClient(execClient)
	ws := te.GetWorkflowService()
	flags.Set(t, "app.build_buddy_url", *testhttp.StartServer(t, ws))
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
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

	require.Len(t, execClient.ExecuteRequests, 1, "expected one workflow execution to be started")
	exec := getExecution(t, ctx, te, execClient.ExecuteRequests[0])
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
	env := envVars(exec.Command)
	assert.Contains(t, env, "BUILDBUDDY_API_KEY", "trusted workflow should have BUILDBUDDY_API_KEY env var")
}

func TestWebhook_TrustedApprovalOnAlreadyTrustedPullRequest_NOP(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	execClient := &fakeExecutionClient{}
	te.SetRemoteExecutionClient(execClient)
	ws := te.GetWorkflowService()
	flags.Set(t, "app.build_buddy_url", *testhttp.StartServer(t, ws))
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
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

	require.Len(t, execClient.ExecuteRequests, 0, "expected no workflow executions to be started")
}

func TestWebhook_UntrustedApprovalOnUntrustedPullRequest_NOP(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	execClient := &fakeExecutionClient{}
	te.SetRemoteExecutionClient(execClient)
	ws := te.GetWorkflowService()
	flags.Set(t, "app.build_buddy_url", *testhttp.StartServer(t, ws))
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
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

	require.Len(t, execClient.ExecuteRequests, 0, "expected no workflow executions to be started")
}

func TestWebhook_TrustedPush_StartsTrustedWorkflow(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	execClient := &fakeExecutionClient{}
	te.SetRemoteExecutionClient(execClient)
	ws := te.GetWorkflowService()
	flags.Set(t, "app.build_buddy_url", *testhttp.StartServer(t, ws))
	flags.Set(t, "remote_execution.enable_remote_exec", true)
	provider := setupFakeGitProvider(t, te)
	repoURL := makeTempRepo(t)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)
	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
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

	require.Len(t, execClient.ExecuteRequests, 1, "expected one workflow execution to be started")
	exec := getExecution(t, ctx, te, execClient.ExecuteRequests[0])
	assert.Equal(t, "./buildbuddy_ci_runner", exec.Command.GetArguments()[0])
	env := envVars(exec.Command)
	assert.Contains(t, env, "BUILDBUDDY_API_KEY", "trusted workflow should have BUILDBUDDY_API_KEY env var")
}
