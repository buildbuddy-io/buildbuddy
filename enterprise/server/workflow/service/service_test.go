package service_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
)

func newTestEnv(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetWorkflowService(workflow.NewWorkflowService(te))
	te.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers(
		"USER1", "GROUP1",
		"USER2", "GROUP2",
	)))
	return te
}

func setupFakeGitProvider(t *testing.T, te *testenv.TestEnv) *testgit.FakeProvider {
	provider := testgit.NewFakeProvider()
	te.SetGitProviders([]interfaces.GitProvider{provider})
	return provider
}

func runBBServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	buildBuddyServer, err := buildbuddy_server.NewBuildBuddyServer(env /*sslService=*/, nil)
	if err != nil {
		t.Error(err)
	}

	grpcServer, runFunc := env.LocalGRPCServer()
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, buildBuddyServer)

	go runFunc()

	clientConn, err := env.LocalGRPCConn(ctx)
	if err != nil {
		t.Error(err)
	}

	return clientConn
}

func TestCreate(t *testing.T) {
	ctx := context.Background()
	te := newTestEnv(t)
	provider := setupFakeGitProvider(t, te)
	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		Name:           "BuildBuddy OS Workflow",
		GitRepo: &wfpb.CreateWorkflowRequest_GitRepo{
			RepoUrl: "git@github.com:buildbuddy-io/buildbuddy.git",
		},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
	rsp, err := bbClient.CreateWorkflow(ctx, req)

	assert.NoError(t, err)
	assert.Regexp(t, "^WF.*", rsp.GetId(), "workflow ID should exist and match WF.*")
	assert.Regexp(t, "^.*/webhooks/workflow/.*", rsp.GetWebhookUrl(), "workflow webhook URL should exist and match /webhooks/workflow/.*")
	assert.True(t, rsp.GetWebhookRegistered())

	var row tables.Workflow
	err = te.GetDBHandle().First(&row).Error
	assert.NoError(t, err)
	assert.Equal(t, rsp.GetId(), row.WorkflowID, "inserted table workflow ID should match create response")
	assert.Equal(t, "GROUP1", row.UserID, "inserted table workflow user should match auth")
	assert.Equal(t, "GROUP1", row.GroupID, "inserted table workflow group should match auth")
	assert.Equal(
		t, testgit.FakeWebhookID, row.GitProviderWebhookID,
		"inserted table workflow git provider webhook ID should be set based on git provider response",
	)

	assert.Equal(t, rsp.GetWebhookUrl(), provider.RegisteredWebhookURL, "returned webhook URL should be registered to the provider")
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
		RepoURL:              "git@github.com:buildbuddy-io/buildbuddy.git",
		GitProviderWebhookID: testgit.FakeWebhookID,
	}
	err := te.GetDBHandle().Create(&row).Error
	assert.NoError(t, err)

	req := &wfpb.DeleteWorkflowRequest{Id: "WF1"}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
	_, err = bbClient.DeleteWorkflow(ctx, req)

	assert.Nil(t, err)
	assert.Equal(t, testgit.FakeWebhookID, provider.UnregisteredWebhookID, "should unregister webhook upon deletion")

	err = te.GetDBHandle().First(&row).Error
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
		RepoURL:    "git@github.com:buildbuddy-io/buildbuddy.git",
		WebhookID:  "WHID1",
	}
	err := te.GetDBHandle().Create(&row).Error
	assert.Nil(t, err)

	row2 := &tables.Workflow{
		WorkflowID: "WF2",
		UserID:     "GROUP1",
		GroupID:    "GROUP1",
		Perms:      48,
		RepoURL:    "git@github.com:buildbuddy-io/buildbuddy-internal.git",
		WebhookID:  "WHID2",
	}
	err = te.GetDBHandle().Create(&row2).Error
	assert.NoError(t, err)

	row3 := &tables.Workflow{
		WorkflowID: "WF3",
		UserID:     "GROUP2",
		GroupID:    "GROUP2",
		Perms:      48,
		RepoURL:    "git@github.com:someOtherRepo",
		WebhookID:  "WHID3",
	}
	err = te.GetDBHandle().Create(&row3).Error
	assert.NoError(t, err)

	req := &wfpb.GetWorkflowsRequest{}
	ctx1 := metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER1")
	rsp, err := bbClient.GetWorkflows(ctx1, req)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rsp.GetWorkflow()), "Two workflows owned by USER1 should be returned")

	ctx2 := metadata.AppendToOutgoingContext(ctx, testauth.APIKeyHeader, "USER2")
	rsp, err = bbClient.GetWorkflows(ctx2, req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(rsp.GetWorkflow()), "One workflow owned by USER2 should be returned")

	if len(rsp.GetWorkflow()) == 1 {
		wf := rsp.GetWorkflow()[0]
		assert.Equal(t, &wfpb.GetWorkflowsResponse_Workflow{
			Id:         "WF3",
			RepoUrl:    "git@github.com:someOtherRepo",
			WebhookUrl: "http://localhost:8080/webhooks/workflow/WHID3",
		}, wf, "Expected fields should be returned")
	}
}
