package service_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"

	workflow "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
)

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
	te := testenv.GetTestEnv(t)
	te.SetWorkflowService(workflow.NewWorkflowService(te))

	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(authenticator)

	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &wfpb.CreateWorkflowRequest{
		RequestContext: testauth.RequestContext("USER1", "GROUP1"),
		Name:           "BuildBuddy OS Workflow",
		GitRepo: &wfpb.CreateWorkflowRequest_GitRepo{
			RepoUrl: "git@github.com:buildbuddy-io/buildbuddy.git",
		},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.TestApiKeyHeader, "USER1")
	rsp, err := bbClient.CreateWorkflow(ctx, req)
	assert.Nil(t, err)
	assert.Regexp(t, "^WF.*", rsp.GetId(), "workflow ID should exist and match WF.*")
	assert.Regexp(t, "^.*/webhooks/workflow/.*", rsp.GetWebhookUrl(), "workflow webhook URL should exist and match /webhooks/workflow/.*")

	var row tables.Workflow
	err = te.GetDBHandle().First(&row).Error
	assert.Nil(t, err)
	assert.Equal(t, rsp.GetId(), row.WorkflowID, "inserted table workflow ID should match create response")
	assert.Equal(t, "GROUP1", row.UserID, "inserted table workflow user should match auth")
	assert.Equal(t, "GROUP1", row.GroupID, "inserted table workflow group should match auth")
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetWorkflowService(workflow.NewWorkflowService(te))
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(authenticator)

	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	// Create a single workflow row.
	row := &tables.Workflow{
		Name:       "Workflow to be deleted",
		WorkflowID: "WF1",
		UserID:     "GROUP1",
		GroupID:    "GROUP1",
		Perms:      48,
		RepoURL:    "git@github.com:buildbuddy-io/buildbuddy.git",
	}
	err := te.GetDBHandle().Create(&row).Error
	assert.Nil(t, err)

	req := &wfpb.DeleteWorkflowRequest{Id: "WF1"}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.TestApiKeyHeader, "USER1")
	_, err = bbClient.DeleteWorkflow(ctx, req)
	assert.Nil(t, err)

	err = te.GetDBHandle().First(&row).Error
	assert.ErrorIs(t, gorm.ErrRecordNotFound, err)
}

func TestList(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	te.SetWorkflowService(workflow.NewWorkflowService(te))
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1", "USER2", "GROUP2"))
	te.SetAuthenticator(authenticator)

	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	// Write some workflows.
	row := &tables.Workflow{
		Name:       "Workflow to be deleted",
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
		Name:       "Workflow to be deleted",
		WorkflowID: "WF2",
		UserID:     "GROUP1",
		GroupID:    "GROUP1",
		Perms:      48,
		RepoURL:    "git@github.com:buildbuddy-io/buildbuddy-internal.git",
		WebhookID:  "WHID2",
	}
	err = te.GetDBHandle().Create(&row2).Error
	assert.Nil(t, err)

	row3 := &tables.Workflow{
		Name:       "Workflow to be deleted",
		WorkflowID: "WF3",
		UserID:     "GROUP2",
		GroupID:    "GROUP2",
		Perms:      48,
		RepoURL:    "git@github.com:someOtherRepo",
		WebhookID:  "WHID3",
	}
	err = te.GetDBHandle().Create(&row3).Error
	assert.Nil(t, err)

	req := &wfpb.GetWorkflowsRequest{}
	ctx1 := metadata.AppendToOutgoingContext(ctx, testauth.TestApiKeyHeader, "USER1")
	rsp, err := bbClient.GetWorkflows(ctx1, req)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(rsp.GetWorkflow()), "Two workflows owned by USER1 should be returned")

	ctx2 := metadata.AppendToOutgoingContext(ctx, testauth.TestApiKeyHeader, "USER2")
	rsp, err = bbClient.GetWorkflows(ctx2, req)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(rsp.GetWorkflow()), "One workflow owned by USER2 should be returned")

	if len(rsp.GetWorkflow()) == 1 {
		wf := rsp.GetWorkflow()[0]
		assert.Equal(t, &wfpb.GetWorkflowsResponse_Workflow{
			Id:         "WF3",
			Name:       "Workflow to be deleted",
			RepoUrl:    "git@github.com:someOtherRepo",
			WebhookUrl: "http://localhost:8080/webhooks/workflow/WHID3",
		}, wf, "Expected fields should be returned")
	}
}
