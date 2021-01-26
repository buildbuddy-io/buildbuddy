package workflow_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/auth"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
)

func runBBServer(ctx context.Context, env *test_environment.TestEnv, t *testing.T) *grpc.ClientConn {
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
	te, err := test_environment.GetTestEnv()
	if err != nil {
		t.Error(err)
	}
	authenticator := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(authenticator)

	clientConn := runBBServer(ctx, te, t)
	bbClient := bbspb.NewBuildBuddyServiceClient(clientConn)

	req := &trpb.CreateWorkflowRequest{
		Name: "BuildBuddy OS Workflow",
		GitRepo: &trpb.CreateWorkflowRequest_GitRepo{
			RepoUrl: "git@github.com:buildbuddy-io/buildbuddy.git",
		},
	}
	ctx = metadata.AppendToOutgoingContext(ctx, testauth.TestApiKeyHeader, "USER1")
	rsp, err := bbClient.CreateWorkflow(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	assert.Regexp(t, "^WF.*", rsp.GetId(), "workflow ID should exist and match WF.*")
	assert.Regexp(t, "^.*/webhooks/workflow/.*", rsp.GetWebhookUrl(), "workflow webhook URL should exist and match /webhooks/workflow/.*")
}
