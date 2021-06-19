package workflows_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
)

var (
	simpleRepoContents = map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"buildbuddy.yaml": `
actions:
  - name: "Version"
    triggers: { push: { branches: [ master ] } }
    bazel_commands: [ version ]
`,
	}
)

func newWorkflowsTestEnv(t *testing.T, gp interfaces.GitProvider) *rbetest.Env {
	port := app.FreePort(t)
	flags.Set(t, "app.events_api_url", fmt.Sprintf("grpc://localhost:%d", port))
	flags.Set(t, "remote_execution.workflows_default_image", "none")
	flags.Set(t, "remote_execution.workflows_ci_runner_bazel_command", testbazel.BinaryPath(t))

	env := rbetest.NewRBETestEnv(t)
	env.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		GRPCPort: port,
		EnvModifier: func(env *testenv.TestEnv) {
			env.SetRepoDownloader(repo_downloader.NewRepoDownloader())
			env.SetGitProviders([]interfaces.GitProvider{gp})
			env.SetWorkflowService(service.NewWorkflowService(env))
		},
	})
	env.AddExecutors(2)
	return env
}

func waitForInvocationComplete(t *testing.T, ctx context.Context, bb bbspb.BuildBuddyServiceClient, reqCtx *ctxpb.RequestContext, invocationID string) *inpb.Invocation {
	for {
		invResp, err := bb.GetInvocation(ctx, &inpb.GetInvocationRequest{
			RequestContext: reqCtx,
			Lookup:         &inpb.InvocationLookup{InvocationId: invocationID},
		})
		require.NoError(t, err)
		require.Greater(t, len(invResp.GetInvocation()), 0)
		inv := invResp.GetInvocation()[0]
		status := inv.GetInvocationStatus()
		require.NotEqual(t, inpb.Invocation_DISCONNECTED_INVOCATION_STATUS, status)

		if status == inpb.Invocation_COMPLETE_INVOCATION_STATUS {
			return inv
		}

		<-time.After(50 * time.Millisecond)
	}
}

func TestCreateAndExecute(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	env := newWorkflowsTestEnv(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()
	repoPath, commitSHA := testgit.MakeTempRepo(t, simpleRepoContents)
	repoURL := fmt.Sprintf("file://%s", repoPath)
	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}

	createResp, err := bb.CreateWorkflow(ctx, &wfpb.CreateWorkflowRequest{
		RequestContext: reqCtx,
		GitRepo:        &wfpb.CreateWorkflowRequest_GitRepo{RepoUrl: repoURL},
	})

	require.NoError(t, err)
	require.NotEmpty(t, createResp.GetId())

	execResp, err := bb.ExecuteWorkflow(ctx, &wfpb.ExecuteWorkflowRequest{
		RequestContext: reqCtx,
		WorkflowId:     createResp.GetId(),
		ActionName:     "Version",
		CommitSha:      commitSHA,
		Branch:         "master",
	})

	require.NoError(t, err)
	require.NotEmpty(t, execResp.GetInvocationId())

	inv := waitForInvocationComplete(t, ctx, bb, reqCtx, execResp.GetInvocationId())

	assert.True(t, inv.GetSuccess())
	assert.Equal(t, repoURL, inv.GetRepoUrl())
	assert.Equal(t, commitSHA, inv.GetCommitSha())
	assert.Equal(t, "CI_RUNNER", inv.GetRole())
}
