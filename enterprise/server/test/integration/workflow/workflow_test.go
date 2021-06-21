package workflows_test

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
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
		"BUILD": `
sh_binary(
    name = "nop",
    srcs = ["nop.sh"],
)
`,
		"nop.sh": ``,
		"buildbuddy.yaml": `
actions:
  - name: "Version"
    triggers: { push: { branches: [ master ] } }
    bazel_commands: [ "build //:nop" ]
`,
	}
)

func newWorkflowsTestEnv(t *testing.T, gp interfaces.GitProvider) *rbetest.Env {
	port := app.FreePort(t)
	// Set events_api_url to point to the test BB app server (this gets
	// propagated to the CI runner so it knows where to publish build events).
	flags.Set(t, "app.events_api_url", fmt.Sprintf("grpc://localhost:%d", port))
	// Use bare execution -- Docker isn't supported in tests yet.
	flags.Set(t, "remote_execution.workflows_default_image", "none")
	// Use a pre-built bazel instead of invoking bazelisk, which significantly
	// slows down the test.
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
	env.AddExecutors(10)
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

func actionCount(t *testing.T, inv *inpb.Invocation) int {
	re := regexp.MustCompile(`([\d]+) total actions?`)
	matches := re.FindAllStringSubmatch(inv.GetConsoleBuffer(), -1)
	require.NotEmpty(t, matches)
	nStr := matches[0][1]
	n, err := strconv.Atoi(nStr)
	require.NoError(t, err)
	return n
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

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	require.Equal(t, repoURL, inv.GetRepoUrl())
	require.Equal(t, commitSHA, inv.GetCommitSha())
	require.Equal(t, "CI_RUNNER", inv.GetRole())
	nActionsFirstRun := actionCount(t, inv)

	// Now run the workflow again and make sure the build is cached.

	execResp, err = bb.ExecuteWorkflow(ctx, &wfpb.ExecuteWorkflowRequest{
		RequestContext: reqCtx,
		WorkflowId:     createResp.GetId(),
		ActionName:     "Version",
		CommitSha:      commitSHA,
		Branch:         "master",
	})

	require.NoError(t, err)
	require.NotEmpty(t, execResp.GetInvocationId())

	inv = waitForInvocationComplete(t, ctx, bb, reqCtx, execResp.GetInvocationId())

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	nActionsSecondRun := actionCount(t, inv)
	require.Less(
		t, nActionsSecondRun, nActionsFirstRun,
		"should execute fewer actions on second run since build should be cached",
	)
}
