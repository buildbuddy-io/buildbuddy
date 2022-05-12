package workflows_test

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
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
  - name: "Test action"
    triggers: { push: { branches: [ master ] } }
    bazel_commands: [ "build //:nop" ]
`,
	}
)

func setup(t *testing.T, gp interfaces.GitProvider) (*rbetest.Env, interfaces.WorkflowService) {
	env := rbetest.NewRBETestEnv(t)
	var workflowService interfaces.WorkflowService

	bbServer := env.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		EnvModifier: func(env *testenv.TestEnv) {
			env.SetRepoDownloader(repo_downloader.NewRepoDownloader())
			env.SetGitProviders([]interfaces.GitProvider{gp})
			workflowService = service.NewWorkflowService(env)
			env.SetWorkflowService(workflowService)
			iss := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle())
			env.SetInvocationSearchService(iss)
		},
	})

	// Configure executors so that we can run at least one workflow task.
	// Workflow tasks require 8Gi to schedule, but all of our workflow test cases
	// typically use far less RAM. To enable this test to run on machines smaller
	// than 8Gi, force-set the executor memory_bytes to 10Gi so the workflow
	// tasks will schedule.
	flags.Set(t, "executor.memory_bytes", 10_000_000_000)
	// Use bare execution -- Docker isn't supported in tests yet.
	flags.Set(t, "remote_execution.workflows_default_image", "none")
	// Use a pre-built bazel instead of invoking bazelisk, which significantly
	// slows down the test.
	flags.Set(t, "remote_execution.workflows_ci_runner_bazel_command", testbazel.BinaryPath(t))
	// Set events_api_url to point to the test BB app server (this gets
	// propagated to the CI runner so it knows where to publish build events).
	u, err := url.Parse(fmt.Sprintf("grpc://localhost:%d", bbServer.GRPCPort()))
	require.NoError(t, err)
	flags.Set(t, "app.events_api_url", *u)

	env.AddExecutors(t, 10)
	return env, workflowService
}

func waitForAnyWorkflowInvocationCreated(t *testing.T, ctx context.Context, bb bbspb.BuildBuddyServiceClient, reqCtx *ctxpb.RequestContext) string {
	for delay := 50 * time.Millisecond; delay < 1*time.Minute; delay *= 2 {
		searchResp, err := bb.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
			RequestContext: reqCtx,
			Query:          &inpb.InvocationQuery{GroupId: reqCtx.GetGroupId()},
		})
		if err != nil && !strings.Contains(err.Error(), "not found") {
			require.NoError(t, err)
		}
		for _, in := range searchResp.GetInvocation() {
			if in.GetRole() == "CI_RUNNER" {
				return in.GetInvocationId()
			}
		}

		time.Sleep(delay)
	}

	require.FailNowf(t, "timeout", "Timed out waiting for workflow invocation to be created")
	return ""
}

func waitForInvocationComplete(t *testing.T, ctx context.Context, bb bbspb.BuildBuddyServiceClient, reqCtx *ctxpb.RequestContext, invocationID string) *inpb.Invocation {
	for delay := 50 * time.Millisecond; delay < 1*time.Minute; delay *= 2 {
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
			logResp, err := bb.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
				InvocationId: invocationID,
				MinLines:     math.MaxInt32,
			})
			require.NoError(t, err)
			inv.ConsoleBuffer = string(logResp.Buffer)
			return inv
		}

		time.Sleep(delay)
	}

	require.FailNowf(t, "timeout", "Timed out waiting for invocation to complete")
	return nil
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

// TestResponseWriter is an http.ResponseWriter that fails the test if an
// HTTP error code is written, but otherwise discards all data written to it.
type testResponseWriter struct {
	t      *testing.T
	header map[string][]string
}

func NewTestResponseWriter(t *testing.T) *testResponseWriter {
	return &testResponseWriter{
		t:      t,
		header: make(map[string][]string),
	}
}

func (w *testResponseWriter) WriteHeader(statusCode int) {
	require.Less(w.t, statusCode, 400, "Wrote HTTP status %d", statusCode)
}
func (w *testResponseWriter) Header() http.Header {
	return w.header
}
func (w *testResponseWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestCreateAndTriggerViaWebhook(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	env, workflowService := setup(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()
	repoPath, commitSHA := testgit.MakeTempRepo(t, simpleRepoContents)
	repoURL := fmt.Sprintf("file://%s", repoPath)

	// Create the workflow
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

	// Set up the fake git provider so that GetFileContents can return the
	// buildbuddy.yaml from our test repo
	fakeGitProvider.FileContents = simpleRepoContents
	// Configure the fake webhook data to be parsed from the response, then
	// trigger the webhook.
	fakeGitProvider.WebhookData = &interfaces.WebhookData{
		EventName:     "push",
		PushedRepoURL: repoURL,
		PushedBranch:  "master",
		SHA:           commitSHA,
		TargetRepoURL: repoURL,
		TargetBranch:  "master",
	}

	req, err := http.NewRequest("POST", createResp.GetWebhookUrl(), nil /*=body*/)
	require.NoError(t, err)
	workflowService.ServeHTTP(NewTestResponseWriter(t), req)

	iid := waitForAnyWorkflowInvocationCreated(t, ctx, bb, reqCtx)
	inv := waitForInvocationComplete(t, ctx, bb, reqCtx, iid)

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	require.Equal(t, repoURL, inv.GetRepoUrl())
	require.Equal(t, commitSHA, inv.GetCommitSha())
	require.Equal(t, "CI_RUNNER", inv.GetRole())
}

func TestCreateAndExecute(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	fakeGitProvider.FileContents = simpleRepoContents
	env, _ := setup(t, fakeGitProvider)
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

	execReq := &wfpb.ExecuteWorkflowRequest{
		RequestContext: reqCtx,
		WorkflowId:     createResp.GetId(),
		ActionName:     "Test action",
		CommitSha:      commitSHA,
		PushedRepoUrl:  repoURL,
		PushedBranch:   "master",
		TargetRepoUrl:  repoURL,
		TargetBranch:   "master",
	}

	execResp, err := bb.ExecuteWorkflow(ctx, execReq)

	require.NoError(t, err)
	require.NotEmpty(t, execResp.GetInvocationId())

	inv := waitForInvocationComplete(t, ctx, bb, reqCtx, execResp.GetInvocationId())

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	require.Equal(t, repoURL, inv.GetRepoUrl())
	require.Equal(t, commitSHA, inv.GetCommitSha())
	require.Equal(t, "CI_RUNNER", inv.GetRole())
	nActionsFirstRun := actionCount(t, inv)

	// Now run the workflow again and make sure the build is cached.

	execResp, err = bb.ExecuteWorkflow(ctx, execReq)

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
