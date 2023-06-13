package workflows_test

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
)

// simpleRepo simulates a test repo with the config files required to run a workflow
func simpleRepo() map[string]string {
	fileNameToFileContentsMap := map[string]string{
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
    os: ` + runtime.GOOS + `
    arch: ` + runtime.GOARCH + `
`,
	}

	return fileNameToFileContentsMap
}

// repoWithSlowScript simulates a test repo with the config files required to run a workflow
// It sets up a slow script that takes a while to run so the CI runner does not return immediately,
// giving tests that need to modify the workflow (Ex. for testing cancellation) time to complete
func repoWithSlowScript() map[string]string {
	fileNameToFileContentsMap := map[string]string{
		"WORKSPACE": `workspace(name = "test")`,
		"BUILD": `
sh_binary(
    name = "sleep_forever_test",
    srcs = ["sleep_forever_test.sh"],
)
`,
		"sleep_forever_test.sh": "sleep infinity",
		"buildbuddy.yaml": `
actions:
  - name: "Slow test action"
    triggers: { push: { branches: [ master ] } }
    bazel_commands: [ "run //:sleep_forever_test" ]
    os: ` + runtime.GOOS + `
    arch: ` + runtime.GOARCH + `
`,
	}

	return fileNameToFileContentsMap
}

func setup(t *testing.T, gp interfaces.GitProvider) (*rbetest.Env, interfaces.WorkflowService) {
	env := rbetest.NewRBETestEnv(t)
	var workflowService interfaces.WorkflowService

	bbServer := env.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		EnvModifier: func(env *testenv.TestEnv) {
			env.SetRepoDownloader(repo_downloader.NewRepoDownloader())
			env.SetGitProviders([]interfaces.GitProvider{gp})
			workflowService = service.NewWorkflowService(env)
			env.SetWorkflowService(workflowService)
			iss := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())
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

	// Uncomment this line to print output from the ci_runner to the terminal for debugging purposes
	// Otherwise, output from ci_runner/main.go and the bazel commands that are configured to run via the
	// workflow will not be printed
	//flags.Set(t, "debug_stream_command_outputs", true)

	env.AddExecutors(t, 10)
	return env, workflowService
}

func triggerWebhook(t *testing.T, gitProvider *testgit.FakeProvider, workflowService interfaces.WorkflowService, repoContents map[string]string, repoURL string, commitSHA string, webhookURL string) {
	// Set up the fake git provider so that GetFileContents can return the
	// buildbuddy.yaml from our test repo
	gitProvider.FileContents = repoContents
	// Configure the fake webhook data to be parsed from the response
	gitProvider.WebhookData = &interfaces.WebhookData{
		EventName:     "push",
		PushedRepoURL: repoURL,
		PushedBranch:  "master",
		SHA:           commitSHA,
		TargetRepoURL: repoURL,
		TargetBranch:  "master",
	}
	// Generate a "push event" to Github. As configured under `triggers` in buildbuddy.yaml, a push
	// should trigger the action to be executed on an executor spun up by the test
	req, err := http.NewRequest("POST", webhookURL, nil /*=body*/)
	require.NoError(t, err)
	workflowService.ServeHTTP(NewTestResponseWriter(t), req)
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

func waitForInvocationStatus(t *testing.T, ctx context.Context, bb bbspb.BuildBuddyServiceClient, reqCtx *ctxpb.RequestContext, invocationID string, expectedStatus inspb.InvocationStatus) *inpb.Invocation {
	for delay := 50 * time.Millisecond; delay < 1*time.Minute; delay *= 2 {
		invResp, err := bb.GetInvocation(ctx, &inpb.GetInvocationRequest{
			RequestContext: reqCtx,
			Lookup:         &inpb.InvocationLookup{InvocationId: invocationID},
		})
		require.NoError(t, err)
		require.Greater(t, len(invResp.GetInvocation()), 0)
		inv := invResp.GetInvocation()[0]
		status := inv.GetInvocationStatus()

		if status == expectedStatus {
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

	require.FailNowf(t, "timeout", "Timed out waiting for invocation to reach expected status %v", expectedStatus)
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

	repoContentsMap := simpleRepo()
	repoPath, commitSHA := testgit.MakeTempRepo(t, repoContentsMap)
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

	triggerWebhook(t, fakeGitProvider, workflowService, repoContentsMap, repoURL, commitSHA, createResp.GetWebhookUrl())

	iid := waitForAnyWorkflowInvocationCreated(t, ctx, bb, reqCtx)
	inv := waitForInvocationStatus(t, ctx, bb, reqCtx, iid, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS)

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	require.Equal(t, repoURL, inv.GetRepoUrl())
	require.Equal(t, commitSHA, inv.GetCommitSha())
	require.Equal(t, "CI_RUNNER", inv.GetRole())
}

func TestCreateAndExecute(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	fakeGitProvider.FileContents = simpleRepo()
	env, _ := setup(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()

	repoPath, commitSHA := testgit.MakeTempRepo(t, simpleRepo())
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

	// actionName should match an action name from buildbuddy.yaml to tell the workflow service which action config to use
	actionName := "Test action"
	execReq := &wfpb.ExecuteWorkflowRequest{
		RequestContext: reqCtx,
		WorkflowId:     createResp.GetId(),
		ActionName:     actionName,
		CommitSha:      commitSHA,
		PushedRepoUrl:  repoURL,
		PushedBranch:   "master",
		TargetRepoUrl:  repoURL,
		TargetBranch:   "master",
	}

	execResp, err := bb.ExecuteWorkflow(ctx, execReq)

	require.NoError(t, err)
	require.Equal(t, 1, len(execResp.GetActionStatuses()))

	invocationID := execResp.GetActionStatuses()[0].InvocationId
	inv := waitForInvocationStatus(t, ctx, bb, reqCtx, invocationID, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS)

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	require.Equal(t, repoURL, inv.GetRepoUrl())
	require.Equal(t, commitSHA, inv.GetCommitSha())
	require.Equal(t, "CI_RUNNER", inv.GetRole())
	nActionsFirstRun := actionCount(t, inv)

	// Now run the workflow again and make sure the build is cached.
	execResp, err = bb.ExecuteWorkflow(ctx, execReq)
	require.NoError(t, err)
	require.Equal(t, 1, len(execResp.GetActionStatuses()))

	invocationID = execResp.GetActionStatuses()[0].InvocationId
	inv = waitForInvocationStatus(t, ctx, bb, reqCtx, invocationID, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS)

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	nActionsSecondRun := actionCount(t, inv)
	require.Less(
		t, nActionsSecondRun, nActionsFirstRun,
		"should execute fewer actions on second run since build should be cached",
	)
}

func TestCreateAndExecuteAsync(t *testing.T) {
	// Try to run a workflow action that takes forever, to test that we don't
	// have to wait for the action to be completed when `async` is set in the
	// ExecuteWorkflowRequest.
	repoContents := repoWithSlowScript()
	fakeGitProvider := testgit.NewFakeProvider()
	fakeGitProvider.FileContents = repoContents
	env, _ := setup(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()

	repoPath, commitSHA := testgit.MakeTempRepo(t, repoContents)
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

	// actionName should match an action name from buildbuddy.yaml to tell the workflow service which action config to use
	actionName := "Test action"
	execReq := &wfpb.ExecuteWorkflowRequest{
		RequestContext: reqCtx,
		WorkflowId:     createResp.GetId(),
		ActionName:     actionName,
		CommitSha:      commitSHA,
		PushedRepoUrl:  repoURL,
		PushedBranch:   "master",
		TargetRepoUrl:  repoURL,
		TargetBranch:   "master",
		Async:          true,
	}

	// Patch the BES URL so that the workflow invocation can't be successfully
	// created. The Execute response should still return an OK status, because
	// we're running with `Async: true`.
	u, err := url.Parse("grpc://0.1.1.1:1")
	require.NoError(t, err)
	flags.Set(t, "app.events_api_url", *u)

	execResp, err := bb.ExecuteWorkflow(ctx, execReq)

	require.NoError(t, err)
	require.Equal(t, 1, len(execResp.GetActionStatuses()))

	as := execResp.GetActionStatuses()[0]
	require.Equal(t, int32(codes.OK), as.GetStatus().GetCode())
	require.NotEmpty(t, as.GetInvocationId())
}

func TestCancel(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	env, workflowService := setup(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()

	// Set up slow script to give cancellation time to complete
	repoContentsMap := repoWithSlowScript()
	repoPath, commitSHA := testgit.MakeTempRepo(t, repoContentsMap)
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

	// Trigger the workflow to run
	triggerWebhook(t, fakeGitProvider, workflowService, repoContentsMap, repoURL, commitSHA, createResp.GetWebhookUrl())

	// Cancel workflow
	iid := waitForAnyWorkflowInvocationCreated(t, ctx, bb, reqCtx)
	cancelResp, err := bb.CancelExecutions(ctx, &inpb.CancelExecutionsRequest{
		RequestContext: reqCtx,
		InvocationId:   iid,
	})

	inv := waitForInvocationStatus(t, ctx, bb, reqCtx, iid, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS)
	require.NoError(t, err)
	require.NotNil(t, cancelResp)
	require.NotNil(t, inv)
}
