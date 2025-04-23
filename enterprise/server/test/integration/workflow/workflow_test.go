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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
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

const (
	mockGithubAppID = int64(88888)
)

// simpleRepo simulates a test repo with the config files required to run a workflow
func simpleRepo() map[string]string {
	return map[string]string{
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
}

// repoWithSlowScript simulates a test repo with the config files required to run a workflow
// It sets up a slow script that takes a while to run so the CI runner does not return immediately,
// giving tests that need to modify the workflow (Ex. for testing cancellation) time to complete
func repoWithSlowScript() map[string]string {
	return map[string]string{
		"BUILD": `
sh_binary(
    name = "sleep_forever_test",
    srcs = ["sleep_forever_test.sh"],
)
`,
		"sleep_forever_test.sh": "tail -f /dev/null",
		"buildbuddy.yaml": `
actions:
  - name: "Slow test action"
    triggers: { push: { branches: [ master ] } }
    bazel_commands: [ "run //:sleep_forever_test" ]
    os: ` + runtime.GOOS + `
    arch: ` + runtime.GOARCH + `
`,
	}
}

func repoWithInvalidConfig() map[string]string {
	// Note the missing close-quote in the bazel command.
	return map[string]string{
		"buildbuddy.yaml": `
actions:
  - name: Test
    bazel_commands:
      - "test //...
`,
	}
}

func makeRepo(t *testing.T, contents map[string]string) (string, string) {
	repoPath := testbazel.MakeTempModule(t, contents)
	commitSHA := testgit.Init(t, repoPath)
	return repoPath, commitSHA
}

func setup(t *testing.T, gp interfaces.GitProvider) (*rbetest.Env, interfaces.WorkflowService) {
	env := rbetest.NewRBETestEnv(t)
	var workflowService interfaces.WorkflowService

	env.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		EnvModifier: func(e *testenv.TestEnv) {
			e.SetRepoDownloader(repo_downloader.NewRepoDownloader())
			e.SetGitProviders([]interfaces.GitProvider{gp})
			workflowService = service.NewWorkflowService(e)
			e.SetWorkflowService(workflowService)
			iss := invocation_search_service.NewInvocationSearchService(e, e.GetDBHandle(), e.GetOLAPDBHandle())
			e.SetInvocationSearchService(iss)
			e.SetByteStreamClient(env.GetByteStreamClient())
			gh, err := githubapp.NewAppService(e, &testgit.FakeGitHubApp{MockAppID: mockGithubAppID}, nil)
			require.NoError(t, err)
			e.SetGitHubAppService(gh)
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
	flags.Set(t, "github.app.enabled", true)

	// Uncomment this line to print output from the ci_runner to the terminal for debugging purposes
	// Otherwise, output from ci_runner/main.go and the bazel commands that are configured to run via the
	// workflow will not be printed
	//flags.Set(t, "debug_stream_command_outputs", true)

	env.AddExecutors(t, 10)
	return env, workflowService
}

func createWorkflow(t *testing.T, env *rbetest.Env, repoURL string) *tables.GitRepository {
	dbh := env.GetDBHandle()
	require.NotNil(t, dbh)
	repo := &tables.GitRepository{
		RepoURL: repoURL,
		GroupID: env.GroupID1,
		AppID:   mockGithubAppID,
		Perms:   perms.GROUP_READ | perms.GROUP_WRITE,
	}
	err := dbh.NewQuery(context.Background(), "create_git_repo_for_test").Create(repo)
	require.NoError(t, err)
	appInstallation := &tables.GitHubAppInstallation{
		GroupID: env.GroupID1,
		AppID:   mockGithubAppID,
	}
	err = dbh.NewQuery(context.Background(), "create_app_installation_for_test").Create(appInstallation)
	require.NoError(t, err)
	return repo
}

// trigger a webhook through the github app
// TODO(Maggie): Modify integration test to hit the /webhooks/github/app endpoint
// by mocking out more selective parts of the github app
func triggerWebhook(t *testing.T, ctx context.Context, gitProvider *testgit.FakeProvider, workflowService interfaces.WorkflowService, repo *tables.GitRepository, repoContents map[string]string, repoURL string, commitSHA string) {
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
	err := workflowService.HandleRepositoryEvent(ctx, repo, gitProvider.WebhookData, "faketoken")
	require.NoError(t, err)
}

// trigger a webhook using the legacy workflow mechanism
func triggerLegacyWebhook(t *testing.T, gitProvider *testgit.FakeProvider, workflowService interfaces.WorkflowService, repoContents map[string]string, repoURL string, commitSHA string, webhookURL string) {
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
	workflowService.ServeHTTP(testhttp.NewResponseWriter(t), req)
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
		inv := getInvocation(t, ctx, bb, reqCtx, invocationID, false /*fetchChildren*/)
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

func getInvocation(t *testing.T, ctx context.Context, bb bbspb.BuildBuddyServiceClient, reqCtx *ctxpb.RequestContext, invocationID string, fetchChildren bool) *inpb.Invocation {
	invResp, err := bb.GetInvocation(ctx, &inpb.GetInvocationRequest{
		RequestContext: reqCtx,
		Lookup:         &inpb.InvocationLookup{InvocationId: invocationID, FetchChildInvocations: fetchChildren},
	})
	require.NoError(t, err)
	require.Greater(t, len(invResp.GetInvocation()), 0)
	inv := invResp.GetInvocation()[0]
	return inv
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

func TestTriggerViaWebhook(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	env, workflowService := setup(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()

	repoContentsMap := simpleRepo()
	repoPath, commitSHA := makeRepo(t, repoContentsMap)
	repoURL := fmt.Sprintf("file://%s", repoPath)

	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}
	repo := createWorkflow(t, env, repoURL)

	triggerWebhook(t, ctx, fakeGitProvider, workflowService, repo, repoContentsMap, repoURL, commitSHA)

	iid := waitForAnyWorkflowInvocationCreated(t, ctx, bb, reqCtx)
	inv := waitForInvocationStatus(t, ctx, bb, reqCtx, iid, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS)

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	require.Equal(t, repoURL, inv.GetRepoUrl())
	require.Equal(t, commitSHA, inv.GetCommitSha())
	require.Equal(t, "CI_RUNNER", inv.GetRole())
}

func TestTriggerLegacyWorkflowViaWebhook(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	env, workflowService := setup(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()

	repoContentsMap := simpleRepo()
	repoPath, commitSHA := makeRepo(t, repoContentsMap)
	repoURL := fmt.Sprintf("file://%s", repoPath)

	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}
	dbh := env.GetDBHandle()
	require.NotNil(t, dbh)
	webhookID := "12345"
	wf := &tables.Workflow{
		RepoURL:              repoURL,
		GroupID:              env.GroupID1,
		WebhookID:            webhookID,
		WorkflowID:           workflowService.GetLegacyWorkflowIDForGitRepository(env.GroupID1, repoURL),
		GitProviderWebhookID: testgit.FakeWebhookID,
	}
	err := dbh.NewQuery(context.Background(), "create_wf_for_test").Create(wf)
	require.NoError(t, err)

	webhookURL := fmt.Sprintf("%s/webhooks/workflow/%s", build_buddy_url.WithPath("").String(), webhookID)
	triggerLegacyWebhook(t, fakeGitProvider, workflowService, repoContentsMap, repoURL, commitSHA, webhookURL)

	iid := waitForAnyWorkflowInvocationCreated(t, ctx, bb, reqCtx)
	inv := waitForInvocationStatus(t, ctx, bb, reqCtx, iid, inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS)

	require.True(t, inv.GetSuccess(), "workflow invocation should succeed")
	require.Equal(t, repoURL, inv.GetRepoUrl())
	require.Equal(t, commitSHA, inv.GetCommitSha())
	require.Equal(t, "CI_RUNNER", inv.GetRole())
}

func TestExecuteWorkflow(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	fakeGitProvider.FileContents = simpleRepo()
	env, wfService := setup(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()

	repoPath, commitSHA := makeRepo(t, simpleRepo())
	repoURL := fmt.Sprintf("file://%s", repoPath)
	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}
	createWorkflow(t, env, repoURL)

	// actionName should match an action name from buildbuddy.yaml to tell the workflow service which action config to use
	actionName := "Test action"
	workflowID := wfService.GetLegacyWorkflowIDForGitRepository(env.GroupID1, repoURL)
	execReq := &wfpb.ExecuteWorkflowRequest{
		RequestContext: reqCtx,
		WorkflowId:     workflowID,
		CommitSha:      commitSHA,
		PushedRepoUrl:  repoURL,
		PushedBranch:   "master",
		TargetRepoUrl:  repoURL,
		TargetBranch:   "master",
		ActionNames:    []string{actionName},
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

func TestExecuteAsync(t *testing.T) {
	// Try to run a workflow action that takes forever, to test that we don't
	// have to wait for the action to be completed when `async` is set in the
	// ExecuteWorkflowRequest.
	repoContents := repoWithSlowScript()
	fakeGitProvider := testgit.NewFakeProvider()
	fakeGitProvider.FileContents = repoContents
	env, wfService := setup(t, fakeGitProvider)
	bb := env.GetBuildBuddyServiceClient()

	repoPath, commitSHA := makeRepo(t, repoContents)
	repoURL := fmt.Sprintf("file://%s", repoPath)
	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}
	createWorkflow(t, env, repoURL)

	// actionName should match an action name from buildbuddy.yaml to tell the workflow service which action config to use
	actionName := "Test action"
	workflowID := wfService.GetLegacyWorkflowIDForGitRepository(env.GroupID1, repoURL)
	execReq := &wfpb.ExecuteWorkflowRequest{
		RequestContext: reqCtx,
		WorkflowId:     workflowID,
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
	repoPath, commitSHA := makeRepo(t, repoContentsMap)
	repoURL := fmt.Sprintf("file://%s", repoPath)

	// Create the workflow
	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}
	repo := createWorkflow(t, env, repoURL)

	// Trigger the workflow to run
	triggerWebhook(t, ctx, fakeGitProvider, workflowService, repo, repoContentsMap, repoURL, commitSHA)

	// Cancel workflow
	iid := waitForAnyWorkflowInvocationCreated(t, ctx, bb, reqCtx)
	cancelResp, err := bb.CancelExecutions(ctx, &inpb.CancelExecutionsRequest{
		RequestContext: reqCtx,
		InvocationId:   iid,
	})
	require.NoError(t, err)

	inv := waitForInvocationStatus(t, ctx, bb, reqCtx, iid, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS)
	require.NotNil(t, cancelResp)
	require.NotNil(t, inv)

	invWithChildren := getInvocation(t, ctx, bb, reqCtx, iid, true /*fetchChildren*/)
	// Check that no child invocations are still in the running state. The builds should
	// either have completed or been disconnected.
	for _, child := range invWithChildren.ChildInvocations {
		found := false
		for delay := 50 * time.Millisecond; delay < 1*time.Minute; delay *= 2 {
			inv := getInvocation(t, ctx, bb, reqCtx, child.InvocationId, false /*fetchChildren*/)
			status := inv.GetInvocationStatus()

			if status == inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS || status == inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS {
				found = true
				break
			}
			time.Sleep(delay)
		}

		if !found {
			require.FailNowf(t, "timeout", "Timed out waiting for child to either complete or be disconeccted")
		}
	}
}

func TestInvalidYAML(t *testing.T) {
	fakeGitProvider := testgit.NewFakeProvider()
	env, workflowService := setup(t, fakeGitProvider)

	repoContentsMap := repoWithInvalidConfig()
	repoPath, commitSHA := makeRepo(t, repoContentsMap)
	repoURL := fmt.Sprintf("file://%s", repoPath)

	// Create the workflow
	ctx := env.WithUserID(context.Background(), env.UserID1)
	repo := createWorkflow(t, env, repoURL)

	// Attempt to trigger the workflow
	triggerWebhook(t, ctx, fakeGitProvider, workflowService, repo, repoContentsMap, repoURL, commitSHA)

	// Make sure we published an invalid YAML status
	s := <-fakeGitProvider.Statuses
	require.Equal(t, &testgit.Status{
		AccessToken: "faketoken",
		RepoURL:     repoURL,
		CommitSHA:   commitSHA,
		Payload: &github.GithubStatusPayload{
			Context:     pointer("BuildBuddy Workflows"),
			Description: pointer("Invalid buildbuddy.yaml"),
			TargetURL:   pointer("https://buildbuddy.io/docs/workflows-config"),
			State:       pointer("error"),
		},
	}, s)
}

func pointer[T any](val T) *T {
	return &val
}
