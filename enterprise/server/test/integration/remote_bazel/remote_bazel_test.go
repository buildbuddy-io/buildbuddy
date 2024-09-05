package remote_bazel_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hostedrunner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_kvstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/repo_downloader"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgit"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func init() {
	// There is a race condition when the cli redirects stdout to a file to
	// capture some output. Mean while the health checker logs to stdout on
	// startup. Silence the logs to remove the race.
	*log.LogLevel = "warn"
	log.Configure()

}

func waitForInvocationCreated(t *testing.T, ctx context.Context, bb bbspb.BuildBuddyServiceClient, reqCtx *ctxpb.RequestContext) {
	for delay := 50 * time.Millisecond; delay < 1*time.Minute; delay *= 2 {
		searchResp, err := bb.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
			RequestContext: reqCtx,
			Query:          &inpb.InvocationQuery{GroupId: reqCtx.GetGroupId()},
		})
		if err != nil && !strings.Contains(err.Error(), "not found") {
			require.NoError(t, err)
		}
		for _, in := range searchResp.GetInvocation() {
			if in.GetRole() == "HOSTED_BAZEL" {
				return
			}
		}

		time.Sleep(delay)
	}

	require.FailNowf(t, "timeout", "Timed out waiting for workflow invocation to be created")
}

func waitForInvocationStatus(t *testing.T, ctx context.Context, bb bbspb.BuildBuddyServiceClient, reqCtx *ctxpb.RequestContext, invocationID string, expectedStatus inspb.InvocationStatus) {
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
			return
		}

		time.Sleep(delay)
	}

	require.FailNowf(t, "timeout", "Timed out waiting for invocation to reach expected status %v", expectedStatus)
}

func TestWithPublicRepo(t *testing.T) {
	// Use a dir that is persisted on recycled runners
	rootDir := "/root/workspace/remote-bazel-integration-test"
	err := os.Setenv("HOME", rootDir)
	require.NoError(t, err)

	err = os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	if _, err := os.Stat(fmt.Sprintf("%s/bazel-gazelle", rootDir)); os.IsNotExist(err) {
		output := testshell.Run(t, rootDir, "git clone https://github.com/bazelbuild/bazel-gazelle --filter=blob:none --depth=1")
		require.NotContains(t, output, "fatal")
	}

	err = os.Chdir(fmt.Sprintf("%s/bazel-gazelle", rootDir))
	require.NoError(t, err)

	// Run a server and executor locally to run remote bazel against
	env, bbServer, _ := runLocalServerAndExecutor(t, "")
	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}

	// Get an API key to authenticate the remote bazel request
	bbClient := env.GetBuildBuddyServiceClient()
	apiRsp, err := bbClient.CreateApiKey(ctx, &akpb.CreateApiKeyRequest{
		RequestContext: reqCtx,
		Capability: []akpb.ApiKey_Capability{
			akpb.ApiKey_CAS_WRITE_CAPABILITY,
			akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			akpb.ApiKey_ORG_ADMIN_CAPABILITY,
		},
	})
	require.NoError(t, err)
	apiKey := apiRsp.ApiKey.Value

	exitCode, err := remotebazel.HandleRemoteBazel([]string{
		fmt.Sprintf("--remote_runner=%s", bbServer.GRPCAddress()),
		// Have the ci runner use the "none" isolation type because it's simpler
		// to setup than a firecracker runner
		"--runner_exec_properties=workload-isolation-type=none",
		"--runner_exec_properties=container-image=",
		"help",
		fmt.Sprintf("--remote_header=x-buildbuddy-api-key=%s", apiKey)})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	// Check the invocation logs to ensure the "bazel help" command successfully run
	searchRsp, err := bbClient.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
		RequestContext: reqCtx,
		Query:          &inpb.InvocationQuery{GroupId: env.GroupID1},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(searchRsp.GetInvocation()))
	invocationID := searchRsp.Invocation[0].InvocationId

	logResp, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
		InvocationId: invocationID,
		MinLines:     math.MaxInt32,
	})
	require.NoError(t, err)
	require.Contains(t, string(logResp.GetBuffer()), "Usage: bazel <command> <options>")
}

func TestWithPrivateRepo(t *testing.T) {
	repoName := "private-test-repo"
	// If you need to re-generate this PAT, it should only have read access to
	// `private-test-repo`, and should be saved as a BB secret in all environments.
	username := "maggie-lou"
	personalAccessToken := os.Getenv("PRIVATE_TEST_REPO_GIT_ACCESS_TOKEN")
	repoURLWithToken := fmt.Sprintf("https://%s:%s@github.com/buildbuddy-io/private-test-repo.git", username, personalAccessToken)

	// Use a dir that is persisted on recycled runners
	rootDir := "/root/workspace/remote-bazel-integration-test"
	err := os.Setenv("HOME", rootDir)
	require.NoError(t, err)

	err = os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	if _, err := os.Stat(fmt.Sprintf("%s/%s", rootDir, repoName)); os.IsNotExist(err) {
		output := testshell.Run(t, rootDir, fmt.Sprintf("git clone %s --filter=blob:none --depth=1", repoURLWithToken))
		require.NotContains(t, output, "fatal")
	}

	err = os.Chdir(fmt.Sprintf("%s/%s", rootDir, repoName))
	require.NoError(t, err)

	// Run a server and executor locally to run remote bazel against
	env, bbServer, _ := runLocalServerAndExecutor(t, personalAccessToken)

	// Create a workflow for the same repo - will be used to fetch the git token
	dbh := env.GetDBHandle()
	require.NotNil(t, dbh)
	err = dbh.NewQuery(context.Background(), "create_git_repo_for_test").Create(&tables.GitRepository{
		RepoURL: "https://github.com/buildbuddy-io/private-test-repo",
		GroupID: env.GroupID1,
	})
	require.NoError(t, err)

	// Run remote bazel
	exitCode, err := remotebazel.HandleRemoteBazel([]string{
		fmt.Sprintf("--remote_runner=%s", bbServer.GRPCAddress()),
		// Have the ci runner use the "none" isolation type because it's simpler
		// to setup than a firecracker runner
		"--runner_exec_properties=workload-isolation-type=none",
		"--runner_exec_properties=container-image=",
		"run",
		":hello_world",
		"--noenable_bzlmod",
		fmt.Sprintf("--remote_header=x-buildbuddy-api-key=%s", env.APIKey1)})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	// Check the invocation logs to ensure the bazel command successfully ran
	bbClient := env.GetBuildBuddyServiceClient()
	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}
	searchRsp, err := bbClient.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
		RequestContext: reqCtx,
		Query:          &inpb.InvocationQuery{GroupId: env.GroupID1},
	})
	require.NoError(t, err)

	require.Equal(t, 2, len(searchRsp.GetInvocation()))
	// Find outer invocation because it will contain run output
	var inv *inpb.Invocation
	for _, i := range searchRsp.GetInvocation() {
		if i.GetRole() == "HOSTED_BAZEL" {
			inv = i
		}
	}
	invocationID := inv.InvocationId

	logResp, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
		InvocationId: invocationID,
		MinLines:     math.MaxInt32,
	})
	require.NoError(t, err)
	require.Contains(t, string(logResp.GetBuffer()), "Build completed successfully")
	require.Contains(t, string(logResp.GetBuffer()), "FUTURE OF BUILDS!")
}

func runLocalServerAndExecutor(t *testing.T, githubToken string) (*rbetest.Env, *rbetest.BuildBuddyServer, *rbetest.Executor) {
	env := rbetest.NewRBETestEnv(t)
	bbServer := env.AddBuildBuddyServerWithOptions(&rbetest.BuildBuddyServerOptions{
		EnvModifier: func(e *testenv.TestEnv) {
			e.SetRepoDownloader(repo_downloader.NewRepoDownloader())
			e.SetGitProviders([]interfaces.GitProvider{testgit.NewFakeProvider()})
			e.SetWorkflowService(service.NewWorkflowService(e))
			iss := invocation_search_service.NewInvocationSearchService(e, e.GetDBHandle(), e.GetOLAPDBHandle())
			e.SetInvocationSearchService(iss)
			e.SetGitHubApp(&testgit.FakeGitHubApp{Token: githubToken})
			runner, err := hostedrunner.New(e)
			require.NoError(t, err)
			e.SetRunnerService(runner)
			e.SetByteStreamClient(env.GetByteStreamClient())
			e.SetActionCacheClient(env.GetActionResultStorageClient())
			keyValStore, err := memory_kvstore.NewMemoryKeyValStore()
			require.NoError(t, err)
			e.SetKeyValStore(keyValStore)
			e.SetExecutionService(execution_service.NewExecutionService(e))
		},
	})

	executors := env.AddExecutors(t, 1)
	require.Equal(t, 1, len(executors))
	flags.Set(t, "executor.enable_bare_runner", true)

	return env, bbServer, executors[0]
}

func TestCancel(t *testing.T) {
	repoName := "private-test-repo"
	// If you need to re-generate this PAT, it should only have read access to
	// `private-test-repo`, and should be saved as a BB secret in all environments.
	username := "maggie-lou"
	personalAccessToken := os.Getenv("PRIVATE_TEST_REPO_GIT_ACCESS_TOKEN")
	repoURLWithToken := fmt.Sprintf("https://%s:%s@github.com/buildbuddy-io/private-test-repo.git", username, personalAccessToken)

	// Use a dir that is persisted on recycled runners
	rootDir := "/root/workspace/remote-bazel-integration-test"
	err := os.Setenv("HOME", rootDir)
	require.NoError(t, err)

	err = os.MkdirAll(rootDir, 0755)
	require.NoError(t, err)

	if _, err := os.Stat(fmt.Sprintf("%s/%s", rootDir, repoName)); os.IsNotExist(err) {
		output := testshell.Run(t, rootDir, fmt.Sprintf("git clone %s --filter=blob:none --depth=1", repoURLWithToken))
		require.NotContains(t, output, "fatal")
	}

	err = os.Chdir(fmt.Sprintf("%s/%s", rootDir, repoName))
	require.NoError(t, err)

	// Run a server and executor locally to run remote bazel against
	env, bbServer, _ := runLocalServerAndExecutor(t, personalAccessToken)
	ctx := env.WithUserID(context.Background(), env.UserID1)
	reqCtx := &ctxpb.RequestContext{
		UserId:  &uidpb.UserId{Id: env.UserID1},
		GroupId: env.GroupID1,
	}

	// Create a workflow for the same repo - will be used to fetch the git token
	dbh := env.GetDBHandle()
	require.NotNil(t, dbh)
	err = dbh.NewQuery(context.Background(), "create_git_repo_for_test").Create(&tables.GitRepository{
		RepoURL: "https://github.com/buildbuddy-io/private-test-repo",
		GroupID: env.GroupID1,
	})
	require.NoError(t, err)

	// Get an API key to authenticate the remote bazel request
	bbClient := env.GetBuildBuddyServiceClient()
	apiRsp, err := bbClient.CreateApiKey(ctx, &akpb.CreateApiKeyRequest{
		RequestContext: reqCtx,
		Capability: []akpb.ApiKey_Capability{
			akpb.ApiKey_CAS_WRITE_CAPABILITY,
			akpb.ApiKey_CACHE_WRITE_CAPABILITY,
			akpb.ApiKey_ORG_ADMIN_CAPABILITY,
		},
	})
	require.NoError(t, err)
	apiKey := apiRsp.ApiKey.Value

	// Before the remote runner has a chance to complete, cancel the run
	ctxWithCancel, cancel := context.WithCancel(ctx)
	go func() {
		waitForInvocationCreated(t, ctx, bbClient, reqCtx)
		cancel()
	}()

	err = remotebazel.RemoteFlagset.Parse([]string{"--runner_exec_properties=workload-isolation-type=none", "--runner_exec_properties=container-image="})
	require.NoError(t, err)
	wsFilePath, err := bazel.FindWorkspaceFile(".")
	require.NoError(t, err)
	repoConfig, err := remotebazel.Config(".")
	require.NoError(t, err)
	_, err = remotebazel.Run(
		ctxWithCancel,
		remotebazel.RunOpts{
			Server: bbServer.GRPCAddress(),
			APIKey: apiKey,
			Args: []string{
				"run",
				"//:sleep_forever_test",
			},
			WorkspaceFilePath: wsFilePath,
		}, repoConfig)
	require.Contains(t, err.Error(), "context canceled")

	// Check the invocation logs to make sure the invocation was canceled
	searchRsp, err := bbClient.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
		RequestContext: reqCtx,
		Query:          &inpb.InvocationQuery{GroupId: env.GroupID1},
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(searchRsp.GetInvocation()), 1)

	// Find outer invocation because the inner invocation will report a successful
	// status after the build has completed, and will not wait for the infinite
	// script to run
	var inv *inpb.Invocation
	for _, i := range searchRsp.GetInvocation() {
		if i.GetRole() == "HOSTED_BAZEL" {
			inv = i
		}
	}
	require.NotNil(t, inv)
	invocationID := inv.InvocationId

	waitForInvocationStatus(t, ctx, bbClient, reqCtx, invocationID, inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS)
}
