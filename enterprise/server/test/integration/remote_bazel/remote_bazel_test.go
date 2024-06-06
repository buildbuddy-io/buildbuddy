package remote_bazel_test

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

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
		GroupId:        env.GroupID1,
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
		"build",
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
	// We expect 1 invocation for the ci_runner, and 1 invocation for the :hello_world
	// build. Either should contain the log line we're looking for
	require.Equal(t, 2, len(searchRsp.GetInvocation()))
	invocationID := searchRsp.Invocation[0].InvocationId

	logResp, err := bbClient.GetEventLogChunk(ctx, &elpb.GetEventLogChunkRequest{
		InvocationId: invocationID,
		MinLines:     math.MaxInt32,
	})
	require.NoError(t, err)
	require.Contains(t, string(logResp.GetBuffer()), "Build completed successfully")
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
		},
	})
	grpcAddress := fmt.Sprintf("grpc://localhost:%d", bbServer.GRPCPort())
	u, err := url.Parse(grpcAddress)
	require.NoError(t, err)
	flags.Set(t, "app.events_api_url", *u)
	flags.Set(t, "app.cache_api_url", *u)

	executors := env.AddExecutors(t, 1)
	require.Equal(t, 1, len(executors))
	flags.Set(t, "executor.enable_bare_runner", true)
	return env, bbServer, executors[0]
}
