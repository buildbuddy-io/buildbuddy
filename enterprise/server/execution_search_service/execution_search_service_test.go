package execution_search_service_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_search_service"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	ispb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

func makeExecutionID(actionDigest *repb.Digest) string {
	return digest.NewCASResourceName(actionDigest, "buildbuddy-io/buildbuddy", repb.DigestFunction_SHA256).NewUploadString()
}

func TestSearchExecutions(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)

	iid1 := uuid.New()
	iid2 := uuid.New()

	actionDigest1 := &repb.Digest{Hash: "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", SizeBytes: 142}
	actionDigest2 := &repb.Digest{Hash: "fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9", SizeBytes: 256}
	actionDigest3 := &repb.Digest{Hash: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", SizeBytes: 0}

	exid1 := makeExecutionID(actionDigest1)
	exid2 := makeExecutionID(actionDigest2)
	exid3 := makeExecutionID(actionDigest3)

	testTimestampUsec := time.Now().UnixMicro()

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)

	executions := []*olaptables.Execution{
		{
			ExecutionID:                        exid1,
			GroupID:                            "GR1",
			InvocationUUID:                     strings.ReplaceAll(iid1, "-", ""),
			User:                               "ci-runner",
			Host:                               "linux-build-01.internal",
			Command:                            "build",
			Pattern:                            "//server/...",
			RepoURL:                            "https://github.com/buildbuddy-io/buildbuddy",
			BranchName:                         "main",
			CommitSHA:                          "abc123def456789",
			Role:                               "CI",
			Worker:                             "executor-linux-pool-abc123",
			ActionMnemonic:                     "GoCompile",
			TargetLabel:                        "//server/util/status:status",
			CommandSnippet:                     "/usr/local/go/bin/go compile -o bazel-out/...",
			Stage:                              int64(repb.ExecutionStage_COMPLETED),
			StatusCode:                         0,
			ExitCode:                           0,
			Success:                            true,
			InvocationStatus:                   int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			QueuedTimestampUsec:                testTimestampUsec - 5000000,
			WorkerStartTimestampUsec:           testTimestampUsec - 4000000,
			InputFetchStartTimestampUsec:       testTimestampUsec - 3500000,
			InputFetchCompletedTimestampUsec:   testTimestampUsec - 3000000,
			ExecutionStartTimestampUsec:        testTimestampUsec - 3000000,
			ExecutionCompletedTimestampUsec:    testTimestampUsec - 1000000,
			OutputUploadStartTimestampUsec:     testTimestampUsec - 1000000,
			OutputUploadCompletedTimestampUsec: testTimestampUsec - 500000,
			WorkerCompletedTimestampUsec:       testTimestampUsec - 500000,
			FileDownloadCount:                  15,
			FileDownloadSizeBytes:              1024 * 1024,
			FileDownloadDurationUsec:           500000,
			FileUploadCount:                    3,
			FileUploadSizeBytes:                512 * 1024,
			FileUploadDurationUsec:             250000,
			PeakMemoryBytes:                    256 * 1024 * 1024,
			EstimatedMemoryBytes:               512 * 1024 * 1024,
			EstimatedMilliCPU:                  1000,
			CreatedAtUsec:                      testTimestampUsec,
			UpdatedAtUsec:                      testTimestampUsec,
		},
		{
			ExecutionID:                     exid2,
			GroupID:                         "GR1",
			InvocationUUID:                  strings.ReplaceAll(iid2, "-", ""),
			User:                            "developer",
			Host:                            "dev-workstation.local",
			Command:                         "test",
			Pattern:                         "//server/util/status:status_test",
			RepoURL:                         "https://github.com/buildbuddy-io/buildbuddy",
			BranchName:                      "feature/new-feature",
			CommitSHA:                       "def789abc012345",
			Role:                            "CI",
			Worker:                          "executor-linux-pool-def456",
			ActionMnemonic:                  "GoTest",
			TargetLabel:                     "//server/util/status:status_test",
			CommandSnippet:                  "/usr/local/go/bin/go test -test.v ...",
			Stage:                           int64(repb.ExecutionStage_COMPLETED),
			StatusCode:                      0,
			ExitCode:                        0,
			Success:                         true,
			InvocationStatus:                int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			QueuedTimestampUsec:             testTimestampUsec + 1000000 - 5000000,
			WorkerStartTimestampUsec:        testTimestampUsec + 1000000 - 4000000,
			ExecutionStartTimestampUsec:     testTimestampUsec + 1000000 - 3000000,
			ExecutionCompletedTimestampUsec: testTimestampUsec + 1000000 - 1000000,
			WorkerCompletedTimestampUsec:    testTimestampUsec + 1000000 - 500000,
			CreatedAtUsec:                   testTimestampUsec + 1000,
			UpdatedAtUsec:                   testTimestampUsec + 1000,
		},
		{
			ExecutionID:      exid3,
			GroupID:          "GR2",
			InvocationUUID:   strings.ReplaceAll(uuid.New(), "-", ""),
			User:             "other-user",
			Host:             "other-host.internal",
			Command:          "build",
			RepoURL:          "https://github.com/other-org/other-repo",
			Worker:           "executor-pool-xyz789",
			ActionMnemonic:   "CppCompile",
			TargetLabel:      "//src:main",
			InvocationStatus: int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			CreatedAtUsec:    testTimestampUsec + 2000,
			UpdatedAtUsec:    testTimestampUsec + 2000,
		},
	}
	for _, execution := range executions {
		err := env.GetOLAPDBHandle().GORM(ctx, "test_create_execution").Create(execution).Error
		require.NoError(t, err)
	}

	service := execution_search_service.NewExecutionSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	testCtx, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	rsp, err := service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{})
	require.NoError(t, err)
	assert.Len(t, rsp.Execution, 2, "should return 2 executions for GR1")

	executionIDs := make([]string, len(rsp.Execution))
	for i, ex := range rsp.Execution {
		executionIDs[i] = ex.Execution.ExecutionId
	}
	assert.Contains(t, executionIDs, exid1)
	assert.Contains(t, executionIDs, exid2)
	assert.NotContains(t, executionIDs, exid3, "should not contain execution from GR2")

	for _, ex := range rsp.Execution {
		assert.NotEmpty(t, ex.InvocationMetadata.Id)
		assert.NotEmpty(t, ex.Execution.ExecutionId)
		assert.NotEmpty(t, ex.Execution.ActionDigest)
	}

	rsp, err = service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{
		Query: &espb.ExecutionQuery{
			InvocationUser: "ci-runner",
		},
	})
	require.NoError(t, err)
	assert.Len(t, rsp.Execution, 1)
	assert.Equal(t, exid1, rsp.Execution[0].Execution.ExecutionId)

	rsp, err = service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{
		Query: &espb.ExecutionQuery{
			Command: "test",
		},
	})
	require.NoError(t, err)
	assert.Len(t, rsp.Execution, 1)
	assert.Equal(t, exid2, rsp.Execution[0].Execution.ExecutionId)

	rsp, err = service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{
		Query: &espb.ExecutionQuery{
			BranchName: "main",
		},
	})
	require.NoError(t, err)
	assert.Len(t, rsp.Execution, 1)
	assert.Equal(t, exid1, rsp.Execution[0].Execution.ExecutionId)

	rsp, err = service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{
		Query: &espb.ExecutionQuery{
			RepoUrl: "https://github.com/buildbuddy-io/buildbuddy",
		},
	})
	require.NoError(t, err)
	assert.Len(t, rsp.Execution, 2)
}

func TestSearchExecutions_SkipsEmptyInvocationUUID(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)

	iid1 := uuid.New()

	actionDigest1 := &repb.Digest{Hash: "a948904f2f0f479b8f8564cbf12dac6b5c8c3c1f7e8b4d6a3c2e1f0a9b8c7d6e", SizeBytes: 512}
	actionDigest2 := &repb.Digest{Hash: "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c", SizeBytes: 1024}

	exid1 := makeExecutionID(actionDigest1)
	exid2 := makeExecutionID(actionDigest2)

	testTimestampUsec := time.Now().UnixMicro()

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	env.SetAuthenticator(ta)

	executions := []*olaptables.Execution{
		{
			ExecutionID:      exid1,
			GroupID:          "GR1",
			InvocationUUID:   strings.ReplaceAll(iid1, "-", ""),
			User:             "ci-runner",
			Host:             "build-host-01",
			Command:          "build",
			RepoURL:          "https://github.com/buildbuddy-io/buildbuddy",
			Worker:           "executor-pool-abc123",
			ActionMnemonic:   "GoCompile",
			TargetLabel:      "//server/cmd/server:server",
			Stage:            int64(repb.ExecutionStage_COMPLETED),
			InvocationStatus: int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			CreatedAtUsec:    testTimestampUsec,
			UpdatedAtUsec:    testTimestampUsec,
		},
		{
			ExecutionID:      exid2,
			GroupID:          "GR1",
			InvocationUUID:   "",
			User:             "unknown",
			Host:             "unknown-host",
			Command:          "build",
			Worker:           "executor-pool-orphan",
			ActionMnemonic:   "CppCompile",
			TargetLabel:      "//src:orphan_target",
			Stage:            int64(repb.ExecutionStage_COMPLETED),
			InvocationStatus: int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			CreatedAtUsec:    testTimestampUsec + 1000,
			UpdatedAtUsec:    testTimestampUsec + 1000,
		},
	}
	for _, execution := range executions {
		err := env.GetOLAPDBHandle().GORM(ctx, "test_create_execution").Create(execution).Error
		require.NoError(t, err)
	}

	service := execution_search_service.NewExecutionSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	testCtx, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	rsp, err := service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{})
	require.NoError(t, err)
	require.Len(t, rsp.Execution, 1)
	assert.Equal(t, exid1, rsp.Execution[0].Execution.ExecutionId)
	assert.Equal(t, "GoCompile", rsp.Execution[0].Execution.ActionMnemonic)
	assert.Equal(t, "//server/cmd/server:server", rsp.Execution[0].Execution.TargetLabel)
}

func TestSearchExecutions_Pagination(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)

	testTimestampUsec := time.Now().UnixMicro()

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	env.SetAuthenticator(ta)

	executions := make([]*olaptables.Execution, 20)
	for i := 0; i < 20; i++ {
		hash := fmt.Sprintf("a948904f2f0f479b8f8564cbf12dac6b5c8c3c1f7e8b4d6a3c2e1f0a9b8c7d%02x", i)
		actionDigest := &repb.Digest{Hash: hash, SizeBytes: int64(100 + i*10)}
		executions[i] = &olaptables.Execution{
			ExecutionID:      makeExecutionID(actionDigest),
			GroupID:          "GR1",
			InvocationUUID:   strings.ReplaceAll(uuid.New(), "-", ""),
			User:             "ci-runner",
			Host:             fmt.Sprintf("build-host-%02d", i%5),
			Command:          "build",
			RepoURL:          "https://github.com/buildbuddy-io/buildbuddy",
			BranchName:       "main",
			Worker:           fmt.Sprintf("executor-pool-%d", i%3),
			ActionMnemonic:   []string{"GoCompile", "GoLink", "GoTest", "CppCompile", "Javac"}[i%5],
			TargetLabel:      fmt.Sprintf("//server/pkg%d:target", i),
			Stage:            int64(repb.ExecutionStage_COMPLETED),
			InvocationStatus: int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			CreatedAtUsec:    testTimestampUsec + int64(i*1000),
			UpdatedAtUsec:    testTimestampUsec + int64(i*1000),
		}
	}
	for _, execution := range executions {
		err := env.GetOLAPDBHandle().GORM(ctx, "test_create_execution").Create(execution).Error
		require.NoError(t, err)
	}

	service := execution_search_service.NewExecutionSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	testCtx, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	rsp, err := service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{})
	require.NoError(t, err)
	assert.Len(t, rsp.Execution, 15)
	assert.NotEmpty(t, rsp.NextPageToken)

	rsp2, err := service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{
		PageToken: rsp.NextPageToken,
	})
	require.NoError(t, err)
	assert.Len(t, rsp2.Execution, 5)
	assert.Empty(t, rsp2.NextPageToken)

	rsp3, err := service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{
		Count: 5,
	})
	require.NoError(t, err)
	assert.Len(t, rsp3.Execution, 5)
	assert.NotEmpty(t, rsp3.NextPageToken)
}

func TestSearchExecutions_PaginationWithEmptyInvocationUUIDs(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)

	testTimestampUsec := time.Now().UnixMicro()

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	env.SetAuthenticator(ta)

	executions := make([]*olaptables.Execution, 20)
	for i := 0; i < 20; i++ {
		hash := fmt.Sprintf("b948904f2f0f479b8f8564cbf12dac6b5c8c3c1f7e8b4d6a3c2e1f0a9b8c7d%02x", i)
		actionDigest := &repb.Digest{Hash: hash, SizeBytes: int64(100 + i*10)}
		invocationUUID := strings.ReplaceAll(uuid.New(), "-", "")
		if i%4 == 0 {
			invocationUUID = ""
		}
		executions[i] = &olaptables.Execution{
			ExecutionID:      makeExecutionID(actionDigest),
			GroupID:          "GR1",
			InvocationUUID:   invocationUUID,
			User:             "ci-runner",
			Host:             fmt.Sprintf("build-host-%02d", i%5),
			Command:          "build",
			RepoURL:          "https://github.com/buildbuddy-io/buildbuddy",
			BranchName:       "main",
			Worker:           fmt.Sprintf("executor-pool-%d", i%3),
			ActionMnemonic:   []string{"GoCompile", "GoLink", "GoTest", "CppCompile", "Javac"}[i%5],
			TargetLabel:      fmt.Sprintf("//server/pkg%d:target", i),
			Stage:            int64(repb.ExecutionStage_COMPLETED),
			InvocationStatus: int64(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			CreatedAtUsec:    testTimestampUsec + int64(i*1000),
			UpdatedAtUsec:    testTimestampUsec + int64(i*1000),
		}
	}
	for _, execution := range executions {
		err := env.GetOLAPDBHandle().GORM(ctx, "test_create_execution").Create(execution).Error
		require.NoError(t, err)
	}

	service := execution_search_service.NewExecutionSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	testCtx, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	var allExecutions []*espb.ExecutionWithInvocationMetadata
	pageToken := ""
	for {
		rsp, err := service.SearchExecutions(testCtx, &espb.SearchExecutionRequest{
			Count:     10,
			PageToken: pageToken,
		})
		require.NoError(t, err)
		for _, ex := range rsp.Execution {
			require.NotNil(t, ex)
			require.NotEmpty(t, ex.InvocationMetadata.Id)
		}
		allExecutions = append(allExecutions, rsp.Execution...)
		if rsp.NextPageToken == "" {
			break
		}
		pageToken = rsp.NextPageToken
	}

	require.Len(t, allExecutions, 15)
}

func TestSearchExecutions_RequiresAuth(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	env.SetAuthenticator(ta)

	service := execution_search_service.NewExecutionSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	_, err := service.SearchExecutions(ctx, &espb.SearchExecutionRequest{})
	require.Error(t, err, "should require authentication")
}
