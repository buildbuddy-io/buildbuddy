package execution_server_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

type schedulerServerMock struct {
	interfaces.SchedulerService

	canceledCount int
	scheduleReqs  []*scpb.ScheduleTaskRequest
}

func (s *schedulerServerMock) GetPoolInfo(context.Context, string, string, string, interfaces.PoolType) (*interfaces.PoolInfo, error) {
	return &interfaces.PoolInfo{}, nil
}

func (s *schedulerServerMock) ScheduleTask(ctx context.Context, req *scpb.ScheduleTaskRequest) (*scpb.ScheduleTaskResponse, error) {
	s.scheduleReqs = append(s.scheduleReqs, req)
	return &scpb.ScheduleTaskResponse{}, nil
}

func (s *schedulerServerMock) CancelTask(ctx context.Context, taskID string) (bool, error) {
	s.canceledCount++
	return true, nil
}

func setupEnv(t *testing.T) (*testenv.TestEnv, *grpc.ClientConn) {
	env := testenv.GetTestEnv(t)

	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	redisTarget := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(redisTarget))
	env.SetRemoteExecutionRedisClient(rdb)
	env.SetRemoteExecutionRedisPubSubClient(rdb)

	scheduler := &schedulerServerMock{}
	env.SetSchedulerService(scheduler)

	tasksize.Register(env)

	s, err := execution_server.NewExecutionServer(env)
	require.NoError(t, err)
	env.SetRemoteExecutionService(s)

	_, run, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	repb.RegisterExecutionServer(env.GetGRPCServer(), env.GetRemoteExecutionService())
	go run()

	conn, err := testenv.LocalGRPCConn(env.GetServerContext(), lis)
	require.NoError(t, err)
	return env, conn
}

func createExecution(ctx context.Context, t *testing.T, db interfaces.DB, execution *tables.Execution) {
	err := db.NewQuery(ctx, "create_execution").Create(execution)
	require.NoError(t, err)
}

func getExecutions(t *testing.T, env environment.Env) []*tables.Execution {
	rq := env.GetDBHandle().NewQuery(env.GetServerContext(), "get_executions").Raw(`SELECT * FROM Executions`)
	out, err := db.ScanAll(rq, &tables.Execution{})
	require.NoError(t, err)
	return out
}

func TestDispatch(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})
	ctx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	arn := uploadEmptyAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256)
	ad := arn.GetDigest()

	// note: AttachUserPrefix is normally done by Execute(), which wraps
	// Dispatch().
	ctx, err = prefix.AttachUserPrefixToContext(ctx, env)
	require.NoError(t, err)
	taskID, err := s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: ad})
	require.NoError(t, err)

	rn, err := digest.ParseUploadResourceName(taskID)
	require.NoError(t, err)
	assert.Equal(t, "", rn.GetInstanceName(), "instance name mismatch")
	assert.Equal(t, ad.GetHash(), rn.GetDigest().GetHash(), "action hash mismatch")

	rows := getExecutions(t, env)
	require.Equal(t, 1, len(rows))
	assert.Equal(t, taskID, rows[0].ExecutionID)
	assert.Equal(t, "US1", rows[0].UserID)

	sched := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, len(sched.scheduleReqs))
	b := sched.scheduleReqs[0].SerializedTask
	task := &repb.ExecutionTask{}
	err = proto.Unmarshal(b, task)
	require.NoError(t, err)
	assert.Nil(t, task.GetRequestMetadata().GetToolDetails(), "ToolDetails should be nil")
	assert.Equal(t, iid, task.GetRequestMetadata().GetToolInvocationId(), "invocation ID should be passed along")
}

func TestCancel(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	// Create Execution rows to be canceled
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	executionID := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	execution := &tables.Execution{
		ExecutionID:  executionID,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), execution)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, schedulerMock.canceledCount)
}

func TestCancel_SkipCompletedExecution(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	// Create Execution rows to be canceled
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	executionID1 := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	executionID2 := "blobs/2111111111111111111111111111111111111111111111111111111111111111/100"
	completeExecution := &tables.Execution{
		ExecutionID:  executionID1,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_COMPLETED),
	}
	incompleteExecution := &tables.Execution{
		ExecutionID:  executionID2,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), completeExecution)
	createExecution(ctx, t, env.GetDBHandle(), incompleteExecution)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, schedulerMock.canceledCount)
}

func TestCancel_MultipleExecutions(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	// Create Execution rows to be canceled
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	executionID1 := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	executionID2 := "blobs/2111111111111111111111111111111111111111111111111111111111111111/100"
	executionID3 := "blobs/3111111111111111111111111111111111111111111111111111111111111111/100"
	completeExecution := &tables.Execution{
		ExecutionID:  executionID1,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_COMPLETED),
	}
	incompleteExecution1 := &tables.Execution{
		ExecutionID:  executionID2,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	incompleteExecution2 := &tables.Execution{
		ExecutionID:  executionID3,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), completeExecution)
	createExecution(ctx, t, env.GetDBHandle(), incompleteExecution1)
	createExecution(ctx, t, env.GetDBHandle(), incompleteExecution2)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 2, schedulerMock.canceledCount)
}

func TestExecuteAndPublishOperation(t *testing.T) {
	ctx := context.Background()
	env, conn := setupEnv(t)
	client := repb.NewExecutionClient(conn)

	const instanceName = "test-instance"
	const invocationID = "93383cc1-5d6c-4ad1-a321-8ee87c2f6816"
	const digestFunction = repb.DigestFunction_SHA256

	// Schedule execution
	arn := uploadEmptyAction(ctx, t, env, instanceName, digestFunction)
	executionClient, err := client.Execute(ctx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   arn.GetDigest(),
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)
	err = executionClient.CloseSend()
	require.NoError(t, err)
	// Wait for execution to be accepted by the server. This also gives us
	// the task ID.
	op, err := executionClient.Recv()
	require.NoError(t, err)
	taskID := op.GetName()

	// Simulate execution: set up a PublishOperation stream and publish an
	// ExecuteResponse to it.
	ctx, err = bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: invocationID,
	})
	require.NoError(t, err)
	stream, err := client.PublishOperation(ctx)
	require.NoError(t, err)
	queuedTime := time.Unix(100, 0)
	actionResult := &repb.ActionResult{
		ExitCode:  42,
		StderrRaw: []byte("test-stderr"),
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			QueuedTimestamp: tspb.New(queuedTime),
		},
	}
	op, err = operation.Assemble(
		repb.ExecutionStage_COMPLETED, taskID, arn,
		operation.ExecuteResponseWithResult(actionResult, nil),
	)
	require.NoError(t, err)
	err = stream.Send(op)
	require.NoError(t, err)
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	// Wait for the execute response to be streamed back on our initial
	// /Execute stream.
	expectedExecuteResponse := &repb.ExecuteResponse{
		Result: actionResult,
	}
	var executeResponse *repb.ExecuteResponse
	for {
		op, err = executionClient.Recv()
		if err == io.EOF {
			require.NotNil(t, executeResponse, "expected execute response, got EOF")
			break
		}
		require.NoError(t, err)
		if stage := operation.ExtractStage(op); stage != repb.ExecutionStage_COMPLETED {
			continue
		}
		executeResponse = operation.ExtractExecuteResponse(op)
	}
	assert.Empty(t, cmp.Diff(expectedExecuteResponse, executeResponse, protocmp.Transform()))

	// Should also be able to fetch the ExecuteResponse from cache. See field
	// comment on Execution.exeute_response_digest for notes on serialization
	// format.
	cachedExecuteResponse, err := execution.GetCachedExecuteResponse(ctx, env, taskID)
	require.NoError(t, err)
	assert.Empty(t, cmp.Diff(expectedExecuteResponse, cachedExecuteResponse, protocmp.Transform()))
}

func TestMarkFailed(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	// Create Execution rows to be canceled
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	executionID := "test-instance-name/uploads/1797f326-0cd2-45d2-9ad4-f766fd81f2dc/blobs/1111111111111111111111111111111111111111111111111111111111111111/100"

	completeExecution := &tables.Execution{
		ExecutionID:  executionID,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), completeExecution)

	ex := &tables.Execution{}
	err = env.GetDBHandle().GORM(ctx, "select_for_test_mark_failed").Model(ex).Where("execution_id = ?", executionID).Take(ex).Error
	require.NoError(t, err)

	require.Equal(t, int64(repb.ExecutionStage_EXECUTING), ex.Stage)

	err = s.MarkExecutionFailed(ctx, executionID, status.InternalError("It didn't work"))
	require.NoError(t, err)

	// Should exist in DB after marking failed
	ex = &tables.Execution{}
	err = env.GetDBHandle().GORM(ctx, "select_for_test_mark_failed").Model(ex).Where("execution_id = ?", executionID).Take(ex).Error
	require.NoError(t, err)
	assert.Equal(t, executionID, ex.ExecutionID)

	// ExecuteResponse should be cached after marking failed
	executeResponse, err := execution.GetCachedExecuteResponse(ctx, env, executionID)
	require.NoError(t, err)
	assert.Equal(t, "It didn't work", executeResponse.GetStatus().GetMessage())

	err = s.MarkExecutionFailed(ctx, executionID, status.InternalError("It didn't work"))
	require.NoError(t, err)

	require.Equal(t, int64(repb.ExecutionStage_COMPLETED), ex.Stage)

	err = s.MarkExecutionFailed(ctx, "blobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/1", status.InternalError("It didn't work"))
	require.True(t, status.IsNotFoundError(err), "error should be NotFoundError, but was %s", err)

}

func uploadEmptyAction(ctx context.Context, t *testing.T, env *real_environment.RealEnv, instanceName string, df repb.DigestFunction_Value) *digest.ResourceName {
	cmd := &repb.Command{Arguments: []string{"test"}}
	cd, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, df, cmd)
	require.NoError(t, err)
	action := &repb.Action{CommandDigest: cd}
	ad, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, df, action)
	require.NoError(t, err)
	return digest.NewResourceName(ad, instanceName, rspb.CacheType_CAS, df)
}

func withIncomingMetadata(t *testing.T, ctx context.Context, rmd *repb.RequestMetadata) context.Context {
	b, err := proto.Marshal(rmd)
	require.NoError(t, err)
	return metadata.NewIncomingContext(ctx, metadata.Pairs(bazel_request.RequestMetadataKey, string(b)))
}
