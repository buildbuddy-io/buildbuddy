package execution_server_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

type schedulerServerMock struct {
	interfaces.SchedulerService

	canceledCount int
	scheduleReqs  []*scpb.ScheduleTaskRequest
}

func (s *schedulerServerMock) GetPoolInfo(context.Context, string, string, string, bool) (*interfaces.PoolInfo, error) {
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

func setupEnv(t *testing.T) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)

	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	redisTarget := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(redisTarget))
	env.SetRemoteExecutionRedisClient(rdb)
	env.SetRemoteExecutionRedisPubSubClient(rdb)

	scheduler := &schedulerServerMock{}
	env.SetSchedulerService(scheduler)

	testcache.Setup(t, env)
	tasksize.Register(env)

	s, err := execution_server.NewExecutionServer(env)
	require.NoError(t, err)
	env.SetRemoteExecutionService(s)

	return env
}

func createExecution(t *testing.T, db *gorm.DB, execution *tables.Execution) {
	err := db.Create(execution).Error
	require.NoError(t, err)
}

func getExecutions(t *testing.T, env environment.Env) []*tables.Execution {
	db := env.GetDBHandle().DB(env.GetServerContext())
	rows, err := db.Raw(`SELECT * FROM Executions`).Rows()
	require.NoError(t, err)
	var out []*tables.Execution
	for rows.Next() {
		row := &tables.Execution{}
		err := db.ScanRows(rows, row)
		require.NoError(t, err)
		out = append(out, row)
	}
	return out
}

func TestDispatch(t *testing.T) {
	env := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})
	ctx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	cmd := &repb.Command{Arguments: []string{"test"}}
	cd, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), "", repb.DigestFunction_SHA256, cmd)
	require.NoError(t, err)
	action := &repb.Action{CommandDigest: cd}
	ad, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), "", repb.DigestFunction_SHA256, action)
	require.NoError(t, err)

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
	env := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	// Create Execution rows to be canceled
	db := env.GetDBHandle().DB(ctx)
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	executionID := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	execution := &tables.Execution{
		ExecutionID:  executionID,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(t, db, execution)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, schedulerMock.canceledCount)
}

func TestCancel_SkipCompletedExecution(t *testing.T) {
	env := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	// Create Execution rows to be canceled
	db := env.GetDBHandle().DB(ctx)
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
	createExecution(t, db, completeExecution)
	createExecution(t, db, incompleteExecution)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, schedulerMock.canceledCount)
}

func TestCancel_MultipleExecutions(t *testing.T) {
	env := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	// Create Execution rows to be canceled
	db := env.GetDBHandle().DB(ctx)
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
	createExecution(t, db, completeExecution)
	createExecution(t, db, incompleteExecution1)
	createExecution(t, db, incompleteExecution2)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 2, schedulerMock.canceledCount)
}

func withIncomingMetadata(t *testing.T, ctx context.Context, rmd *repb.RequestMetadata) context.Context {
	b, err := proto.Marshal(rmd)
	require.NoError(t, err)
	return metadata.NewIncomingContext(ctx, metadata.Pairs(bazel_request.RequestMetadataKey, string(b)))
}
