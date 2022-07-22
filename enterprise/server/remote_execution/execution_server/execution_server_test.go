package execution_server_test

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"

	"github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type schedulerServerMock struct {
	interfaces.SchedulerService

	canceledCount int
}

func (s *schedulerServerMock) CancelTask(ctx context.Context, taskID string) (bool, error) {
	s.canceledCount++
	return true, nil
}

func setupEnv(t *testing.T) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)

	redisTarget := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(redisTarget))
	env.SetRemoteExecutionRedisClient(rdb)
	env.SetRemoteExecutionRedisPubSubClient(rdb)

	scheduler := &schedulerServerMock{}
	env.SetSchedulerService(scheduler)

	s, err := execution_server.NewExecutionServer(env)
	require.NoError(t, err)
	env.SetRemoteExecutionService(s)

	return env
}

func createExecution(t *testing.T, db *gorm.DB, execution *tables.Execution) {
	err := db.Create(execution).Error
	require.NoError(t, err)
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
		Stage:        int64(remote_execution.ExecutionStage_EXECUTING),
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
		Stage:        int64(remote_execution.ExecutionStage_COMPLETED),
	}
	incompleteExecution := &tables.Execution{
		ExecutionID:  executionID2,
		InvocationID: testInvocationID,
		Stage:        int64(remote_execution.ExecutionStage_EXECUTING),
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
		Stage:        int64(remote_execution.ExecutionStage_COMPLETED),
	}
	incompleteExecution1 := &tables.Execution{
		ExecutionID:  executionID2,
		InvocationID: testInvocationID,
		Stage:        int64(remote_execution.ExecutionStage_EXECUTING),
	}
	incompleteExecution2 := &tables.Execution{
		ExecutionID:  executionID3,
		InvocationID: testInvocationID,
		Stage:        int64(remote_execution.ExecutionStage_EXECUTING),
	}
	createExecution(t, db, completeExecution)
	createExecution(t, db, incompleteExecution1)
	createExecution(t, db, incompleteExecution2)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 2, schedulerMock.canceledCount)
}
