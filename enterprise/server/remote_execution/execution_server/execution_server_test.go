package execution_server

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/task_router"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"

	"github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func executionServer(t *testing.T) *ExecutionServer {
	env := testenv.GetTestEnv(t)

	redisTarget := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(redisTarget))
	env.SetRemoteExecutionRedisClient(rdb)
	env.SetRemoteExecutionRedisPubSubClient(rdb)

	router, err := task_router.New(env)
	if err != nil {
		t.Fatal(err)
	}
	env.SetTaskRouter(router)

	scheduler := scheduler_server.NewMockSchedulerServer()
	env.SetSchedulerService(scheduler)

	s, err := NewExecutionServer(env)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func createExecution(t *testing.T, db *gorm.DB, execution *tables.Execution) {
	err := db.Create(execution).Error
	if err != nil {
		t.Fatal(err)
	}
}

func TestCancel(t *testing.T) {
	ctx := context.Background()
	s := executionServer(t)

	// Create Execution rows to be canceled
	db := s.env.GetDBHandle().DB(ctx)
	testUUID, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	testInvocationID := testUUID.String()

	executionID := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	execution := &tables.Execution{
		ExecutionID:  executionID,
		InvocationID: testInvocationID,
		Stage:        int64(remote_execution.ExecutionStage_EXECUTING),
	}
	createExecution(t, db, execution)

	numCanceled, err := s.Cancel(ctx, testInvocationID)
	assert.Nil(t, err)
	assert.Equal(t, 1, numCanceled)
}

func TestCancel_SkipCompletedExecution(t *testing.T) {
	ctx := context.Background()
	s := executionServer(t)

	// Create Execution rows to be canceled
	db := s.env.GetDBHandle().DB(ctx)
	testUUID, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
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

	numCanceled, err := s.Cancel(ctx, testInvocationID)
	assert.Nil(t, err)
	assert.Equal(t, 1, numCanceled)
}

func TestCancel_MultipleExecutions(t *testing.T) {
	ctx := context.Background()
	s := executionServer(t)

	// Create Execution rows to be canceled
	db := s.env.GetDBHandle().DB(ctx)
	testUUID, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
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

	numCanceled, err := s.Cancel(ctx, testInvocationID)
	assert.Nil(t, err)
	assert.Equal(t, 2, numCanceled)
}
