package execution_server

import (
	"context"
	"fmt"
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

	scheduler, err := scheduler_server.NewSchedulerServerWithOptions(env, &scheduler_server.Options{
		EnableRedisAvailabilityMonitoring: true,
		LocalPortOverride:                 80,
	})
	if err != nil {
		t.Fatal(err)
	}
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

func createExecutionRedis(t *testing.T, redisClient redis.UniversalClient, executionID string) {
	key := fmt.Sprintf("task/{%s}", executionID)
	mockData := map[string]interface{}{
		"key1": "value1",
	}
	numAdded, err := redisClient.HSet(context.Background(), key, mockData).Result()
	if err != nil {
		t.Fatal(err)
	} else if numAdded == 0 {
		t.Fatal("Could not add any rows to redis")
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

	createExecutionRedis(t, s.rdb, executionID1)
	createExecutionRedis(t, s.rdb, executionID2)
	createExecutionRedis(t, s.rdb, executionID3)

	numCanceled, err := s.Cancel(ctx, testInvocationID)
	assert.Nil(t, err)
	assert.Equal(t, 2, numCanceled)
}
