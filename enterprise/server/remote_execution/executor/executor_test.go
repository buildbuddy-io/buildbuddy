package executor_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/longrunning"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

type mockExecutionServer struct {
	repb.UnimplementedExecutionServer
	operations []*longrunning.Operation
	finished   chan struct{}
}

func (s *mockExecutionServer) Execute(req *repb.ExecuteRequest, stream repb.Execution_ExecuteServer) error {
	return nil
}

func (s *mockExecutionServer) WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error {
	return nil
}

func (s *mockExecutionServer) PublishOperation(stream repb.Execution_PublishOperationServer) error {
	for {
		op, err := stream.Recv()
		if err != nil {
			return err
		}
		s.operations = append(s.operations, op)
		stage := operation.ExtractStage(op)
		if stage == repb.ExecutionStage_COMPLETED {
			close(s.finished)
		}
	}
}

type counters struct {
	countFinishedCleanly int
	countRecycled        int
}

func getExecutor(t *testing.T) (*executor.Executor, repb.ExecutionClient, *mockExecutionServer, *counters) {
	env := enterprise_testenv.New(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	clock := clockwork.NewFakeClock()
	env.SetClock(clock)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	mockServer := &mockExecutionServer{operations: make([]*longrunning.Operation, 0), finished: make(chan struct{})}
	repb.RegisterExecutionServer(env.GetGRPCServer(), mockServer)
	go runServer()

	c := &counters{}
	cacheRoot := testfs.MakeTempDir(t)
	runnerPool := rbetest.NewTestRunnerPool(t, env, cacheRoot, rbetest.TestRunnerOverrides{
		RunInterceptor:     rbetest.RunNoop(),
		DownloadInputsMock: rbetest.DownloadInputsNoop(),
		RecycleInterceptor: func(ctx context.Context, r interfaces.Runner, finishedCleanly bool, original rbetest.TryRecycleFunc) {
			// Simulate that recycling takes 1min.
			clock.Advance(1 * time.Minute)
			c.countRecycled++
			if finishedCleanly {
				c.countFinishedCleanly++
			}
		},
	})
	exec, err := executor.NewExecutor(env, "executor-id", "host-id", "hostname", runnerPool)
	require.NoError(t, err)

	conn, err := testenv.LocalGRPCConn(context.Background(), lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	client := repb.NewExecutionClient(conn)

	return exec, client, mockServer, c
}

func getTask() *repb.ScheduledTask {
	r := digest.NewCASResourceName(&repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 1}, "", repb.DigestFunction_SHA256)
	executionID := r.NewUploadString()
	return &repb.ScheduledTask{
		ExecutionTask: &repb.ExecutionTask{
			ExecutionId: executionID,
			ExecuteRequest: &repb.ExecuteRequest{
				ActionDigest: &repb.Digest{
					Hash:      strings.Repeat("b", 64),
					SizeBytes: 123,
				},
				InstanceName:   "",
				DigestFunction: repb.DigestFunction_SHA256,
			},
			Action: &repb.Action{},
			Command: &repb.Command{
				Arguments: []string{"echo", "hello"},
			},
		},
		SchedulingMetadata: &scpb.SchedulingMetadata{
			TaskSize: &scpb.TaskSize{
				EstimatedMemoryBytes: 100,
				EstimatedMilliCpu:    100,
			},
		},
	}
}

// TODO: If we don't want to charge customers on this, we can look at OutputUploadCompletedTimestamp
func TestExecuteTaskAndStreamResults(t *testing.T) {
	ctx := context.Background()
	exec, execClient, mockServer, mockCounter := getExecutor(t)
	task := getTask()

	publisher, err := operation.Publish(ctx, execClient, task.ExecutionTask.ExecutionId)
	require.NoError(t, err)

	retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
	require.NoError(t, err)
	require.False(t, retry)

	// Wait until all progress updates have finished sending.
	require.Eventually(t, func() bool {
		<-mockServer.finished
		return true
	}, 15*time.Second, 50*time.Millisecond)

	require.Equal(t, 1, mockCounter.countRecycled)
	require.Equal(t, 1, mockCounter.countFinishedCleanly)

	operationStageCount := make(map[repb.ExecutionStage_Value]int, len(mockServer.operations))
	for _, op := range mockServer.operations {
		stage := operation.ExtractStage(op)
		operationStageCount[stage]++
	}

	require.GreaterOrEqual(t, len(mockServer.operations), 3)
	require.GreaterOrEqual(t, operationStageCount[repb.ExecutionStage_EXECUTING], 1)
	require.Equal(t, 1, operationStageCount[repb.ExecutionStage_CLEANUP])
	require.Equal(t, 1, operationStageCount[repb.ExecutionStage_COMPLETED])

	cleanupOp := mockServer.operations[len(mockServer.operations)-2]
	require.Equal(t, repb.ExecutionStage_CLEANUP, operation.ExtractStage(cleanupOp))
	completedOp := mockServer.operations[len(mockServer.operations)-1]
	require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))

	// Verify that worker completed timestamp includes cleanup time.
	// (Cleanup is mocked out to take 1min in `getExecutor`).
	execResponse := operation.ExtractExecuteResponse(completedOp)
	metadata := execResponse.GetResult().GetExecutionMetadata()
	require.NotNil(t, metadata)
	workerDuration := metadata.WorkerCompletedTimestamp.AsTime().Sub(metadata.WorkerStartTimestamp.AsTime())
	require.True(t, workerDuration >= 1*time.Minute)
}
