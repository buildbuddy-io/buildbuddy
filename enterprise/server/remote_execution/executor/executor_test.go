package executor_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/oom"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gstatus "google.golang.org/grpc/status"
)

type mockExecutionServer struct {
	repb.UnimplementedExecutionServer
	mu              sync.Mutex
	operations      []*longrunningpb.Operation
	finished        chan struct{}
	completedSignal sync.Once
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
		s.mu.Lock()
		s.operations = append(s.operations, op)
		s.mu.Unlock()
		stage := operation.ExtractStage(op)
		if stage == repb.ExecutionStage_COMPLETED {
			s.completedSignal.Do(func() { close(s.finished) })
		}
	}
}

// getOperations returns a snapshot of received operations under the mutex.
// Callers should use this rather than reading s.operations directly to avoid
// races with the gRPC handler goroutine that appends to the slice.
func (s *mockExecutionServer) getOperations() []*longrunningpb.Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.operations)
}

func operationsStageCounts(ops []*longrunningpb.Operation) map[repb.ExecutionStage_Value]int {
	stages := make(map[repb.ExecutionStage_Value]int)
	for _, op := range ops {
		stages[operation.ExtractStage(op)]++
	}
	return stages
}

type mockPublisher struct {
	pingFailure bool
	sendFailure bool
}

func (p *mockPublisher) Context() context.Context {
	return context.Background()
}

func (p *mockPublisher) Send(op *longrunningpb.Operation) error {
	if p.sendFailure {
		return status.InternalError("uh oh")
	}
	return nil
}

func (p *mockPublisher) Ping() error {
	if p.pingFailure {
		return status.InternalError("uh oh")
	}
	return nil
}

func (p *mockPublisher) SetState(state repb.ExecutionProgress_ExecutionState) error {
	return nil
}

func (p *mockPublisher) CloseAndRecv() (*repb.PublishOperationResponse, error) {
	return &repb.PublishOperationResponse{}, nil
}

type counters struct {
	countFinishedCleanly int
	countRecycled        int
}

func getExecutor(t *testing.T, runOverride rbetest.RunInterceptor) (*executor.Executor, *testenv.TestEnv, repb.ExecutionClient, *mockExecutionServer, *counters) {
	env := enterprise_testenv.New(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1")))
	clock := clockwork.NewFakeClock()
	env.SetClock(clock)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	mockServer := &mockExecutionServer{finished: make(chan struct{})}
	repb.RegisterExecutionServer(env.GetGRPCServer(), mockServer)
	go runServer()

	c := &counters{}
	cacheRoot := testfs.MakeTempDir(t)
	runFunc := rbetest.RunNoop()
	if runOverride != nil {
		runFunc = runOverride
	}
	runnerPool := rbetest.NewTestRunnerPool(t, env, cacheRoot, rbetest.TestRunnerOverrides{
		RunInterceptor: runFunc,
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

	return exec, env, client, mockServer, c
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
			Action: &repb.Action{
				InputRootDigest: &repb.Digest{Hash: digest.EmptySha256},
			},
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

func TestExecuteTaskAndStreamResults(t *testing.T) {
	for _, tc := range []struct {
		name                string
		runOverride         rbetest.RunInterceptor
		expectFinishCleanly bool

		expectInputFetchMetadata *espb.InputFetchMetadata
	}{
		{
			name:                "Success",
			expectFinishCleanly: true,
		},
		{
			name:                "Run failure",
			runOverride:         rbetest.AlwaysReturn(commandutil.ErrorResult(errors.New("run failed"))),
			expectFinishCleanly: false,
		},
		{
			name: "Auxiliary metadata",
			runOverride: rbetest.AlwaysReturn(&interfaces.CommandResult{
				InputFetchMetadata: &espb.InputFetchMetadata{
					DownloadedFileIndicesBitmap: []byte{1, 2, 3},
				},
			}),
			expectInputFetchMetadata: &espb.InputFetchMetadata{
				DownloadedFileIndicesBitmap: []byte{1, 2, 3},
			},
			expectFinishCleanly: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			exec, _, execClient, mockServer, mockCounter := getExecutor(t, tc.runOverride)
			task := getTask()

			publisher, err := operation.Publish(ctx, execClient, task.ExecutionTask.ExecutionId)
			require.NoError(t, err)

			retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
			require.NoError(t, err)
			require.False(t, retry)

			// Wait until all progress updates have finished sending.
			<-mockServer.finished

			require.Equal(t, 1, mockCounter.countRecycled)
			if tc.expectFinishCleanly {
				require.Equal(t, 1, mockCounter.countFinishedCleanly)
			} else {
				require.Equal(t, 0, mockCounter.countFinishedCleanly)
			}

			ops := mockServer.getOperations()
			operationStageCount := operationsStageCounts(ops)

			require.GreaterOrEqual(t, len(ops), 3)
			require.GreaterOrEqual(t, operationStageCount[repb.ExecutionStage_EXECUTING], 1)
			require.Equal(t, 1, operationStageCount[repb.ExecutionStage_COMPLETED])

			completedOp := ops[len(ops)-1]
			require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))

			unpackedCompletedOp, err := rexec.UnpackOperation(completedOp)
			require.NoError(t, err)
			rsp := unpackedCompletedOp.ExecuteResponse

			if tc.expectInputFetchMetadata != nil {
				auxMeta := &espb.ExecutionAuxiliaryMetadata{}
				ok, err := rexec.FindFirstAuxiliaryMetadata(rsp.GetResult().GetExecutionMetadata(), auxMeta)
				require.True(t, ok)
				require.NoError(t, err)
				require.Empty(t, cmp.Diff(
					tc.expectInputFetchMetadata,
					auxMeta.GetInputFetchDetailedStats(),
					protocmp.Transform(),
				))
			}
		})
	}
}

func TestExecuteTaskAndStreamResults_PreservesOOMError(t *testing.T) {
	ctx := t.Context()
	task := getTask()
	observedMemoryBytes := int64(800)
	usageMemoryBytes := int64(100)
	// Set up a fake executor where all runners return oom.Error
	runOverride := rbetest.AlwaysReturn(&interfaces.CommandResult{
		ExitCode: commandutil.NoExitCode,
		Error: oom.Error(oom.Details{
			EstimatedMemoryBytes: task.GetSchedulingMetadata().GetTaskSize().GetEstimatedMemoryBytes(),
			ObservedMemoryBytes:  observedMemoryBytes,
		}),
		DoNotRecycle: true,
		UsageStats: &repb.UsageStats{
			MemoryBytes: usageMemoryBytes,
		},
	})
	exec, _, execClient, mockServer, mockCounter := getExecutor(t, runOverride)
	publisher, err := operation.Publish(ctx, execClient, task.GetExecutionTask().GetExecutionId())
	require.NoError(t, err)

	// Execute the scheduled task through the executor so the runner result is
	// normalized and published as an ExecuteResponse.
	retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
	require.NoError(t, err)
	require.False(t, retry)

	// The executor should preserve the OOM status details, pass
	// finishedCleanly=false to the recycle hook, and leave the command usage
	// stats untouched.
	<-mockServer.finished
	require.Equal(t, 1, mockCounter.countRecycled)
	require.Equal(t, 0, mockCounter.countFinishedCleanly)
	ops := mockServer.getOperations()
	completedOp := ops[len(ops)-1]
	require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))

	unpackedCompletedOp, err := rexec.UnpackOperation(completedOp)
	require.NoError(t, err)
	rsp := unpackedCompletedOp.ExecuteResponse
	require.Equal(t, int32(codes.Unavailable), rsp.GetStatus().GetCode())
	require.Equal(t, int32(commandutil.NoExitCode), rsp.GetResult().GetExitCode())
	require.Equal(t, usageMemoryBytes, rsp.GetResult().GetExecutionMetadata().GetUsageStats().GetMemoryBytes())
	details, ok := oom.DetailsFromStatusProto(rsp.GetStatus())
	require.True(t, ok)
	require.Equal(t, task.GetSchedulingMetadata().GetTaskSize().GetEstimatedMemoryBytes(), details.EstimatedMemoryBytes)
	require.Equal(t, observedMemoryBytes, details.ObservedMemoryBytes)
}

func TestExecuteTaskAndStreamResults_PreservesOOMErrorAfterGracefulTermination(t *testing.T) {
	ctx := t.Context()
	task := getTask()
	task.GetExecutionTask().GetAction().Timeout = durationpb.New(50 * time.Millisecond)
	task.GetExecutionTask().GetCommand().Platform = &repb.Platform{
		Properties: []*repb.Platform_Property{
			{Name: platform.TerminationGracePeriodPropertyName, Value: time.Second.String()},
		},
	}
	observedMemoryBytes := int64(800)
	usageMemoryBytes := int64(100)
	// Set up a fake executor where the runner returns oom.Error after the task
	// timeout has triggered graceful termination.
	runOverride := func(ctx context.Context, original rbetest.RunFunc) *interfaces.CommandResult {
		time.Sleep(200 * time.Millisecond)
		return &interfaces.CommandResult{
			ExitCode: commandutil.NoExitCode,
			Error: oom.Error(oom.Details{
				EstimatedMemoryBytes: task.GetSchedulingMetadata().GetTaskSize().GetEstimatedMemoryBytes(),
				ObservedMemoryBytes:  observedMemoryBytes,
			}),
			DoNotRecycle: true,
			UsageStats: &repb.UsageStats{
				MemoryBytes: usageMemoryBytes,
			},
		}
	}
	exec, _, execClient, mockServer, mockCounter := getExecutor(t, runOverride)
	publisher, err := operation.Publish(ctx, execClient, task.GetExecutionTask().GetExecutionId())
	require.NoError(t, err)

	// Execute the scheduled task through the timeout path where
	// gracefullyTerminated is closed before the runner returns.
	retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
	require.NoError(t, err)
	require.False(t, retry)

	// The graceful termination rewrite should not replace the more specific OOM
	// error with DeadlineExceeded.
	<-mockServer.finished
	require.Equal(t, 1, mockCounter.countRecycled)
	require.Equal(t, 0, mockCounter.countFinishedCleanly)
	ops := mockServer.getOperations()
	completedOp := ops[len(ops)-1]
	require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))

	unpackedCompletedOp, err := rexec.UnpackOperation(completedOp)
	require.NoError(t, err)
	rsp := unpackedCompletedOp.ExecuteResponse
	require.Equal(t, int32(codes.Unavailable), rsp.GetStatus().GetCode())
	require.Equal(t, int32(commandutil.NoExitCode), rsp.GetResult().GetExitCode())
	require.Equal(t, usageMemoryBytes, rsp.GetResult().GetExecutionMetadata().GetUsageStats().GetMemoryBytes())
	details, ok := oom.DetailsFromStatusProto(rsp.GetStatus())
	require.True(t, ok)
	require.Equal(t, task.GetSchedulingMetadata().GetTaskSize().GetEstimatedMemoryBytes(), details.EstimatedMemoryBytes)
	require.Equal(t, observedMemoryBytes, details.ObservedMemoryBytes)
}

func TestExecuteTaskAndStreamResults_CacheHit(t *testing.T) {
	ctx := context.Background()
	exec, env, execClient, mockServer, mockCounter := getExecutor(t, nil)
	task := getTask()

	// Store an existing result in the cache.
	actionDigest := task.ExecutionTask.ExecuteRequest.ActionDigest
	instanceName := task.ExecutionTask.ExecuteRequest.InstanceName
	digestFunction := task.ExecutionTask.ExecuteRequest.DigestFunction
	acResourceName := digest.NewACResourceName(actionDigest, instanceName, digestFunction)
	actionResult := &repb.ActionResult{
		ExitCode:  0,
		StdoutRaw: []byte("cached result"),
	}
	err := cachetools.UploadActionResult(ctx, env.GetActionCacheClient(), acResourceName, actionResult)
	require.NoError(t, err)

	publisher, err := operation.Publish(ctx, execClient, task.ExecutionTask.ExecutionId)
	require.NoError(t, err)

	retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
	require.NoError(t, err)
	require.False(t, retry)

	// Wait until all progress updates have finished sending.
	<-mockServer.finished

	// No runner should've been created.
	require.Equal(t, 0, mockCounter.countRecycled)

	ops := mockServer.getOperations()

	require.GreaterOrEqual(t, len(ops), 1)
	completedOp := ops[len(ops)-1]
	require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))
}

// TestExecuteTaskAndStreamResults_PostCompletionStats verifies that the
// executor sends a follow-up stage=COMPLETED Operation carrying
// PostCompletionStats after TryRecycle runs, when the runner exposes such
// stats and the task opts in via the publish_post_completion_stats
// experiment.
func TestExecuteTaskAndStreamResults_PostCompletionStats(t *testing.T) {
	for _, tc := range []struct {
		name                string
		postCompletionStats *espb.PostCompletionStats
		experimentEnabled   bool
		expectFollowUp      bool
	}{
		{
			name: "WithStats",
			postCompletionStats: &espb.PostCompletionStats{
				PauseDurationUsec: 67890,
				FirecrackerPostExecStats: &espb.FirecrackerPostExecStats{
					SnapshotSavedLocally:  true,
					SnapshotSavedRemotely: true,
					SnapshotIsDiff:        true,
					SnapshotSavedBytes:    12345,
				},
			},
			experimentEnabled: true,
			expectFollowUp:    true,
		},
		{
			// Empty stats from the runner (e.g. non-firecracker containers
			// without any container-specific stats to report) should still
			// produce a follow-up when the experiment is enabled.
			name:                "EmptyStats",
			postCompletionStats: &espb.PostCompletionStats{},
			experimentEnabled:   true,
			expectFollowUp:      true,
		},
		{
			name: "ExperimentOff",
			postCompletionStats: &espb.PostCompletionStats{
				PauseDurationUsec: 67890,
				FirecrackerPostExecStats: &espb.FirecrackerPostExecStats{
					SnapshotSavedLocally: true,
					SnapshotSavedBytes:   12345,
				},
			},
			experimentEnabled: false,
			expectFollowUp:    false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			env := enterprise_testenv.New(t)
			env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1")))
			clock := clockwork.NewFakeClock()
			env.SetClock(clock)

			_, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
			testcache.Setup(t, env, lis)
			mockServer := &mockExecutionServer{finished: make(chan struct{})}
			repb.RegisterExecutionServer(env.GetGRPCServer(), mockServer)
			go runServer()

			cacheRoot := testfs.MakeTempDir(t)
			runnerPool := rbetest.NewTestRunnerPool(t, env, cacheRoot, rbetest.TestRunnerOverrides{
				RunInterceptor:      rbetest.RunNoop(),
				PostCompletionStats: tc.postCompletionStats,
			})
			exec, err := executor.NewExecutor(env, "executor-id", "host-id", "hostname", runnerPool)
			require.NoError(t, err)

			conn, err := testenv.LocalGRPCConn(context.Background(), lis)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			execClient := repb.NewExecutionClient(conn)

			task := getTask()
			if tc.experimentEnabled {
				task.ExecutionTask.Experiments = []string{"remote_execution.publish_post_completion_stats"}
			}
			publisher, err := operation.Publish(ctx, execClient, task.ExecutionTask.ExecutionId)
			require.NoError(t, err)

			retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
			require.NoError(t, err)
			require.False(t, retry)
			<-mockServer.finished

			completedOps := func() []*longrunningpb.Operation {
				ops := []*longrunningpb.Operation{}
				for _, op := range mockServer.getOperations() {
					if operation.ExtractStage(op) == repb.ExecutionStage_COMPLETED {
						ops = append(ops, op)
					}
				}
				return ops
			}

			expectedCount := 1
			if tc.expectFollowUp {
				expectedCount = 2
			}
			require.Eventually(t, func() bool {
				return len(completedOps()) >= expectedCount
			}, 5*time.Second, 10*time.Millisecond, "expected %d COMPLETED ops", expectedCount)
			// Give a brief moment to verify no additional ops sneak in.
			time.Sleep(50 * time.Millisecond)

			ops := completedOps()
			require.Equal(t, expectedCount, len(ops))
			if !tc.expectFollowUp {
				return
			}

			followUp, err := rexec.UnpackOperation(ops[1])
			require.NoError(t, err)
			gotStats := &espb.PostCompletionStats{}
			ok, err := rexec.FindFirstAuxiliaryMetadata(followUp.ExecuteResponse.GetResult().GetExecutionMetadata(), gotStats)
			require.NoError(t, err)
			require.True(t, ok, "follow-up COMPLETED should carry PostCompletionStats")
			require.Empty(t, cmp.Diff(tc.postCompletionStats, gotStats, protocmp.Transform()))
		})
	}
}

func TestExecuteTaskAndStreamResults_PublishFailures(t *testing.T) {
	for _, tc := range []struct {
		name         string
		sendFailure  bool
		pingFaillure bool
	}{
		{
			name:        "Send failure",
			sendFailure: true,
		},
		{
			name:         "Ping failure",
			pingFaillure: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "executor.task_progress_publish_interval", 1*time.Millisecond)

			ctx := context.Background()
			runOverride := func(ctx context.Context, original rbetest.RunFunc) *interfaces.CommandResult {
				// Block the run call to give the executor time to ping the publisher
				time.Sleep(5 * time.Minute)
				return &interfaces.CommandResult{}
			}
			exec, _, _, _, mockCounter := getExecutor(t, runOverride)
			task := getTask()

			publisher := &mockPublisher{sendFailure: tc.sendFailure, pingFailure: tc.pingFaillure}

			retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
			require.Error(t, err)
			require.True(t, retry)

			if !tc.sendFailure {
				// Runner should still be recycled.
				// Skip if there's a send failure, because we exit early
				// before creating the runner
				require.Equal(t, 1, mockCounter.countRecycled)
			}
		})
	}
}

func TestExecuteTaskAndStreamResults_InternalInputDownloadTimeout(t *testing.T) {
	ctx := context.Background()
	exec, env, execClient, mockServer, mockCounter := getExecutor(t, nil)
	task := getTask()
	// Prepare an input root with a large input that requires a bytestream
	// download.
	inputRoot := testfs.MakeTempDir(t)
	err := os.WriteFile(filepath.Join(inputRoot, "input.txt"), make([]byte, 128*1024*1024), 0644)
	require.NoError(t, err)
	ird, _, err := cachetools.UploadDirectoryToCAS(ctx, env, "", repb.DigestFunction_SHA256, inputRoot)
	require.NoError(t, err)
	task.ExecutionTask.Action.InputRootDigest = ird

	// Set CAS RPC timeout so low that it's guaranteed to timeout.
	flags.Set(t, "cache.client.cas_rpc_timeout", 1*time.Nanosecond)

	publisher, err := operation.Publish(ctx, execClient, task.ExecutionTask.ExecutionId)
	require.NoError(t, err)
	retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
	require.True(t, status.IsUnavailableError(err), "expected Unavailable error, got: %v", err)
	require.True(
		t,
		strings.Contains(err.Error(), "timed out waiting for Read response") || strings.Contains(err.Error(), "context deadline exceeded"),
		"expected read timeout error, got: %v",
		err,
	)
	require.False(t, retry, "bazel will retry Unavailable errors, so we should not retry internally")

	<-mockServer.finished
	// We should still recycle the runner if we timed out downloading inputs -
	// this is a recoverable error.
	require.Equal(t, 1, mockCounter.countRecycled)
	ops := mockServer.getOperations()
	require.GreaterOrEqual(t, len(ops), 1)
	completedOp := ops[len(ops)-1]
	require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))
	// We should still report the input fetch completed timestamp if fetching
	// inputs fails.
	rsp, err := rexec.UnpackOperation(completedOp)
	require.NoError(t, err)
	actionResult := rsp.ExecuteResponse.GetResult()
	inputFetchStart := actionResult.GetExecutionMetadata().GetInputFetchStartTimestamp().AsTime()
	inputFetchCompleted := actionResult.GetExecutionMetadata().GetInputFetchCompletedTimestamp().AsTime()
	require.GreaterOrEqual(t, inputFetchCompleted, inputFetchStart)
}

func TestExecuteTaskAndStreamResults_MissingInput(t *testing.T) {
	missingInputRoot := &repb.Directory{
		// Set some arbitrary properties to ensure the Directory proto is not
		// empty, since empty digests do not require consulting the cache.
		NodeProperties: &repb.NodeProperties{Mtime: timestamppb.New(time.Now())},
	}
	uploadedInputRootWithMissingSmallFile := &repb.Directory{
		Files: []*repb.FileNode{
			{
				Name: "small_file.txt",
				Digest: &repb.Digest{
					Hash:      "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
					SizeBytes: 3,
				},
			},
		},
	}
	uploadedInputRootWithMissingLargeFile := &repb.Directory{
		Files: []*repb.FileNode{
			{
				Name: "large_file.txt",
				Digest: &repb.Digest{
					Hash:      "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
					SizeBytes: 128 * 1024 * 1024,
				},
			},
		},
	}

	for _, tc := range []struct {
		name      string
		inputRoot *repb.Directory
	}{
		{
			name:      "missing input root",
			inputRoot: missingInputRoot,
		},
		{
			name:      "missing small file",
			inputRoot: uploadedInputRootWithMissingSmallFile,
		},
		{
			name:      "missing large file",
			inputRoot: uploadedInputRootWithMissingLargeFile,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			exec, env, execClient, mockServer, mockCounter := getExecutor(t, nil)
			task := getTask()

			// Upload the two "uploadedInputRoot" protos to the CAS, but not
			// their referenced files.
			_, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), "", repb.DigestFunction_SHA256, uploadedInputRootWithMissingLargeFile)
			require.NoError(t, err)
			_, err = cachetools.UploadProto(ctx, env.GetByteStreamClient(), "", repb.DigestFunction_SHA256, uploadedInputRootWithMissingSmallFile)
			require.NoError(t, err)

			// Set the input root digest to a missing digest.
			ird, err := digest.ComputeForMessage(tc.inputRoot, repb.DigestFunction_SHA256)
			require.NoError(t, err)
			task.ExecutionTask.Action.InputRootDigest = ird
			publisher, err := operation.Publish(ctx, execClient, task.ExecutionTask.ExecutionId)
			require.NoError(t, err)
			retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
			require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition error, got: %v", err)
			s, ok := gstatus.FromError(err)
			require.True(t, ok, "expected a status error, got: %v", err)
			// Expecting a "MISSING" violation.
			details := s.Details()
			require.Equal(t, 1, len(details))
			pf, ok := details[0].(*errdetails.PreconditionFailure)
			require.True(t, ok, "expected a PreconditionFailure, got: %v", details[0])
			require.NotEmpty(t, pf.Violations)
			for _, v := range pf.Violations {
				require.Equal(t, "MISSING", v.Type)
				require.Regexp(t, "blobs/(blake3/)?[0-9a-f]+/[0-9]+", v.Subject)
			}
			require.False(t, retry, "bazel will retry MissingDigest errors, so we should not retry internally")

			<-mockServer.finished
			// We should still recycle the runner if inputs are missing.
			require.Equal(t, 1, mockCounter.countRecycled)
			ops := mockServer.getOperations()
			require.GreaterOrEqual(t, len(ops), 1)
			completedOp := ops[len(ops)-1]
			require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))
			// We should still report the input fetch completed timestamp if fetching
			// inputs fails.
			rsp, err := rexec.UnpackOperation(completedOp)
			require.NoError(t, err)
			actionResult := rsp.ExecuteResponse.GetResult()
			inputFetchStart := actionResult.GetExecutionMetadata().GetInputFetchStartTimestamp().AsTime()
			inputFetchCompleted := actionResult.GetExecutionMetadata().GetInputFetchCompletedTimestamp().AsTime()
			require.GreaterOrEqual(t, inputFetchCompleted, inputFetchStart)
		})
	}
}

func TestExecuteTaskAndStreamResults_Timeout(t *testing.T) {
	tests := []struct {
		name            string
		actionTimeout   *durationpb.Duration
		platformTimeout *durationpb.Duration
		expectedTimeout time.Duration
	}{
		{name: "set action timeout", actionTimeout: durationpb.New(10 * time.Minute), platformTimeout: nil, expectedTimeout: 10 * time.Minute},
		{name: "set timeout platform property", actionTimeout: nil, platformTimeout: durationpb.New(1 * time.Hour), expectedTimeout: 1 * time.Hour},
		// The timeout on the action should take precedence over the platform property.
		// Timeout logic for remote runners (ci_runner_util.go) assumes this holds true.
		{name: "set both action and platform property timeout", actionTimeout: durationpb.New(10 * time.Minute), platformTimeout: durationpb.New(1 * time.Hour), expectedTimeout: 10 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			exec, _, execClient, mockServer, _ := getExecutor(t, nil)
			task := getTask()

			task.ExecutionTask.Action.Timeout = tc.actionTimeout
			var props []*repb.Platform_Property
			if tc.platformTimeout != nil {
				props = append(props, &repb.Platform_Property{
					Name:  platform.DefaultTimeoutPropertyName,
					Value: tc.platformTimeout.AsDuration().String(),
				})
			}
			task.ExecutionTask.Action.Platform = &repb.Platform{Properties: props}

			publisher, err := operation.Publish(ctx, execClient, task.ExecutionTask.ExecutionId)
			require.NoError(t, err)

			retry, err := exec.ExecuteTaskAndStreamResults(ctx, task, publisher)
			require.NoError(t, err)
			require.False(t, retry)

			<-mockServer.finished
			ops := mockServer.getOperations()
			completedOp := ops[len(ops)-1]
			require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))

			rsp, err := rexec.UnpackOperation(completedOp)
			require.NoError(t, err)
			auxMeta := &espb.ExecutionAuxiliaryMetadata{}
			ok, err := rexec.FindFirstAuxiliaryMetadata(rsp.ExecuteResponse.GetResult().GetExecutionMetadata(), auxMeta)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.expectedTimeout, auxMeta.GetTimeout().AsDuration())
		})
	}
}
