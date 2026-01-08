package executor_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gstatus "google.golang.org/grpc/status"
)

type mockExecutionServer struct {
	repb.UnimplementedExecutionServer
	operations []*longrunningpb.Operation
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
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
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

			operationStageCount := make(map[repb.ExecutionStage_Value]int, len(mockServer.operations))
			for _, op := range mockServer.operations {
				stage := operation.ExtractStage(op)
				operationStageCount[stage]++
			}

			require.GreaterOrEqual(t, len(mockServer.operations), 3)
			require.GreaterOrEqual(t, operationStageCount[repb.ExecutionStage_EXECUTING], 1)
			require.Equal(t, 1, operationStageCount[repb.ExecutionStage_COMPLETED])

			completedOp := mockServer.operations[len(mockServer.operations)-1]
			require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))
		})
	}
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

	operationStageCount := make(map[repb.ExecutionStage_Value]int, len(mockServer.operations))
	for _, op := range mockServer.operations {
		stage := operation.ExtractStage(op)
		operationStageCount[stage]++
	}

	require.GreaterOrEqual(t, len(mockServer.operations), 1)
	completedOp := mockServer.operations[len(mockServer.operations)-1]
	require.Equal(t, repb.ExecutionStage_COMPLETED, operation.ExtractStage(completedOp))
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
	require.ErrorContains(t, err, "timed out waiting for Read response")
	require.False(t, retry, "bazel will retry Unavailable errors, so we should not retry internally")

	<-mockServer.finished
	// We should still recycle the runner if we timed out downloading inputs -
	// this is a recoverable error.
	require.Equal(t, 1, mockCounter.countRecycled)
	operationStageCount := make(map[repb.ExecutionStage_Value]int, len(mockServer.operations))
	for _, op := range mockServer.operations {
		stage := operation.ExtractStage(op)
		operationStageCount[stage]++
	}
	require.GreaterOrEqual(t, len(mockServer.operations), 1)
	completedOp := mockServer.operations[len(mockServer.operations)-1]
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
			operationStageCount := make(map[repb.ExecutionStage_Value]int, len(mockServer.operations))
			for _, op := range mockServer.operations {
				stage := operation.ExtractStage(op)
				operationStageCount[stage]++
			}
			require.GreaterOrEqual(t, len(mockServer.operations), 1)
			completedOp := mockServer.operations[len(mockServer.operations)-1]
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
