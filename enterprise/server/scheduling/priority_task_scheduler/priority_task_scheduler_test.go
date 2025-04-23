package priority_task_scheduler

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

const (
	testGroupID1 = "group1"
	testGroupID2 = "group2"
	testGroupID3 = "group3"
)

func newTaskReservationRequest(taskID, taskGroupID string, priority int32) *scpb.EnqueueTaskReservationRequest {
	return &scpb.EnqueueTaskReservationRequest{
		TaskId: taskID,
		SchedulingMetadata: &scpb.SchedulingMetadata{
			TaskGroupId: taskGroupID,
			Priority:    priority,
		},
	}
}

func TestTaskQueue_SingleGroup(t *testing.T) {
	q := newTaskQueue()
	require.Equal(t, 0, q.Len())
	require.Nil(t, q.Peek())

	q.Enqueue(newTaskReservationRequest("1", testGroupID1, 0))
	require.Equal(t, 1, q.Len())

	// Peeking should return the reservation but not remove it.
	req := q.Peek()
	require.Equal(t, "1", req.GetTaskId())
	require.Equal(t, 1, q.Len())

	// Dequeueing should return the reservation and remove it.
	req = q.Dequeue()
	require.Equal(t, "1", req.GetTaskId())
	require.Equal(t, 0, q.Len())

	// Queue should be empty.
	require.Equal(t, 0, q.Len())
	require.Nil(t, q.Peek())

	q.Enqueue(newTaskReservationRequest("2", testGroupID1, 0))
	q.Enqueue(newTaskReservationRequest("3", testGroupID1, 0))
	q.Enqueue(newTaskReservationRequest("4", testGroupID1, 0))
	// Enqueue task "1" last but give it the highest priority so it gets
	// dequeued first.
	q.Enqueue(newTaskReservationRequest("1", testGroupID1, -1000))

	require.Equal(t, "1", q.Dequeue().GetTaskId())
	require.Equal(t, "2", q.Dequeue().GetTaskId())
	require.Equal(t, "3", q.Dequeue().GetTaskId())
	require.Equal(t, "4", q.Dequeue().GetTaskId())
}

func TestTaskQueue_MultipleGroups(t *testing.T) {
	q := newTaskQueue()

	// First group has 3 task reservations.
	q.Enqueue(newTaskReservationRequest("group1Task1", testGroupID1, 0))
	q.Enqueue(newTaskReservationRequest("group1Task2", testGroupID1, 0))
	q.Enqueue(newTaskReservationRequest("group1Task3", testGroupID1, 0))
	// Second group has 1 task reservation.
	q.Enqueue(newTaskReservationRequest("group2Task1", testGroupID2, 0))
	// Third group has 2 task reservations.
	// group3Task1 is enqueued last, but has higher priority so it should be
	// dequeued first.
	q.Enqueue(newTaskReservationRequest("group3Task2", testGroupID3, 0))
	q.Enqueue(newTaskReservationRequest("group3Task1", testGroupID3, -1000))

	require.Equal(t, "group1Task1", q.Dequeue().GetTaskId())
	require.Equal(t, "group2Task1", q.Dequeue().GetTaskId())
	require.Equal(t, "group3Task1", q.Dequeue().GetTaskId())
	require.Equal(t, "group1Task2", q.Dequeue().GetTaskId())
	require.Equal(t, "group3Task2", q.Dequeue().GetTaskId())
	require.Equal(t, "group1Task3", q.Dequeue().GetTaskId())
	require.Nil(t, q.Dequeue())
}

func TestTaskQueue_DedupesTasks(t *testing.T) {
	q := newTaskQueue()

	require.True(t, q.Enqueue(newTaskReservationRequest("1", testGroupID1, 0)))
	require.False(t, q.Enqueue(newTaskReservationRequest("1", testGroupID1, 0)))

	require.Equal(t, 1, q.Len())
	require.Equal(t, "1", q.Dequeue().GetTaskId())
	require.Equal(t, 0, q.Len())
	require.Nil(t, q.Dequeue())
}

func TestPriorityTaskScheduler_CustomResourcesDontPreventNormalTaskScheduling(t *testing.T) {
	env := testenv.GetTestEnv(t)
	env.SetRemoteExecutionClient(&FakeExecutionClient{})

	flags.Set(t, "executor.millicpu", 30_000)
	flags.Set(t, "executor.memory_bytes", 64_000_000_000)
	flags.Set(t, "executor.custom_resources", []resources.CustomResource{
		{Name: "gpu", Value: 1.0},
	})
	err := resources.Configure(false /*=mmapLRUEnabled*/)
	require.NoError(t, err)

	executor := NewFakeExecutor()
	runnerPool := &FakeRunnerPool{}
	leaser := &FakeTaskLeaser{}

	scheduler := NewPriorityTaskScheduler(env, executor, runnerPool, leaser, &Options{})
	scheduler.Start()
	t.Cleanup(func() {
		err := scheduler.Stop()
		require.NoError(t, err)
	})

	ctx := context.Background()

	oneCPU := &scpb.TaskSize{
		EstimatedMilliCpu:    1000,
		EstimatedMemoryBytes: 1000,
	}
	oneCPUAndOneGPU := &scpb.TaskSize{
		EstimatedMilliCpu:    1000,
		EstimatedMemoryBytes: 1000,
		CustomResources:      []*scpb.CustomResource{{Name: "gpu", Value: 1.0}},
	}

	// Schedule 2 GPU tasks, where the second task should block waiting for the
	// first task to complete. Then schedule a CPU-only task which should be
	// allowed to "skip ahead" in the queue since it doesn't require any custom
	// resources.
	gpuTask1ID := fakeTaskID("gpu-task-1")
	gpuTask2ID := fakeTaskID("gpu-task-2")
	cpuTask1ID := fakeTaskID("cpu-task-1")

	_, err = scheduler.EnqueueTaskReservation(ctx, &scpb.EnqueueTaskReservationRequest{
		TaskId:             gpuTask1ID,
		TaskSize:           oneCPUAndOneGPU,
		SchedulingMetadata: &scpb.SchedulingMetadata{TaskSize: oneCPUAndOneGPU},
	})
	require.NoError(t, err)
	_, err = scheduler.EnqueueTaskReservation(ctx, &scpb.EnqueueTaskReservationRequest{
		TaskId:             gpuTask2ID,
		TaskSize:           oneCPUAndOneGPU,
		SchedulingMetadata: &scpb.SchedulingMetadata{TaskSize: oneCPUAndOneGPU},
	})
	require.NoError(t, err)
	_, err = scheduler.EnqueueTaskReservation(ctx, &scpb.EnqueueTaskReservationRequest{
		TaskId:             cpuTask1ID,
		TaskSize:           oneCPU,
		SchedulingMetadata: &scpb.SchedulingMetadata{TaskSize: oneCPU},
	})
	require.NoError(t, err)

	execution1 := <-executor.StartedExecutions
	execution2 := <-executor.StartedExecutions

	require.Equal(t, scheduler.q.Len(), 1)

	// Tasks are enqueued synchronously but executed asynchronously, so we
	// assert that both of our expected tasks were started, but in no particular
	// order.
	startedTaskIDs := []string{
		execution1.ScheduledTask.GetExecutionTask().GetExecutionId(),
		execution2.ScheduledTask.GetExecutionTask().GetExecutionId(),
	}
	require.ElementsMatch(t, []string{gpuTask1ID, cpuTask1ID}, startedTaskIDs)

	// Finish the GPU task that is currently running, which should allow the
	// next GPU task to be scheduled.
	var gpuExecution *FakeExecution
	if execution1.ScheduledTask.GetExecutionTask().GetExecutionId() == gpuTask1ID {
		gpuExecution = execution1
	} else {
		gpuExecution = execution2
	}
	gpuExecution.Complete()

	execution3 := <-executor.StartedExecutions
	require.Equal(t, gpuTask2ID, execution3.ScheduledTask.GetExecutionTask().GetExecutionId())
	require.Equal(t, scheduler.q.Len(), 0)
}

func TestPriorityTaskScheduler_ExecutionErrorHandling(t *testing.T) {
	for _, test := range []struct {
		name string

		executionErr          error
		executionErrRetryable bool
		streamCloseAndRecvErr error

		expectedLeaseCloseRetry bool
		expectedLeaseCloseErr   error
	}{
		{
			name:                    "should close lease with non-retryable execution error if operation update succeeds",
			executionErr:            status.UnavailableError("execution error injected by test"),
			executionErrRetryable:   false,
			streamCloseAndRecvErr:   nil,
			expectedLeaseCloseRetry: false,
			expectedLeaseCloseErr:   status.UnavailableError("execution error injected by test"),
		},
		{
			name:                    "should retry task even on non-retryable execution error if operation update fails",
			executionErr:            status.UnavailableError("execution error injected by test"),
			executionErrRetryable:   false,
			streamCloseAndRecvErr:   status.UnavailableError("operation update error injected by test"),
			expectedLeaseCloseRetry: true,
			expectedLeaseCloseErr:   status.UnavailableError("finalize execution update stream: operation update error injected by test"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			executionClient := &FakeExecutionClient{}
			env.SetRemoteExecutionClient(executionClient)
			executor := NewFakeExecutor()
			runnerPool := &FakeRunnerPool{}
			leaser := &FakeTaskLeaser{}
			scheduler := NewPriorityTaskScheduler(env, executor, runnerPool, leaser, &Options{})
			scheduler.Start()
			t.Cleanup(func() {
				err := scheduler.Stop()
				require.NoError(t, err)
			})
			ctx := context.Background()

			// Set up the fake execution client to inject a failure into CloseAndRecv.
			executionClient.CloseAndRecvErr = test.streamCloseAndRecvErr

			// Start a task.
			reservation := &scpb.EnqueueTaskReservationRequest{TaskId: fakeTaskID("task1")}
			_, err := scheduler.EnqueueTaskReservation(ctx, reservation)
			require.NoError(t, err)

			// Fail the task with an error that is normally non-retryable, but because
			// the operation update failed, we have to retry because otherwise the
			// client won't be notified of the failure.
			task := <-executor.StartedExecutions
			task.CompleteWith(test.executionErrRetryable, test.executionErr)

			// Inspect the lease and make sure it was closed with a retry.
			lease := <-leaser.GrantedLeases
			retry, err := lease.WaitClosed()
			require.Equal(t, test.expectedLeaseCloseErr, err, "unexpected lease close error")
			require.Equal(t, test.expectedLeaseCloseRetry, retry, "unexpected lease retry value")
		})
	}
}

func fakeTaskID(label string) string {
	return label + "/uploads/" + uuid.New() + "/blobs/2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae/3"
}

type FakeExecutor struct {
	StartedExecutions chan *FakeExecution
}

func NewFakeExecutor() *FakeExecutor {
	return &FakeExecutor{
		StartedExecutions: make(chan *FakeExecution, 100),
	}
}

type FakeExecution struct {
	ScheduledTask *repb.ScheduledTask
	retry         bool
	err           error
	completeCh    chan struct{}
}

func (e *FakeExecution) Complete() {
	close(e.completeCh)
}

func (e *FakeExecution) CompleteWith(retry bool, err error) {
	e.retry = retry
	e.err = err
	close(e.completeCh)
}

func (e *FakeExecutor) ID() string {
	return "fake-executor-id"
}

func (e *FakeExecutor) HostID() string {
	return "fake-host-id"
}

func (e *FakeExecutor) ExecuteTaskAndStreamResults(ctx context.Context, st *repb.ScheduledTask, stream *operation.Publisher) (retry bool, err error) {
	log.Debugf("FakeExecutor: starting task %q", st.GetExecutionTask().GetExecutionId())
	fe := &FakeExecution{
		ScheduledTask: st,
		completeCh:    make(chan struct{}),
	}
	e.StartedExecutions <- fe
	<-fe.completeCh
	log.Debugf("FakeExecutor: completed task %q (retry=%t, err=%v)", st.GetExecutionTask().GetExecutionId(), fe.retry, fe.err)
	return fe.retry, fe.err
}

type FakeRunnerPool struct {
	interfaces.RunnerPool
}

func (*FakeRunnerPool) Wait() {
}

type FakeTaskLeaser struct {
	GrantedLeases chan *FakeLease
}

func (f *FakeTaskLeaser) Lease(ctx context.Context, taskID string) (interfaces.TaskLease, error) {
	if f.GrantedLeases == nil {
		f.GrantedLeases = make(chan *FakeLease, 512)
	}
	lease := &FakeLease{
		ctx:    ctx,
		taskID: taskID,
		closed: make(chan struct{}),
	}
	f.GrantedLeases <- lease
	return lease, nil
}

type FakeLease struct {
	err   error
	retry bool

	ctx    context.Context
	taskID string
	closed chan struct{}
}

func (f *FakeLease) Task() *repb.ExecutionTask {
	return &repb.ExecutionTask{
		ExecutionId: f.taskID,
	}
}

func (f *FakeLease) Context() context.Context {
	return f.ctx
}

func (f *FakeLease) Close(ctx context.Context, err error, retry bool) {
	f.err = err
	f.retry = retry
	close(f.closed)
}

func (f *FakeLease) WaitClosed() (retry bool, err error) {
	<-f.closed
	return f.retry, f.err
}

// TODO: fake the operation publisher instead of faking the low-level
// ExecutionClient.
type FakeExecutionClient struct {
	repb.ExecutionClient

	// CloseAndRecvErr is the error to be returned from any CloseAndRecv call on
	// any PublishOperationClient.
	CloseAndRecvErr error
}

func (f *FakeExecutionClient) PublishOperation(ctx context.Context, opts ...grpc.CallOption) (repb.Execution_PublishOperationClient, error) {
	return &FakePublishOperationClient{
		CloseAndRecvErr: f.CloseAndRecvErr,
	}, nil
}

type FakePublishOperationClient struct {
	repb.Execution_PublishOperationClient

	// CloseAndRecvErr is the error to return from CloseAndRecv.
	CloseAndRecvErr error
}

func (f *FakePublishOperationClient) CloseAndRecv() (*repb.PublishOperationResponse, error) {
	return nil, f.CloseAndRecvErr
}
