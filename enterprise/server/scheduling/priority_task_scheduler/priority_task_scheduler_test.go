package priority_task_scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

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

func TestTaskQueue_RespectsDiskResourcesIfConfigured(t *testing.T) {
	flags.Set(t, "executor.millicpu", 8_000)
	flags.Set(t, "executor.memory_bytes", 32e9)
	flags.Set(t, "executor.disk.read_iops", 1000)
	flags.Set(t, "executor.disk.write_iops", 100)
	flags.Set(t, "executor.disk.read_bps", 1000*4096)
	flags.Set(t, "executor.disk.write_bps", 100*4096)
	err := resources.Configure(false /*=mmapLRUEnabled*/)
	require.NoError(t, err)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ex := &fakeExecutor{}

	bigScheduled := make(chan struct{})
	smallDone := make(chan struct{}, 1)
	bigDone := make(chan struct{})
	tasks := map[string]func(){
		"small": func() {
			<-bigScheduled
			time.Sleep(time.Duration(rand.Intn(int(1 * time.Millisecond))))
			smallDone <- struct{}{}
		},
		"big": func() {
			defer func() { bigDone <- struct{}{} }()
			select {
			case <-smallDone:
			default:
				require.FailNow(t, "small task should be done by the time the big task starts")
			}
		},
	}
	leaseAndRun := func(ctx context.Context, reservation *scpb.EnqueueTaskReservationRequest) {
		tasks[reservation.GetTaskId()]()
	}
	scheduler := NewPriorityTaskScheduler(env, ex, interfaces.RunnerPool(nil), &Options{
		LeaseAndRunFunc: leaseAndRun,
	})
	err = scheduler.Start()
	require.NoError(t, err)

	smallTask := &scpb.EnqueueTaskReservationRequest{
		TaskId: "small",
		TaskSize: &scpb.TaskSize{
			EstimatedMilliCpu:    1,
			EstimatedMemoryBytes: 1,
			DiskReadIops:         1,
			DiskWriteIops:        1,
			DiskReadBps:          1,
			DiskWriteBps:         1,
		},
	}
	bigTask := &scpb.EnqueueTaskReservationRequest{
		TaskId: "big",
		TaskSize: &scpb.TaskSize{
			EstimatedMilliCpu:    1000,
			EstimatedMemoryBytes: 1000,
			DiskReadIops:         1000,
			DiskWriteIops:        100,
			DiskReadBps:          1000 * 4096,
			DiskWriteBps:         100 * 4096,
		},
	}
	for range 10 {
		_, err = scheduler.EnqueueTaskReservation(ctx, smallTask)
		require.NoError(t, err)
		_, err = scheduler.EnqueueTaskReservation(ctx, bigTask)
		require.NoError(t, err)
		bigScheduled <- struct{}{}
		<-bigDone
	}
}

type fakeExecutor struct{}

func (x *fakeExecutor) ID() string {
	return "test-executor"
}

func (x *fakeExecutor) HostID() string {
	return "test-host"
}

func (x *fakeExecutor) ExecuteTaskAndStreamResults(ctx context.Context, st *repb.ScheduledTask, stream *operation.Publisher) (bool, error) {
	return false, fmt.Errorf("not implemented")
}
