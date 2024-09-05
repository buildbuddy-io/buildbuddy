package priority_task_scheduler

import (
	"testing"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	"github.com/stretchr/testify/require"
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
