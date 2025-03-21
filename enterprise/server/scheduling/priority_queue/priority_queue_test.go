package priority_queue_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_queue"
	"github.com/stretchr/testify/require"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

func TestIndexedOperations(t *testing.T) {
	// Run the test several times to get high confidence that we're covering
	// edge cases, since each run is randomized.
	for range 1000 {
		// Choose initial queue size randomly from [0, 16).
		n := rand.N(16)
		// Create n expectedTasks with numbered task IDs.
		// All expectedTasks have the same priority for this test, so the queue
		// should behave like a FIFO queue.
		expectedTasks := make([]*scpb.EnqueueTaskReservationRequest, n)
		for j := range n {
			expectedTasks[j] = &scpb.EnqueueTaskReservationRequest{
				TaskId: fmt.Sprintf("%d", j),
			}
		}
		// Shuffle the tasks.
		rand.Shuffle(n, func(i, j int) {
			expectedTasks[i], expectedTasks[j] = expectedTasks[j], expectedTasks[i]
		})
		// Create a priority queue and enqueue all of the tasks.
		pq := priority_queue.NewPriorityQueue()
		for _, task := range expectedTasks {
			pq.Push(task)
		}
		// Perform a random number of Push and RemoveAt operations. For each
		// iteration, verify that PeekAt() returns the expected tasks.
		for range rand.N(n + 1) {
			r := rand.Float64()
			if r < 0.5 {
				// Remove a random task.
				removedTask := pq.RemoveAt(rand.N(pq.Len()))
				expectedTasks = slices.DeleteFunc(expectedTasks, func(el *scpb.EnqueueTaskReservationRequest) bool {
					return el == removedTask
				})
			} else {
				// Add a new task.
				task := &scpb.EnqueueTaskReservationRequest{
					TaskId: fmt.Sprintf("%d", n),
				}
				n++
				pq.Push(task)
				expectedTasks = append(expectedTasks, task)
			}
			// Check that PeekAt() returns the expected results.
			for i := range expectedTasks {
				peekedTask := pq.PeekAt(i)
				require.Equal(t, expectedTasks[i], peekedTask)
			}
			require.Equal(t, len(expectedTasks), pq.Len())
			require.Nil(t, pq.PeekAt(len(expectedTasks)))
		}
		// Pop all remaining tasks and verify the order.
		for _, task := range expectedTasks {
			dequeuedTask := pq.Pop()
			require.Equal(t, task.GetTaskId(), dequeuedTask.GetTaskId())
		}
		// Verify that the queue is empty.
		require.Nil(t, pq.Pop())
		require.Equal(t, 0, pq.Len())
	}
}
