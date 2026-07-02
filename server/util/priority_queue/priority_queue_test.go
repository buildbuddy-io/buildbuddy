package priority_queue_test

import (
	"container/heap"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPushPop(t *testing.T) {
	q := priority_queue.New[string]()
	q.Push("A", 1)
	q.Push("E", 5)
	q.Push("D", 4)
	q.Push("B", 2)

	v, ok := q.Pop()
	assert.Equal(t, "E", v)
	assert.True(t, ok)

	v, ok = q.Pop()
	assert.Equal(t, "D", v)
	assert.True(t, ok)

	v, ok = q.Pop()
	assert.Equal(t, "B", v)
	assert.True(t, ok)

	v, ok = q.Pop()
	assert.Equal(t, "A", v)
	assert.True(t, ok)

	v, ok = q.Pop()
	assert.Equal(t, "", v)
	assert.False(t, ok)
}

func TestZeroValue(t *testing.T) {
	q := priority_queue.New[int]()
	q.Push(1, 1)
	q.Push(2, 5)
	q.Push(3, 4)

	v, ok := q.Pop()
	assert.Equal(t, 2, v)
	assert.True(t, ok)

	v, ok = q.Pop()
	assert.Equal(t, 3, v)
	assert.True(t, ok)

	v, ok = q.Pop()
	assert.Equal(t, 1, v)
	assert.True(t, ok)

	v, ok = q.Pop()
	assert.Equal(t, 0, v)
	assert.False(t, ok)

	v, ok = q.Pop()
	assert.Equal(t, 0, v)
	assert.False(t, ok)
}

func TestRemoveAt(t *testing.T) {
	q := priority_queue.New[string]()
	q.Push("A", 1)
	q.Push("E", 5)
	q.Push("D", 4)
	q.Push("B", 2)

	// Queue should now be [E, D, B, A]

	// Remove B from the middle of the queue:
	v, ok := q.RemoveAt(2)
	require.True(t, ok)
	require.Equal(t, "B", v)

	// Queue should now be [E, D, A]

	// Remove E from the head of the queue:
	v, ok = q.RemoveAt(0)
	require.True(t, ok)
	require.Equal(t, "E", v)

	// Queue should now be [D, A]

	// Remove A from the tail of the queue:
	v, ok = q.RemoveAt(1)
	require.True(t, ok)
	require.Equal(t, "A", v)

	// Queue should now be [D]

	// Remove D from the queue:
	v, ok = q.RemoveAt(0)
	require.True(t, ok)
	require.Equal(t, "D", v)

	// Queue should now be empty
	_, ok = q.RemoveAt(0)
	require.False(t, ok)
}

func TestGetAllUnordered(t *testing.T) {
	q := priority_queue.New[string]()
	q.Push("A", 1)
	q.Push("B", 2)
	q.Push("C", 3)
	q.Push("D", 4)

	// GetAllUnordered should return all elements, in any order.
	require.ElementsMatch(t, []string{"A", "B", "C", "D"}, q.GetAllUnordered())

	// The queue should not be modified.
	require.Equal(t, 4, q.Len())
}

func TestGetAllOrdered(t *testing.T) {
	q := priority_queue.New[string]()
	// Push elements in increasing priority order, which leaves the heap's
	// internal array out of priority order.
	q.Push("A", 1)
	q.Push("B", 2)
	q.Push("C", 3)
	q.Push("D", 4)
	// Push an element tied with B's priority; the tie should be broken by
	// insertion time, so B comes first.
	q.Push("E", 2)

	// GetAllOrdered should return elements in priority order, highest
	// priority first.
	require.Equal(t, []string{"D", "C", "B", "E", "A"}, q.GetAllOrdered())

	// The queue should not be modified: it should still contain all elements
	// and pop them in priority order.
	require.Equal(t, 5, q.Len())
	popped := make([]string, 0, 5)
	for range 5 {
		v, ok := q.Pop()
		require.True(t, ok)
		popped = append(popped, v)
	}
	require.Equal(t, []string{"D", "C", "B", "E", "A"}, popped)
}

func TestRemoveItemWithMinPriority(t *testing.T) {
	pq := &priority_queue.PriorityQueue[string]{}
	heap.Push(pq, priority_queue.NewItem("A", 1))
	heap.Push(pq, priority_queue.NewItem("B", 2))

	item := pq.RemoveItemWithMinPriority()
	require.Equal(t, "A", item.Value())

	heap.Push(pq, priority_queue.NewItem("C", 3))
	heap.Push(pq, priority_queue.NewItem("D", 4))
	item = pq.RemoveItemWithMinPriority()
	require.Equal(t, "B", item.Value())
}
