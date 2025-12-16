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

func TestGetAllIndexDoesNotMatchRemoveAt(t *testing.T) {
	q := priority_queue.New[string]()
	// Insert in this specific order to create heap where array order != priority order
	q.Push("A", 1)
	q.Push("E", 5)
	q.Push("D", 4)
	q.Push("B", 2)

	// Heap array order after insertions: [E, B, D, A]
	// (B bubbles up past A because 2 > 1)
	//
	// Priority order: [E(5), D(4), B(2), A(1)]
	//
	// These differ at indices 1 and 2!

	all := q.GetAll()
	// Find where "D" is in the array
	var dIndex int
	for i, v := range all {
		if v == "D" {
			dIndex = i
			break
		}
	}
	// D is at array index 2

	// Now try to remove D using that index
	removed, ok := q.RemoveAt(dIndex)
	require.True(t, ok)

	// BUG: We wanted to remove "D" but RemoveAt(2) removes the 3rd
	// highest priority item, which is "B"!
	assert.Equal(t, "D", removed) // FAILS - removed is "B"
}
