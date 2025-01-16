package priority_queue_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
	"github.com/stretchr/testify/assert"
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
