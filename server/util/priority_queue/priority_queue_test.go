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

	assert.Equal(t, "E", q.Pop())
	assert.Equal(t, "D", q.Pop())
	assert.Equal(t, "B", q.Pop())
	assert.Equal(t, "A", q.Pop())
	assert.Equal(t, "", q.Pop())
}
