package boundedstack

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// BoundedStack is a concurrency-safe stack with bounded size: when the capacity
// of the stack is exceeded, the least recently added item ("bottom") of the
// stack is removed.
//
// Its intended usage is to prioritize the most recently added items for eager
// chunk fetching.
//
// It exposes a channel C which streams a value whenever a value is pushed on
// the stack.
//
// IMPORTANT: after receiving a message from C, make sure to always call Pop.
// Otherwise, the stack item corresponding to the notification on C may be
// dropped unnecessarily.
//
// Example code for a goroutine that processes the latest items pushed to the
// stack:
//
//	for range stack.C {
//		val, ok := stack.Pop()
//		if !ok {
//			// A receive from C normally guarantees that a value is ready,
//			// but it's best to always check `ok` when popping from the stack.
//			continue
//		}
//		// Do something with val...
//	}
type BoundedStack[T any] struct {
	// C is a notification channel which allows goroutines to listen for values
	// being pushed to the stack.
	//
	// After a goroutine receives from C, it must call Pop - otherwise the stack
	// item corresponding to the notification on C may be dropped unnecessarily.
	C chan struct{}

	mu     sync.Mutex
	buffer []T
	top    int
	size   int
}

func New[T any](capacity int) (*BoundedStack[T], error) {
	if capacity <= 0 {
		return nil, status.InvalidArgumentError("bounded stack capacity must be > 0")
	}
	return &BoundedStack[T]{
		C:      make(chan struct{}, capacity),
		buffer: make([]T, capacity),
	}, nil
}

// Push adds an item to the top of the stack, evicting the bottom item of the
// stack if the stack is full.
func (s *BoundedStack[T]) Push(item T) {
	s.push(item)
	// Notify the channel that a new item was pushed.
	select {
	case s.C <- struct{}{}:
	default:
		// If the channel buffer is full then it just means that the goroutines
		// processing values from this stack aren't keeping up; this is fine.
	}
}

func (s *BoundedStack[T]) push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.top = (s.top + 1) % len(s.buffer)
	s.buffer[s.top] = item
	s.size = min(s.size+1, len(s.buffer))
}

// Pop returns the top element of the stack. The second return value indicates
// whether the returned item is valid; it will be false if and only if the stack
// was empty.
func (c *BoundedStack[T]) Pop() (item T, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.size == 0 {
		var zero T
		return zero, false
	}
	item = c.buffer[c.top]
	// Zero out the item from the buffer to avoid keeping unnecessary references
	// to any values transitively referenced by T (so that they can be
	// garbage-collected).
	var zero T
	c.buffer[c.top] = zero

	c.top = (c.top - 1 + len(c.buffer)) % len(c.buffer)
	c.size--
	return item, true
}
