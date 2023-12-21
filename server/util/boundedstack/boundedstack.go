package boundedstack

import (
	"context"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// BoundedStack is a concurrency-safe stack with bounded size: when the capacity
// of the stack is exceeded, the least recently added item ("bottom") of the
// stack is removed.
//
// An example use case for a BoundedStack is eager data fetching, where data
// is fetched purely as an optimization, and more recent fetches should replace
// older ones when there is a high request volume but limited worker capacity.
//
// Example code for a goroutine that processes the latest items pushed to the
// stack:
//
//	for {
//		val, err := stack.Recv(ctx)
//		if err != nil {
//			break // ctx was canceled
//		}
//		// Do something with val...
//	}
type BoundedStack[T any] struct {
	// recvNotify is a channel used to wake up any goroutines in Recv whenever a
	// value is pushed to the stack.
	recvNotify chan struct{}

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
		recvNotify: make(chan struct{}, capacity),
		buffer:     make([]T, capacity),
	}, nil
}

// Push adds an item to the top of the stack, evicting the bottom item of the
// stack if the stack is full.
func (s *BoundedStack[T]) Push(item T) {
	s.push(item)
	// Notify the channel that a new item was pushed.
	select {
	case s.recvNotify <- struct{}{}:
	default:
		// If the channel buffer is full then it just means that the goroutines
		// processing values from this stack aren't keeping up; this is fine.
	}
}

// pop returns the top element of the stack. The second return value indicates
// whether the returned item is valid; it will be false if and only if the stack
// was empty.
//
// Pop does not block. Recv can be used to block until an item is available.
func (c *BoundedStack[T]) pop() (item T, ok bool) {
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

// Recv blocks until an item is available, then pops from the top of the stack.
// If the returned error is non-nil then it will be equal to ctx.Err().
func (s *BoundedStack[T]) Recv(ctx context.Context) (item T, err error) {
	for {
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-s.recvNotify:
			item, ok := s.pop()
			if !ok {
				// This can potentially happen if more pushes happened after the
				// recv call and then other goroutines woke up and consumed the
				// items that were pushed before we got a chance to call pop().
				// This is fine (and is probably rare in practice), so for now
				// just retry.
				continue
			}
			return item, nil
		}
	}
}

func (s *BoundedStack[T]) push(item T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.top = (s.top + 1) % len(s.buffer)
	s.buffer[s.top] = item
	s.size = min(s.size+1, len(s.buffer))
}
