// Priority queue implementation that is tailored for the executor's local task
// queue. In particular, it expects tasks to mostly be inserted at the end of
// the priority queue, and tasks to mostly be removed from the front of the
// queue. This allows us to avoid worrying about Push() and RemoveAt() being
// slow for the most part.
//
// A more general-purpose implementation can be found in
// server/util/priority_queue.go

package priority_queue

import (
	"sync"
	"time"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

type pqItem struct {
	value      *scpb.EnqueueTaskReservationRequest
	priority   int
	insertTime time.Time
}

func less(pq []pqItem, i, j int) bool {
	return pq[i].priority > pq[j].priority ||
		(pq[i].priority == pq[j].priority && pq[i].insertTime.Before(pq[j].insertTime))
}

type PriorityQueue struct {
	items []pqItem
	mu    sync.Mutex
}

func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{}
}

func (pq *PriorityQueue) Push(req *scpb.EnqueueTaskReservationRequest) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	pq.items = append(pq.items, pqItem{
		value:      req,
		insertTime: time.Now(),
		// Note: the remote API specifies that higher `priority` values are
		// assigned to actions that should execute *later*, so we invert the
		// priority here.
		priority: -int(req.GetSchedulingMetadata().GetPriority()),
	})
	// Perform one round of bubble-sort to get the item to the correct position
	// in the list. Note that for executor scheduling, we expect most tasks to
	// wind up at the end of the list, in which case this loop doesn't run at
	// all.
	for i := len(pq.items) - 1; i > 0 && less(pq.items, i, i-1); i-- {
		pq.items[i], pq.items[i-1] = pq.items[i-1], pq.items[i]
	}
}

func (pq *PriorityQueue) Pop() *scpb.EnqueueTaskReservationRequest {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.items) == 0 {
		return nil
	}
	item := pq.items[0]
	// Release references to avoid leaking memory.
	pq.items[0] = pqItem{}
	pq.items = pq.items[1:]
	return item.value
}

func (pq *PriorityQueue) RemoveAt(i int) *scpb.EnqueueTaskReservationRequest {
	// Avoid expensive shift operation in the common case of removing the first
	// element.
	if i == 0 {
		return pq.Pop()
	}
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if i >= len(pq.items) {
		return nil
	}
	item := pq.items[i]
	lastElement := &pq.items[len(pq.items)-1]
	pq.items = append(pq.items[:i], pq.items[i+1:]...)
	// After shifting forward, the backing array will have a duplicate element
	// at the end. Release references held by this duplicate element to avoid
	// leaking memory.
	*lastElement = pqItem{}
	return item.value
}

func (pq *PriorityQueue) Peek() *scpb.EnqueueTaskReservationRequest {
	return pq.PeekAt(0)
}

func (pq *PriorityQueue) PeekAt(i int) *scpb.EnqueueTaskReservationRequest {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if i >= len(pq.items) {
		return nil
	}
	return pq.items[i].value
}

func (pq *PriorityQueue) GetAll() []*scpb.EnqueueTaskReservationRequest {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	reservations := make([]*scpb.EnqueueTaskReservationRequest, 0, len(pq.items))
	for _, item := range pq.items {
		reservations = append(reservations, item.value)
	}
	return reservations
}

func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.items)
}
