package priority_queue

import (
	"container/heap"
	"sync"
	"time"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

// A pqItem is the element managed by a priority queue.
type pqItem struct {
	value      *scpb.EnqueueTaskReservationRequest
	priority   int
	insertTime time.Time
}

// A priorityQueue implements heap.Interface and holds pqItems.
type innerPQ []*pqItem

func (pq innerPQ) Len() int { return len(pq) }
func (pq innerPQ) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority ||
		(pq[i].priority == pq[j].priority && pq[i].insertTime.Before(pq[j].insertTime))
}
func (pq innerPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}
func (pq *innerPQ) Push(x interface{}) {
	item := x.(*pqItem)
	*pq = append(*pq, item)
}
func (pq *innerPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

type PriorityQueue struct {
	inner *innerPQ
	mu    sync.Mutex
}

func NewPriorityQueue() *PriorityQueue {
	inner := make(innerPQ, 0)
	return &PriorityQueue{
		inner: &inner,
	}
}

func (pq *PriorityQueue) Push(req *scpb.EnqueueTaskReservationRequest) {
	pq.mu.Lock()
	heap.Push(pq.inner, &pqItem{
		value:      req,
		priority:   0,
		insertTime: time.Now(),
	})

	pq.mu.Unlock()
}

func (pq *PriorityQueue) Pop() *scpb.EnqueueTaskReservationRequest {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(*pq.inner) == 0 {
		return nil
	}
	item := heap.Pop(pq.inner).(*pqItem)
	return item.value
}

func (pq *PriorityQueue) Peek() *scpb.EnqueueTaskReservationRequest {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(*pq.inner) == 0 {
		return nil
	}
	return (*pq.inner)[0].value
}

func (pq *PriorityQueue) GetAll() []*scpb.EnqueueTaskReservationRequest {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	var reservations []*scpb.EnqueueTaskReservationRequest
	for _, i := range *pq.inner {
		reservations = append(reservations, i.value)
	}
	return reservations
}

func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(*pq.inner)
}
