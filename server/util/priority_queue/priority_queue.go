package priority_queue

import (
	"container/heap"
	"slices"
	"sync"
	"time"
)

// A pqItem is the element managed by a priority queue.
type pqItem[V any] struct {
	value      V
	priority   int
	insertTime time.Time
}

// innerPQ implements heap.Interface and holds pqItems.
type innerPQ[V any] []*pqItem[V]

func (pq innerPQ[V]) Len() int { return len(pq) }
func (pq innerPQ[V]) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority ||
		(pq[i].priority == pq[j].priority && pq[i].insertTime.Before(pq[j].insertTime))
}
func (pq innerPQ[V]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}
func (pq *innerPQ[V]) Push(x any) {
	item := x.(*pqItem[V])
	*pq = append(*pq, item)
}
func (pq *innerPQ[V]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

// PriorityQueue implements a thread safe priority queue for type V.
// If the queue is empty, calling Pop() or Peek() will return a zero value of
// type V, or a specific empty value configured via options.
type PriorityQueue[V any] struct {
	mu    sync.Mutex // protects inner
	inner innerPQ[V]
}

func New[V any]() *PriorityQueue[V] {
	return &PriorityQueue[V]{}
}

func (pq *PriorityQueue[V]) Clone() *PriorityQueue[V] {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return &PriorityQueue[V]{
		inner: slices.Clone(pq.inner),
	}
}

func (pq *PriorityQueue[V]) zeroValue() V {
	var zero V
	return zero
}

func (pq *PriorityQueue[V]) Push(v V, priority int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(&pq.inner, &pqItem[V]{
		value:      v,
		insertTime: time.Now(),
		priority:   priority,
	})
}

func (pq *PriorityQueue[V]) Pop() (V, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.inner) == 0 {
		return pq.zeroValue(), false
	}
	item := heap.Pop(&pq.inner).(*pqItem[V])
	return item.value, true
}

func (pq *PriorityQueue[V]) Peek() (V, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.inner) == 0 {
		return pq.zeroValue(), false
	}
	return (pq.inner)[0].value, true
}

// RemoveAt removes the item with the (i+1)'th highest priority from the queue.
//
// RemoveAt(0) removes the highest priority item, and is equivalent to Pop().
// RemoveAt(1) removes the item with the second highest priority, and so on.
//
// Ties in priority are broken by insertion time.
//
// It has complexity O(index * log(n)) where n is the number of elements in the
// queue.
func (pq *PriorityQueue[V]) RemoveAt(index int) (V, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if index >= len(pq.inner) {
		return pq.zeroValue(), false
	}

	// Pop `index` elements
	removed := make([]any, index) // TODO: reuse this buffer?
	for i := range index {
		removed[i] = heap.Pop(&pq.inner)
	}

	// Pop one more element to get the item to remove
	item := heap.Pop(&pq.inner)

	// Add the removed elements back to the queue
	for _, v := range removed {
		heap.Push(&pq.inner, v)
	}

	return item.(*pqItem[V]).value, true
}

func (pq *PriorityQueue[V]) GetAll() []V {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	allValues := make([]V, len(pq.inner))
	for i, v := range pq.inner {
		allValues[i] = v.value
	}
	return allValues
}

func (pq *PriorityQueue[V]) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.inner)
}
