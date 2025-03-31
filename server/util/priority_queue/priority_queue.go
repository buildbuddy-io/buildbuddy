package priority_queue

import (
	"container/heap"
	"slices"
	"sync"
	"time"
)

// Item is an element managed by a priority queue.
type Item[V any] struct {
	value      V
	index      int // The index of the item in the heap
	priority   float64
	insertTime time.Time
}

func (i *Item[V]) Value() V {
	return i.value
}
func NewItem[V any](v V, priority float64) *Item[V] {
	return &Item[V]{
		value:      v,
		insertTime: time.Now(),
		priority:   priority,
	}
}

// PriorityQueue implements heap.Interface and holds items.
type PriorityQueue[V any] []*Item[V]

func (pq PriorityQueue[V]) Len() int { return len(pq) }
func (pq PriorityQueue[V]) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority ||
		(pq[i].priority == pq[j].priority && pq[i].insertTime.Before(pq[j].insertTime))
}
func (pq PriorityQueue[V]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}
func (pq *PriorityQueue[V]) Push(x any) {
	n := len(*pq)
	item := x.(*Item[V])
	item.index = n
	*pq = append(*pq, item)
}
func (pq *PriorityQueue[V]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
func (pq *PriorityQueue[V]) Update(item *Item[V], priority float64) {
	item.priority = priority
	heap.Fix(pq, item.index)
}

// RemoveItemWithMinPriority removes the item with the minimum priority and
// returns the removed item's value.
func (pq *PriorityQueue[V]) RemoveItemWithMinPriority() *Item[V] {
	old := *pq
	n := len(old)

	// The min item can only be at the leaf nodes; so we only need to scan the
	// right half of the array.
	minIndex := n / 2

	for i := n/2 + 1; i < n; i++ {
		if pq.Less(minIndex, i) {
			minIndex = i
		}
	}
	return heap.Remove(pq, minIndex).(*Item[V])
}

// ThreadSafePriorityQueue implements a thread safe priority queue for type V.
// If the queue is empty, calling Pop() or Peek() will return a zero value of
// type V, or a specific empty value configured via options.
type ThreadSafePriorityQueue[V any] struct {
	mu    sync.Mutex // protects inner
	inner PriorityQueue[V]
}

func New[V any]() *ThreadSafePriorityQueue[V] {
	return &ThreadSafePriorityQueue[V]{}
}

func (pq *ThreadSafePriorityQueue[V]) Clone() *ThreadSafePriorityQueue[V] {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return &ThreadSafePriorityQueue[V]{
		inner: slices.Clone(pq.inner),
	}
}

func (pq *ThreadSafePriorityQueue[V]) zeroValue() V {
	var zero V
	return zero
}

func (pq *ThreadSafePriorityQueue[V]) Push(v V, priority float64) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(&pq.inner, NewItem(v, priority))
}

func (pq *ThreadSafePriorityQueue[V]) Pop() (V, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(pq.inner) == 0 {
		return pq.zeroValue(), false
	}
	item := heap.Pop(&pq.inner).(*Item[V])
	return item.value, true
}

func (pq *ThreadSafePriorityQueue[V]) Peek() (V, bool) {
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
func (pq *ThreadSafePriorityQueue[V]) RemoveAt(index int) (V, bool) {
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

	return item.(*Item[V]).value, true
}

func (pq *ThreadSafePriorityQueue[V]) GetAll() []V {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	allValues := make([]V, len(pq.inner))
	for i, v := range pq.inner {
		allValues[i] = v.value
	}
	return allValues
}

func (pq *ThreadSafePriorityQueue[V]) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.inner)
}
