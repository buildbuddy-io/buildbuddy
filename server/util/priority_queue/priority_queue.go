package priority_queue

import (
	"container/heap"
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
	inner       *innerPQ[V]
	mu          sync.Mutex
	noValueFunc func() V
}

type Options struct {
	noValueGetter func() any
}

func defaultOptions() *Options {
	return &Options{}
}

type Option func(*Options)

func WithEmptyValue(v any) Option {
	return func(o *Options) {
		o.noValueGetter = func() any {
			return v
		}
	}
}

func New[V any](opts ...Option) *PriorityQueue[V] {
	inner := make(innerPQ[V], 0)
	pq := &PriorityQueue[V]{
		inner: &inner,
	}
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}
	if options.noValueGetter != nil {
		pq.noValueFunc = func() V {
			return options.noValueGetter().(V)
		}
	} else {
		pq.noValueFunc = pq.zeroValue
	}
	return pq
}

func (pq *PriorityQueue[V]) zeroValue() V {
	var zero V
	return zero
}

func (pq *PriorityQueue[V]) Push(v V, priority int) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(pq.inner, &pqItem[V]{
		value:      v,
		insertTime: time.Now(),
		priority:   priority,
	})
}

func (pq *PriorityQueue[V]) Pop() V {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(*pq.inner) == 0 {
		return pq.noValueFunc()
	}
	item := heap.Pop(pq.inner).(*pqItem[V])
	return item.value
}

func (pq *PriorityQueue[V]) Peek() V {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if len(*pq.inner) == 0 {
		return pq.noValueFunc()
	}
	return (*pq.inner)[0].value
}

func (pq *PriorityQueue[V]) GetAll() []V {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	var allValues []V
	for _, i := range *pq.inner {
		allValues = append(allValues, i.value)
	}
	return allValues
}

func (pq *PriorityQueue[V]) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(*pq.inner)
}
