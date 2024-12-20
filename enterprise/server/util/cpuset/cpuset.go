package cpuset

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
	"golang.org/x/exp/slices"
)

const emptyValue = -1

// Compile-time check that cpuLeaser implements the interface.
var _ interfaces.CPULeaser = (*cpuLeaser)(nil)

type cpuLeaser struct {
	mu     sync.Mutex
	leases map[int][]string
}

// Acquire leases a set of CPUs (identified by index) for a task. The returned
// function should be called to free the CPUs when they are no longer used.
func (l *cpuLeaser) Acquire(numCPUs int, taskID string) ([]int, func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pq := priority_queue.New[int](priority_queue.WithEmptyValue(emptyValue))

	for cpuid, tasks := range l.leases {
		// we want the least loaded cpus first, so give the
		// cpus with more tasks a more negative score.
		pq.Push(cpuid, -1*len(tasks))
	}

	leastLoaded := make([]int, 0)
	for i := 0; i < numCPUs; i++ {
		cpuid := pq.Pop()
		if cpuid == emptyValue {
			// Task is requesting more CPUs than are available;
			// just return the available CPUs.
			break
		}
		l.leases[cpuid] = append(l.leases[cpuid], taskID)
		leastLoaded = append(leastLoaded, cpuid)
	}
	return leastLoaded, func() {
		l.release(taskID)
	}
}

func (l *cpuLeaser) release(taskID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for cpuid, tasks := range l.leases {
		l.leases[cpuid] = slices.DeleteFunc(tasks, func(s string) bool {
			return s == taskID
		})
	}
}

type Options struct {
	testNumCPUs int
}

type Option func(*Options)

// WithTestOnlySetNumCPUs overrides the number of assignable CPUs. TEST ONLY!
func WithTestOnlySetNumCPUs(numCPUs int) Option {
	return func(o *Options) {
		o.testNumCPUs = numCPUs
	}
}

func NewLeaser(opts ...Option) (interfaces.CPULeaser, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	numCPUs := resources.GetNumCPUs()
	if options.testNumCPUs > 0 {
		numCPUs = options.testNumCPUs
	}

	cl := &cpuLeaser{
		leases: make(map[int][]string),
	}
	for i := 0; i < numCPUs; i++ {
		cl.leases[i] = make([]string, 0)
	}
	return cl, nil
}
