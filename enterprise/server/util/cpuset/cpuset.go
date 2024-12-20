package cpuset

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/prometheus/procfs"
	"golang.org/x/exp/slices"
)

var _ interfaces.CPULeaser = (*cpuLeaser)(nil)

type cpuLeaser struct {
	mu     sync.Mutex
	leases map[int][]string
}

func (l *cpuLeaser) Acquire(numCPUs int) (func(), []int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pq := priority_queue.New[int](priority_queue.WithEmptyValue(-1))

	for cpuid, tags := range l.leases {
		// we want the least loaded cpus first, so give the
		// cpus with more tasks a more negative score.
		pq.Push(cpuid, -1*len(tags))
	}

	tag := uuid.New()
	leastLoaded := make([]int, 0)
	for i := 0; i < numCPUs; i++ {
		cpuid := pq.Pop()
		if cpuid == -1 {
			break
		}
		l.leases[cpuid] = append(l.leases[cpuid], tag)
		leastLoaded = append(leastLoaded, cpuid)
	}
	return func() {
		l.release(tag)
	}, leastLoaded
}

func (l *cpuLeaser) release(tag string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for cpuid, tags := range l.leases {
		l.leases[cpuid] = slices.DeleteFunc(tags, func(s string) bool {
			return s == tag
		})
	}
}

type Options struct {
	testNumCPUs int
}

type Option func(*Options)

func WithTestOnlySetNumCPUs(numCPUs int) Option {
	return func(o *Options) {
		o.testNumCPUs = numCPUs
	}
}

func NewLeaser(opts ...Option) (interfaces.CPULeaser, error) {
	fs, err := procfs.NewDefaultFS()
	if err != nil {
		return nil, err
	}
	cpuInfos, err := fs.CPUInfo()
	if err != nil {
		return nil, err
	}
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	numCPUs := len(cpuInfos)
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
