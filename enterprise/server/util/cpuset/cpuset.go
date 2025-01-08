package cpuset

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var (
	cpuLeaserEnable      = flag.Bool("executor.cpu_leaser.enable", false, "Enable cpu leaser functionality")
	cpuLeaserOverhead    = flag.Float64("executor.cpu_leaser.overhead", .20, "The amount of extra CPU *above the task size* to include in a lease")
	cpuLeaserMinOverhead = flag.Int("executor.cpu_leaser.min_overhead", 2, "Always ensure at least this many extra cpus are included in a lease")
	cpuLeaserCPUSet      = flag.String("executor.cpu_leaser.cpuset", "", "Manual override for the set of CPUs that may be leased. Ignored if empty. Ex. '0-1,3'")
)

// Compile-time check that cpuLeaser implements the interface.
var _ interfaces.CPULeaser = (*cpuLeaser)(nil)

type cpuLeaser struct {
	mu     sync.Mutex
	leases map[int][]string
}

func parseCPUSet(s string) ([]int, error) {
	// Example: "0-1,3" is parsed as []int{0, 1, 3}
	var nodes []int
	nodeRanges := strings.Split(s, ",")
	for _, r := range nodeRanges {
		startStr, endStr, _ := strings.Cut(r, "-")
		start, err := strconv.Atoi(startStr)
		if err != nil {
			return nodes, fmt.Errorf("malformed file contents")
		}
		end := start
		if endStr != "" {
			n, err := strconv.Atoi(endStr)
			if err != nil {
				return nodes, fmt.Errorf("malformed file contents")
			}
			if n < start {
				return nodes, fmt.Errorf("malformed file contents")
			}
			end = n
		}
		for node := start; node <= end; node++ {
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

func NewLeaser() (interfaces.CPULeaser, error) {
	cl := &cpuLeaser{
		leases: make(map[int][]string),
	}

	var cpus []int
	if *cpuLeaserCPUSet != "" {
		c, err := parseCPUSet(*cpuLeaserCPUSet)
		if err != nil {
			return nil, err
		}
		cpus = c
	} else {
		cpus = GetCPUs()
	}

	for _, cpu := range cpus {
		cl.leases[cpu] = make([]string, 0)
	}
	return cl, nil
}

func computeNumCPUs(milliCPU int64) int {
	rawNumCPUs := int(math.Ceil(float64(milliCPU) / 1000.0))
	overheadCPUs := int(*cpuLeaserOverhead*float64(milliCPU)) / 1000
	if overheadCPUs < *cpuLeaserMinOverhead {
		overheadCPUs = *cpuLeaserMinOverhead
	}
	return rawNumCPUs + overheadCPUs
}

// Acquire leases a set of CPUs (identified by index) for a task. The returned
// function should be called to free the CPUs when they are no longer used.
func (l *cpuLeaser) Acquire(milliCPU int64, taskID string) ([]int, func()) {
	numCPUs := computeNumCPUs(milliCPU)
	pq := priority_queue.New[int]()

	l.mu.Lock()
	defer l.mu.Unlock()

	// If the CPU leaser is disabled; return all CPUs.
	if !*cpuLeaserEnable {
		return maps.Keys(l.leases), func() {}
	}

	for cpuid, tasks := range l.leases {
		// we want the least loaded cpus first, so give the
		// cpus with more tasks a more negative score.
		pq.Push(cpuid, -1*len(tasks))
	}

	leastLoaded := make([]int, 0)
	for i := 0; i < numCPUs; i++ {
		cpuid, ok := pq.Pop()
		if !ok {
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
