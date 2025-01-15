package cpuset

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/priority_queue"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

var (
	cpuLeaserEnable      = flag.Bool("executor.cpu_leaser.enable", false, "Enable cpu leaser functionality")
	cpuLeaserOverhead    = flag.Float64("executor.cpu_leaser.overhead", .20, "The amount of extra CPU *above the task size* to include in a lease")
	cpuLeaserMinOverhead = flag.Int("executor.cpu_leaser.min_overhead", 2, "Always ensure at least this many extra cpus are included in a lease")
	cpuLeaserCPUSet      = flag.String("executor.cpu_leaser.cpuset", "", "Manual override for the set of CPUs that may be leased. Ignored if empty. Ex. '0-1,3'")
	warnAboutLeaks       = flag.Bool("executor.cpu_leaser.warn_about_leaks", true, "If set, warn about leaked leases")
)

const (
	// MaxNumLeases configures a safety valve to prevent a memory leak if
	// leasers forget to close their leases.
	MaxNumLeases = 1000
)

// Compile-time check that cpuLeaser implements the interface.
var _ interfaces.CPULeaser = (*CPULeaser)(nil)

type CPULeaser struct {
	mu                 sync.Mutex
	cpus               []cpuInfo
	leases             []lease
	load               map[int]int
	physicalProcessors int
}

type cpuInfo struct {
	processor  int // cpuset id
	physicalID int // numa node
}

type lease struct {
	taskID   string
	cpus     []int
	location string // only set if *warnAboutLeaks is enabled
}

func toCPUInfos(processors []int, physicalID int) []cpuInfo {
	infos := make([]cpuInfo, len(processors))
	for i, p := range processors {
		infos[i] = cpuInfo{
			processor:  p,
			physicalID: physicalID,
		}
	}
	return infos
}

// Format formats a set of CPUs as a cpuset list-format compatible string.
// See https://man7.org/linux/man-pages/man7/cpuset.7.html for list-format.
func Format[I constraints.Integer](cpus []I) string {
	slices.Sort(cpus)
	cpuStrings := make([]string, len(cpus))
	for i, cpu := range cpus {
		cpuStrings[i] = strconv.Itoa(int(cpu))
	}
	return strings.Join(cpuStrings, ",")
}

func parseListFormat(s string) ([]cpuInfo, error) {
	// Example: "0-1,3" is parsed as []int{0, 1, 3}
	var nodes []cpuInfo
	var physicalID int
	nodeRanges := strings.Split(s, ",")
	for _, r := range nodeRanges {
		startStr, endStr, _ := strings.Cut(r, "-")
		if strings.Contains(startStr, ":") {
			var numaStr string
			numaStr, startStr, _ = strings.Cut(startStr, ":")
			numaID, err := strconv.Atoi(numaStr)
			if err != nil {
				return nodes, fmt.Errorf("malformed file contents")
			}
			physicalID = numaID
		}
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
			nodes = append(nodes, cpuInfo{
				processor:  node,
				physicalID: physicalID,
			})
		}
	}
	return nodes, nil
}

// Parse convers a cpuset list-format compatible string into a slice of
// processor ids (ints).
// See https://man7.org/linux/man-pages/man7/cpuset.7.html for list-format.
func Parse(s string) ([]int, error) {
	cpuInfos, err := parseListFormat(s)
	if err != nil {
		return nil, err
	}
	processors := make([]int, len(cpuInfos))
	for i, c := range cpuInfos {
		processors[i] = c.processor
	}
	return processors, nil
}

func NewLeaser() (*CPULeaser, error) {
	cl := &CPULeaser{
		leases: make([]lease, 0, MaxNumLeases),
		load:   make(map[int]int, 0),
	}

	var cpus []cpuInfo
	if *cpuLeaserCPUSet != "" {
		c, err := parseListFormat(*cpuLeaserCPUSet)
		if err != nil {
			return nil, err
		}
		cpus = c
	} else {
		cpus = GetCPUs()
	}

	cl.cpus = make([]cpuInfo, len(cpus))

	processors := make(map[int]struct{}, 0)
	for i, cpu := range cpus {
		cl.cpus[i] = cpu
		cl.load[cpu.processor] = 0
		processors[cpu.physicalID] = struct{}{}
	}
	cl.physicalProcessors = len(processors)
	log.Debugf("NewLeaser with %d processors and %d cores", cl.physicalProcessors, len(cl.cpus))
	return cl, nil
}

func computeNumCPUs(milliCPU int64, allowOverhead bool) int {
	rawNumCPUs := int(math.Ceil(float64(milliCPU) / 1000.0))
	if !allowOverhead {
		return rawNumCPUs
	}

	overheadCPUs := int(*cpuLeaserOverhead*float64(milliCPU)) / 1000
	if overheadCPUs < *cpuLeaserMinOverhead {
		overheadCPUs = *cpuLeaserMinOverhead
	}
	return rawNumCPUs + overheadCPUs
}

type Options struct {
	disableOverhead bool
}

type Option func(*Options)

func WithNoOverhead() Option {
	return func(o *Options) {
		o.disableOverhead = true
	}
}

// Acquire leases a set of CPUs (identified by index) for a task. The returned
// function should be called to free the CPUs when they are no longer used.
func (l *CPULeaser) Acquire(milliCPU int64, taskID string, opts ...any) (int, []int, func()) {
	l.mu.Lock()
	defer l.mu.Unlock()

	options := &Options{}
	for _, optI := range opts {
		if opt, ok := optI.(Option); ok {
			opt(options)
		}
	}

	numCPUs := computeNumCPUs(milliCPU, !options.disableOverhead)
	// If the CPU leaser is disabled; return all CPUs.
	if !*cpuLeaserEnable {
		numCPUs = len(l.cpus)
	}

	// Put all CPUs in a priority queue.
	pq := priority_queue.New[cpuInfo]()
	for _, cpuInfo := range l.cpus {
		numTasks := l.load[cpuInfo.processor]
		// we want the least loaded cpus first, so give the
		// cpus with more tasks a more negative score.
		pq.Push(cpuInfo, -1*numTasks)
	}

	// Get the set of CPUs, in order of load (incr).
	leastLoaded := pq.GetAll()

	// Find the numa node with the largest number of cores in the first
	// numCPUs CPUs.
	numaCount := make(map[int]int, l.physicalProcessors)
	for i := 0; i < numCPUs; i++ {
		c := leastLoaded[i]
		numaCount[c.physicalID]++
	}
	selectedNode := -1
	numCores := 0
	for numaNode, coreCount := range numaCount {
		if coreCount > numCores {
			selectedNode = numaNode
			numCores = coreCount
		}
	}

	// Now filter the set of CPUs to just the selected numaNode.
	leaseSet := make([]int, 0, numCPUs)
	for _, c := range leastLoaded {
		if c.physicalID != selectedNode {
			continue
		}
		leaseSet = append(leaseSet, c.processor)
		l.load[c.processor] += 1
		if len(leaseSet) == numCPUs {
			break
		}
	}

	// If the CPULeaser is enabled, actually track the lease.
	if *cpuLeaserEnable {
		if len(l.leases) >= MaxNumLeases {
			droppedLease := l.leases[0]
			l.leases = l.leases[1:]
			for _, processor := range droppedLease.cpus {
				l.load[processor] -= 1
			}
			if *warnAboutLeaks {
				alert.UnexpectedEvent("cpu_leaser_leak", "Acquire() handle leak at %s!", droppedLease.location)
			}
		}

		lease := lease{
			taskID: taskID,
			cpus:   leaseSet,
		}
		if *warnAboutLeaks {
			if _, file, no, ok := runtime.Caller(1); ok {
				lease.location = fmt.Sprintf("%s:%d", file, no)
			}
		}

		l.leases = append(l.leases, lease)
	}

	log.Debugf("Leased %s to task: %q (%d milliCPU)", Format(leaseSet), taskID, milliCPU)
	return selectedNode, leaseSet, func() {
		l.release(taskID)
	}
}

func (l *CPULeaser) release(taskID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var cpus []int
	l.leases = slices.DeleteFunc(l.leases, func(l lease) bool {
		if l.taskID == taskID {
			cpus = l.cpus
			return true
		}
		return false
	})

	for _, cpu := range cpus {
		l.load[cpu] -= 1
	}

	log.Debugf("Task: %q released CPUs", taskID)
}

func (l *CPULeaser) TestOnlyGetOpenLeases() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()

	taskCounts := make(map[string]int)
	for _, l := range l.leases {
		taskCounts[l.taskID] = len(l.cpus)
	}
	return taskCounts
}

func (l *CPULeaser) TestOnlyGetLoads() map[int]int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.load
}
