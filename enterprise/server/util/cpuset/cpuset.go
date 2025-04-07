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
	cpus               []CPUInfo
	leases             []lease
	load               map[int]int
	physicalProcessors int
}

type CPUInfo struct {
	Processor  int // cpuset id
	PhysicalID int // numa node
}

type lease struct {
	taskID   string
	cpus     []int
	location string // only set if *warnAboutLeaks is enabled
}

func toCPUInfos(processors []int, physicalID int) []CPUInfo {
	infos := make([]CPUInfo, len(processors))
	for i, p := range processors {
		infos[i] = CPUInfo{
			Processor:  p,
			PhysicalID: physicalID,
		}
	}
	return infos
}

// Format formats a set of CPUs as a cpuset list-format compatible string.
// See https://man7.org/linux/man-pages/man7/cpuset.7.html for list-format.
func Format[I constraints.Integer](cpus ...I) string {
	slices.Sort(cpus)
	cpuStrings := make([]string, len(cpus))
	for i, cpu := range cpus {
		cpuStrings[i] = strconv.Itoa(int(cpu))
	}
	return strings.Join(cpuStrings, ",")
}

// parseCPUs parses a list of CPUs from a comma-separated list of
// "NODE:CPU_RANGE" pairs where the "NODE:" prefix is optional, and CPU_RANGE
// can either be a single CPU index or a range of CPUs like "0-127".
//
// Examples:
// - Single CPU: "0"
// - Multiple CPUs: "0,1,2"
// - CPU range: "0-127"
// - CPU range with node: "0:0-127"
// - Multiple CPUs or CPU ranges, with optional nodes: "0:0-127,128-255,256"
//
// If the NODE prefix is omitted, -1 is returned, and the caller is responsible
// for mapping these processor IDs to physical IDs.
func parseCPUs(s string) ([]CPUInfo, error) {
	var cpus []CPUInfo
	nodeRanges := strings.Split(s, ",")
	for _, r := range nodeRanges {
		physicalID := -1
		if numaStr, rangeStr, ok := strings.Cut(r, ":"); ok {
			numaID, err := strconv.Atoi(numaStr)
			if err != nil {
				return nil, fmt.Errorf("invalid NUMA node %q", numaStr)
			}
			physicalID = numaID
			r = rangeStr
		}
		startStr, endStr, isRange := strings.Cut(r, "-")
		start, err := strconv.Atoi(startStr)
		if err != nil {
			if isRange {
				return nil, fmt.Errorf("invalid CPU range start index %q", startStr)
			}
			return nil, fmt.Errorf("invalid CPU index %q", startStr)
		}
		end := start
		if isRange {
			n, err := strconv.Atoi(endStr)
			if err != nil {
				return nil, fmt.Errorf("invalid CPU range end index %q", endStr)
			}
			if n < start {
				return nil, fmt.Errorf("invalid CPU range end index %d: must exceed start index %d", n, start)
			}
			end = n
		}
		for processor := start; processor <= end; processor++ {
			cpus = append(cpus, CPUInfo{
				Processor:  processor,
				PhysicalID: physicalID,
			})
		}
	}
	return cpus, nil
}

type LeaserOpts struct {
	// SystemCPUs is the set of available CPUs on the current system. If empty,
	// CPU information will be fetched from the OS.
	SystemCPUs []CPUInfo
}

func NewLeaser(opts LeaserOpts) (*CPULeaser, error) {
	systemCPUs := opts.SystemCPUs
	if len(systemCPUs) == 0 {
		c, err := GetCPUs()
		if err != nil {
			return nil, fmt.Errorf("get CPUs: %w", err)
		}
		systemCPUs = c
	}

	cl := &CPULeaser{
		leases: make([]lease, 0, MaxNumLeases),
		load:   make(map[int]int, 0),
	}

	leaseableCPUs := systemCPUs
	if *cpuLeaserCPUSet != "" {
		c, err := parseCPUs(*cpuLeaserCPUSet)
		if err != nil {
			return nil, err
		}
		// Validate "NODE:" prefixes against system CPU information, or populate
		// them if they were omitted.
		physicalIDs := make(map[int]int, len(systemCPUs))
		for _, cpu := range systemCPUs {
			physicalIDs[cpu.Processor] = cpu.PhysicalID
		}
		for i := range c {
			cpu := &c[i]
			if cpu.PhysicalID == -1 {
				cpu.PhysicalID = physicalIDs[cpu.Processor]
			} else {
				if physicalIDs[cpu.Processor] != cpu.PhysicalID {
					return nil, fmt.Errorf("invalid node ID %d for CPU %d: does not match OS-reported ID %d", cpu.PhysicalID, cpu.Processor, physicalIDs[cpu.Processor])
				}
			}
		}
		leaseableCPUs = c
	}

	cl.cpus = make([]CPUInfo, len(leaseableCPUs))

	processors := make(map[int]struct{}, 0)
	for i, cpu := range leaseableCPUs {
		cl.cpus[i] = cpu
		cl.load[cpu.Processor] = 0
		processors[cpu.PhysicalID] = struct{}{}
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
	pq := priority_queue.New[CPUInfo]()
	for _, cpuInfo := range l.cpus {
		numTasks := l.load[cpuInfo.Processor]
		// we want the least loaded cpus first, so give the
		// cpus with more tasks a more negative score.
		pq.Push(cpuInfo, float64(-1*numTasks))
	}

	// Get the set of CPUs, in order of load (incr).
	leastLoaded := pq.GetAll()

	// Find the numa node with the largest number of cores in the first
	// numCPUs CPUs.
	numaCount := make(map[int]int, l.physicalProcessors)
	for i := 0; i < numCPUs; i++ {
		c := leastLoaded[i]
		numaCount[c.PhysicalID]++
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
		if c.PhysicalID != selectedNode {
			continue
		}
		leaseSet = append(leaseSet, c.Processor)
		l.load[c.Processor] += 1
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

	log.Debugf("Leased %s to task: %q (%d milliCPU)", Format(leaseSet...), taskID, milliCPU)
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
