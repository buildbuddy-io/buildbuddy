package cpuset

import (
	"cmp"
	"fmt"
	"math"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/exp/constraints"
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

// LeasingEnabled returns whether CPU leasing is enabled via the
// executor.cpu_leaser.enable flag.
func LeasingEnabled() bool {
	return *cpuLeaserEnable
}

type CPULeaser struct {
	mu     sync.Mutex
	cpus   []CPUInfo
	leases []lease
	load   map[int]int
	// nodeCPUs is the number of leaseable CPUs on each NUMA node.
	nodeCPUs map[int]int
	// nodeMemoryBytes is the total memory of each NUMA node.
	nodeMemoryBytes map[int]int64
}

type CPUInfo struct {
	Processor int // cpuset id
	NumaNode  int // numa node
}

type lease struct {
	taskID   string
	cpus     []int
	location string // only set if *warnAboutLeaks is enabled
}

func toCPUInfos(processors []int, numaNode int) []CPUInfo {
	infos := make([]CPUInfo, len(processors))
	for i, p := range processors {
		infos[i] = CPUInfo{
			Processor: p,
			NumaNode:  numaNode,
		}
	}
	return infos
}

// Format formats a set of CPUs as a cpuset list-format compatible string.
// See https://man7.org/linux/man-pages/man7/cpuset.7.html for list-format.
func Format[I constraints.Integer](cpus ...I) string {
	cpuStrings := make([]string, len(cpus))
	for i, cpu := range slices.Sorted(slices.Values(cpus)) {
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
	nodeRanges := strings.SplitSeq(s, ",")
	for r := range nodeRanges {
		numaNode := -1
		if numaStr, rangeStr, ok := strings.Cut(r, ":"); ok {
			numaID, err := strconv.Atoi(numaStr)
			if err != nil {
				return nil, fmt.Errorf("invalid NUMA node %q", numaStr)
			}
			numaNode = numaID
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
				Processor: processor,
				NumaNode:  numaNode,
			})
		}
	}
	return cpus, nil
}

type LeaserOpts struct {
	// SystemCPUs is the set of available CPUs on the current system. If empty,
	// CPU information will be fetched from the OS.
	SystemCPUs []CPUInfo

	// NodeMemoryBytes is the total memory of each NUMA node on the current
	// system, keyed by node ID. Nodes that are missing have no memory. It is
	// only used if SystemCPUs is set; otherwise, memory information is
	// fetched from the OS along with CPU information.
	NodeMemoryBytes map[int]int64
}

func NewLeaser(opts LeaserOpts) (*CPULeaser, error) {
	systemCPUs := opts.SystemCPUs
	nodeMemoryBytes := opts.NodeMemoryBytes
	if len(systemCPUs) == 0 {
		c, err := GetCPUs()
		if err != nil {
			return nil, fmt.Errorf("get CPUs: %w", err)
		}
		systemCPUs = c
		m, err := getNodeMemoryBytes()
		if err != nil {
			return nil, fmt.Errorf("get NUMA node memory: %w", err)
		}
		nodeMemoryBytes = m
	}

	cl := &CPULeaser{
		leases:          make([]lease, 0, MaxNumLeases),
		load:            make(map[int]int, 0),
		nodeCPUs:        make(map[int]int, 0),
		nodeMemoryBytes: nodeMemoryBytes,
	}

	leaseableCPUs := systemCPUs
	if *cpuLeaserCPUSet != "" {
		c, err := parseCPUs(*cpuLeaserCPUSet)
		if err != nil {
			return nil, err
		}
		// Validate "NODE:" prefixes against system CPU information, or populate
		// them if they were omitted.
		sysCPUs := make(map[int]CPUInfo, len(systemCPUs))
		for _, cpu := range systemCPUs {
			sysCPUs[cpu.Processor] = cpu
		}
		for i := range c {
			cpu := &c[i]
			sysCPU, ok := sysCPUs[cpu.Processor]
			if !ok {
				return nil, fmt.Errorf("invalid CPU %d in cpuset flag: CPU does not exist on this machine", cpu.Processor)
			}
			if cpu.NumaNode == -1 {
				cpu.NumaNode = sysCPU.NumaNode
			} else {
				if sysCPU.NumaNode != cpu.NumaNode {
					return nil, fmt.Errorf("invalid node: %d for CPU %d: does not match OS-reported node: %d", cpu.NumaNode, cpu.Processor, sysCPU.NumaNode)
				}
			}
		}
		leaseableCPUs = c
	}

	cl.cpus = make([]CPUInfo, len(leaseableCPUs))

	for i, cpu := range leaseableCPUs {
		cl.cpus[i] = cpu
		cl.load[cpu.Processor] = 0
		cl.nodeCPUs[cpu.NumaNode]++
	}
	log.Debugf("NewLeaser with %d NUMA nodes and %d CPUs", len(cl.nodeCPUs), len(cl.cpus))
	return cl, nil
}

func computeNumCPUs(milliCPU int64, allowOverhead bool) int {
	rawNumCPUs := int(math.Ceil(float64(milliCPU) / 1000.0))
	if !allowOverhead {
		return rawNumCPUs
	}

	overheadCPUs := max(int(*cpuLeaserOverhead*float64(milliCPU))/1000, *cpuLeaserMinOverhead)
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

// Acquire leases a set of CPUs (identified by index) for a task with the given
// CPU and memory estimates, and returns the NUMA nodes whose memory the task
// may use. The returned function should be called to free the CPUs when they
// are no longer used.
func (l *CPULeaser) Acquire(milliCPU, memoryBytes int64, taskID string, opts ...any) ([]int, []int, func()) {
	l.mu.Lock()
	defer l.mu.Unlock()

	options := &Options{}
	for _, optI := range opts {
		if opt, ok := optI.(Option); ok {
			opt(options)
		}
	}

	// Overhead is extra room rather than a requirement, so never ask for more
	// CPUs than can be leased.
	numCPUs := min(computeNumCPUs(milliCPU, !options.disableOverhead), len(l.cpus))
	// If the CPU leaser is disabled; return all CPUs.
	if !*cpuLeaserEnable {
		numCPUs = len(l.cpus)
	}

	// Order the CPUs from least to most loaded. Among equally loaded CPUs, the
	// ones on less loaded nodes go first, so that equally loaded nodes take
	// turns receiving tasks instead of the first node filling up before the
	// others get any.
	nodeLoad := make(map[int]int, len(l.nodeCPUs))
	for _, c := range l.cpus {
		nodeLoad[c.NumaNode] += l.load[c.Processor]
	}
	leastLoaded := slices.Clone(l.cpus)
	slices.SortStableFunc(leastLoaded, func(a, b CPUInfo) int {
		return cmp.Or(l.load[a.Processor]-l.load[b.Processor], nodeLoad[a.NumaNode]-nodeLoad[b.NumaNode])
	})

	// Rank the NUMA nodes by how many of the first numCPUs least loaded CPUs
	// they hold. Ties go to the node with less total load, then to the lower
	// node ID.
	numaCount := make(map[int]int, len(l.nodeCPUs))
	for _, c := range leastLoaded[:numCPUs] {
		numaCount[c.NumaNode]++
	}
	rankedNodes := make([]int, 0, len(l.nodeCPUs))
	for node := range l.nodeCPUs {
		rankedNodes = append(rankedNodes, node)
	}
	slices.SortFunc(rankedNodes, func(a, b int) int {
		return cmp.Or(numaCount[b]-numaCount[a], nodeLoad[a]-nodeLoad[b], a-b)
	})

	// Pin the task to the highest ranked node that can hold its CPU and
	// memory estimates. Overhead CPUs are extra room rather than a
	// requirement, so they are only leased if they fit on the selected nodes.
	estimatedCPUs := computeNumCPUs(milliCPU, false)
	var selectedNodes []int
	for _, node := range rankedNodes {
		if l.nodeCPUs[node] >= estimatedCPUs && l.nodeMemoryBytes[node] >= memoryBytes {
			selectedNodes = []int{node}
			break
		}
	}
	// If no node can hold the task by itself, pin it to several nodes. The
	// nodes with the most CPUs go first, so that the task spans as few nodes
	// as possible when nodes have different numbers of leaseable CPUs.
	if selectedNodes == nil {
		nodesBySize := slices.Clone(rankedNodes)
		slices.SortStableFunc(nodesBySize, func(a, b int) int {
			return l.nodeCPUs[b] - l.nodeCPUs[a]
		})
		cpuCapacity, memoryCapacity := 0, int64(0)
		for _, node := range nodesBySize {
			selectedNodes = append(selectedNodes, node)
			cpuCapacity += l.nodeCPUs[node]
			memoryCapacity += l.nodeMemoryBytes[node]
			if cpuCapacity >= estimatedCPUs && memoryCapacity >= memoryBytes {
				break
			}
		}
	}

	// Lease the least loaded CPUs on the selected nodes.
	leaseSet := make([]int, 0, numCPUs)
	for _, c := range leastLoaded {
		if len(leaseSet) == numCPUs {
			break
		}
		if !slices.Contains(selectedNodes, c.NumaNode) {
			continue
		}
		leaseSet = append(leaseSet, c.Processor)
		l.load[c.Processor] += 1
	}

	// Track the lease even if the CPU leaser is disabled, because node
	// selection still depends on load, and load only goes down when a tracked
	// lease is released.
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

	// The kernel rejects cpuset.mems values that include nodes without memory,
	// so we only return the selected nodes that have memory.
	numaNodes := slices.DeleteFunc(selectedNodes, func(node int) bool {
		return l.nodeMemoryBytes[node] == 0
	})

	log.Debugf("Leased CPUs %s and memory on NUMA nodes %s to task: %q (%d milliCPU, %d memory bytes)", Format(leaseSet...), Format(numaNodes...), taskID, milliCPU, memoryBytes)
	return numaNodes, leaseSet, func() {
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
