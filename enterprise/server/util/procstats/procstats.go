package procstats

import (
	"time"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ps "github.com/mitchellh/go-ps"
	procutil "github.com/shirou/gopsutil/v3/process"
)

const (
	// Parameters for stats polling. The poll interval starts out at
	// statsInitialPollInterval and is multiplied by statsPollBackoff after each
	// attempt, up to statsMaxPollInterval.
	statsInitialPollInterval = 25 * time.Millisecond
	statsPollBackoff         = 1.25
	statsMaxPollInterval     = 1000 * time.Millisecond
)

// Listener is a function that is called whenever new container stats are
// available. This can be used to live-update stats while a command is running.
type Listener func(*repb.UsageStats)

// Monitor polls resource usage of a process tree rooted at the given pid. The
// process identified by pid is expected to have been started before calling
// this function. The caller must close the given channel when the process is
// terminated. This function will unblock once the channel is closed, and it
// will return any stats collected.
func Monitor(pid int, listener Listener, processTerminated <-chan struct{}) *repb.UsageStats {
	ts := NewTreeStats(pid)
	// Most processes are short-lived so we need a fast poll rate if we want
	// to increase the probability of getting at least one sample. But
	// polling is expensive: a 50ms poll rate consumes around 10% of a CPU
	// on Linux (and it's even worse on macOS). So we take a hybrid approach
	// here: start off polling fast but slow down over time.
	pollInterval := statsInitialPollInterval
	for {
		select {
		case <-processTerminated:
			return ts.Total()
		case <-time.After(pollInterval):
			ts.Update() // ignore error
			listener(ts.Total())
		}
		pollInterval = time.Duration(float64(pollInterval) * statsPollBackoff)
		if pollInterval > statsMaxPollInterval {
			pollInterval = statsMaxPollInterval
		}
	}
}

// TreeStats records stats across the lifecycle of a process tree, accounting
// for processes that may come and go while the process is running.
type TreeStats struct {
	// RootPid is the pid of the root process of the tree.
	RootPid int

	// CurrentTotalMemoryBytes holds the last observed total memory usage across
	// all processes in the tree.
	CurrentTotalMemoryBytes int64

	// PeakTotalMemoryBytes records the highest total memory across all
	// processes in the tree, at any point during the process' lifetime.
	PeakTotalMemoryBytes int64

	// CPUNanosByPid records the most recent CPU stats for all pids observed in
	// the tree at any point during process execution.
	CPUNanosByPid map[int]int64
}

// New returns a new TreeStats tracking the resource usage of the process tree
// rooted at rootPid.
func NewTreeStats(rootPid int) *TreeStats {
	return &TreeStats{
		RootPid:       rootPid,
		CPUNanosByPid: make(map[int]int64, 1),
	}
}

// Update records stats for the current process tree.
func (t *TreeStats) Update() error {
	stats, err := statTree(t.RootPid)
	if err != nil {
		return err
	}
	t.CurrentTotalMemoryBytes = 0
	for _, s := range stats {
		t.CurrentTotalMemoryBytes += s.MemoryBytes
	}
	if t.CurrentTotalMemoryBytes > t.PeakTotalMemoryBytes {
		t.PeakTotalMemoryBytes = t.CurrentTotalMemoryBytes
	}
	for pid, stat := range stats {
		t.CPUNanosByPid[pid] = stat.CpuNanos
	}
	return nil
}

// Total returns stats with PeakMemoryBytes set to match the peak total memory
// observed, and CPUNanos to match the total CPU usage observed across all
// processes in the tree.
func (t *TreeStats) Total() *repb.UsageStats {
	totalCPUNanos := int64(0)
	for _, cpuNanos := range t.CPUNanosByPid {
		totalCPUNanos += cpuNanos
	}
	return &repb.UsageStats{
		PeakMemoryBytes: t.PeakTotalMemoryBytes,
		CpuNanos:        totalCPUNanos,
		MemoryBytes:     t.CurrentTotalMemoryBytes,
	}
}

func statTree(pid int) (map[int]*repb.UsageStats, error) {
	pids, err := pidsInTree(pid)
	if err != nil {
		return nil, err
	}
	stats := make(map[int]*repb.UsageStats, len(pids))
	for _, pid := range pids {
		s, err := getProcessStats(pid)
		if err != nil {
			// If we fail to get stats, the process probably just exited between the
			// time that we observed the PIDs and the tree, and the time we went to
			// read stats for the pid.
			continue
		}
		stats[pid] = s
	}
	return stats, nil
}

// pidsInTree returns all pids in the tree rooted at pid, including pid itself.
func pidsInTree(pid int) ([]int, error) {
	procs, err := ps.Processes()
	if err != nil {
		return nil, err
	}
	children := make(map[int][]int, len(procs))
	for _, p := range procs {
		ppid := p.PPid()
		c := children[ppid]
		c = append(c, p.Pid())
		children[ppid] = c
	}
	pidsVisited := []int{}
	pidsToExplore := []int{pid}
	for len(pidsToExplore) > 0 {
		pid := pidsToExplore[0]
		pidsToExplore = pidsToExplore[1:]
		pidsVisited = append(pidsVisited, pid)
		pidsToExplore = append(pidsToExplore, children[pid]...)
	}
	return pidsVisited, nil
}

func getProcessStats(pid int) (*repb.UsageStats, error) {
	p, err := procutil.NewProcess(int32(pid))
	if err != nil {
		return nil, err
	}
	t, err := p.Times()
	if err != nil {
		return nil, err
	}
	m, err := p.MemoryInfo()
	if err != nil {
		return nil, err
	}
	// TODO(bduffany): Explore using PSS instead of RSS to avoid overcounting
	// shared library memory usage.
	stats := &repb.UsageStats{
		MemoryBytes: int64(m.RSS),
		CpuNanos:    int64((t.User + t.System) * 1e9),
	}
	return stats, nil
}
