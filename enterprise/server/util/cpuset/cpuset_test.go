package cpuset_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCPUs(t *testing.T) {
	for _, test := range []struct {
		name          string
		input         string      // cpuset spec for CPUs returned by getTestCPUs
		expectedCPUs  map[int]int // expected CPU => node mapping
		expectedError string
	}{
		{
			name:         "single CPU",
			input:        "0",
			expectedCPUs: map[int]int{0: 0},
		},
		{
			name:         "multiple CPUs",
			input:        "0,1",
			expectedCPUs: map[int]int{0: 0, 1: 0},
		},
		{
			name:         "CPU range",
			input:        "0-2",
			expectedCPUs: map[int]int{0: 0, 1: 0, 2: 0},
		},
		{
			name:         "multiple CPUs and ranges",
			input:        "0,1-2,3",
			expectedCPUs: map[int]int{0: 0, 1: 0, 2: 0, 3: 0},
		},
		{
			name:         "multiple nodes and ranges with nodes",
			input:        "0:0,1:128-129,1",
			expectedCPUs: map[int]int{0: 0, 128: 1, 129: 1, 1: 0},
		},
		{
			name:          "invalid CPU",
			input:         "invalid",
			expectedError: `invalid CPU index "invalid"`,
		},
		{
			name:          "valid node with invalid CPU",
			input:         "0:invalid",
			expectedError: `invalid CPU index "invalid"`,
		},
		{
			name:          "invalid CPU range start",
			input:         "0:invalid-1",
			expectedError: `invalid CPU range start index "invalid"`,
		},
		{
			name:          "invalid CPU range end",
			input:         "0:0-invalid",
			expectedError: `invalid CPU range end index "invalid"`,
		},
		{
			name:          "CPU range end less than start",
			input:         "0:1-0",
			expectedError: `invalid CPU range end index 0: must exceed start index 1`,
		},
		{
			name:          "missing CPU range end",
			input:         "0:0-",
			expectedError: `invalid CPU range end index ""`,
		},
		{
			name:          "invalid node",
			input:         "1:0",
			expectedError: `invalid node: 1 for CPU 0: does not match OS-reported node: 0`,
		},
		{
			name:          "invalid CPU ID",
			input:         "0:600",
			expectedError: `invalid CPU 600 in cpuset flag: CPU does not exist on this machine`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "executor.cpu_leaser.enable", true)
			flags.Set(t, "executor.cpu_leaser.cpuset", test.input)
			cs, err := cpuset.NewLeaser(getTestLeaserOpts())
			if test.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectedError)
				return
			}

			// Lease 1 more than the number of CPUs we're expecting,
			// and make sure all the returned CPUs are in the expected set.
			actualCPUs := map[int]int{}
			for i := 0; i < len(test.expectedCPUs)+1; i++ {
				task := uuid.New()
				nodes, cpus, cancel := cs.Acquire(1000, 0, task, cpuset.WithNoOverhead())
				defer cancel()
				require.Len(t, nodes, 1)
				for _, cpu := range cpus {
					actualCPUs[cpu] = nodes[0]
				}
			}
			assert.Equal(t, test.expectedCPUs, actualCPUs)
		})
	}
}

func TestCPUSetAcquireRelease(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)
	task1 := uuid.New()
	_, cpus1, cancel1 := cs.Acquire(3000, 0, task1)
	defer cancel1()
	assert.Equal(t, 3, len(cpus1))

	task2 := uuid.New()
	_, cpus2, cancel2 := cs.Acquire(1000, 0, task2)
	defer cancel2()
	require.Equal(t, 1, len(cpus2))

	assert.NotContains(t, cpus1, cpus2[0])
}

func TestEvenDistributionUnderLoad(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)

	counts := make(map[int]int, 4)
	for range 100 {
		task := uuid.New()
		_, cpus, cancel := cs.Acquire(1000, 0, task)
		for _, cpu := range cpus {
			counts[cpu] += 1
		}
		// Cancel when the test is over.
		defer cancel()
	}
	for _, count := range counts {
		assert.Equal(t, 25, count)
	}
}

func TestLeastLoadedCPUsAreSelected(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-2")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)

	// Acquire and hold a single-CPU lease on each of the three CPUs.
	cancels := make(map[int]func(), 3)
	for range 3 {
		_, cpus, cancel := cs.Acquire(1000, 0, uuid.New())
		defer cancel()
		require.Len(t, cpus, 1)
		cancels[cpus[0]] = cancel
	}
	require.Len(t, cancels, 3, "expected the held leases to land on 3 distinct CPUs")

	// Acquire and hold another single-CPU lease. The CPU it lands on now
	// holds two leases.
	_, cpus, cancel := cs.Acquire(1000, 0, uuid.New())
	defer cancel()
	require.Len(t, cpus, 1)
	doubleLoaded := cpus[0]

	// Release one of the initial leases on a CPU other than the double-loaded
	// one, leaving that CPU with no load. The CPU loads are now 2, 1, and 0.
	for cpu := range 3 {
		if cpu != doubleLoaded {
			cancels[cpu]()
			break
		}
	}

	// Acquire two CPUs. The CPUs with loads 0 and 1 should be selected, and
	// the CPU with two leases should be avoided.
	var leastLoaded []int
	for cpu := range 3 {
		if cpu != doubleLoaded {
			leastLoaded = append(leastLoaded, cpu)
		}
	}
	_, cpus, cancel = cs.Acquire(2000, 0, uuid.New())
	defer cancel()
	require.ElementsMatch(t, leastLoaded, cpus)
}

func TestCPUSetOverhead(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", .20)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 2)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-47")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)
	task1 := uuid.New()
	_, cpus1, cancel1 := cs.Acquire(1100, 0, task1)
	defer cancel1()
	assert.Equal(t, 4, len(cpus1))

	task2 := uuid.New()
	_, cpus2, cancel2 := cs.Acquire(1800, 0, task2)
	defer cancel2()
	require.Equal(t, 4, len(cpus2))

	assert.NotContains(t, cpus1, cpus2[0])

	task3 := uuid.New()
	_, cpus3, cancel3 := cs.Acquire(20_000, 0, task3)
	defer cancel3()
	require.Equal(t, 24, len(cpus3))

	// Test that setting NoOverhead also works correctly, even when overhead
	// is configured.
	task4 := uuid.New()
	_, cpus4, cancel4 := cs.Acquire(3000, 0, task4, cpuset.WithNoOverhead())
	defer cancel4()
	require.Equal(t, 3, len(cpus4))
}

func TestCPUSetRequestExceedsLeaseableCPUs(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", .20)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 2)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-31")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		milliCPU int64
		opts     []any
	}{
		{name: "request plus overhead exceeds CPUs", milliCPU: 30_000},
		{name: "request equals CPUs", milliCPU: 32_000},
		{name: "request exceeds CPUs", milliCPU: 40_000},
		{name: "request equals CPUs without overhead", milliCPU: 32_000, opts: []any{cpuset.WithNoOverhead()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cpus, cancel := cs.Acquire(tc.milliCPU, 0, uuid.New(), tc.opts...)
			defer cancel()
			assert.Len(t, cpus, 32)
		})
	}
}

func TestCPUSetRequestExceedsLeaseableCPUsMultiNuma(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", .20)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 2)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:128-133")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)

	// The estimate doesn't fit on either node alone, so the lease is pinned to
	// both nodes.
	numa, cpus, cancel := cs.Acquire(10_000, 0, uuid.New())
	defer cancel()
	assert.ElementsMatch(t, []int{0, 1}, numa)
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 128, 129, 130, 131, 132, 133}, cpus)
}

func TestCPUSetDisabled(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", false)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-47")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)
	task1 := uuid.New()
	numa1, cpus1, cancel1 := cs.Acquire(1100, 0, task1)
	defer cancel1()

	assert.Equal(t, []int{0}, numa1)
	assert.Equal(t, 48, len(cpus1))
}

func TestCPUSetDisabledNumaBalancing(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", false)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:130-133")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)
	nodeFrequency := make(map[int]int, 0)
	for range 100 {
		task := uuid.New()
		numa, _, cancel := cs.Acquire(1100, 0, task)
		defer cancel()
		require.Len(t, numa, 1)
		nodeFrequency[numa[0]]++
	}
	assert.Equal(t, map[int]int{0: 50, 1: 50}, nodeFrequency)
}

func TestCPUSetDisabledReleasedLeasesFreeNodes(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", false)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:130-133")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)

	// While a long task holds a node, short tasks that run one after another
	// should all go to the other node.
	busyNodes, _, cancel := cs.Acquire(1100, 0, uuid.New())
	defer cancel()
	require.Len(t, busyNodes, 1)
	for range 10 {
		nodes, _, cancel := cs.Acquire(1100, 0, uuid.New())
		require.Len(t, nodes, 1)
		require.NotEqual(t, busyNodes[0], nodes[0])
		cancel()
	}
}

func TestCPUSetDisabledManualCPUSet(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", false)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-1,3")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)
	task1 := uuid.New()
	numa1, cpus1, cancel1 := cs.Acquire(1100, 0, task1)
	defer cancel1()
	assert.Equal(t, []int{0}, numa1)
	// Should return all configured CPUs.
	assert.Equal(t, []int{0, 1, 3}, cpus1)
}

func TestNumaNodeFairness(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-1,1:128-131,0:2-3")

	var allCPUs []int

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)
	task1 := uuid.New()
	_, cpus1, cancel1 := cs.Acquire(4000, 0, task1)
	defer cancel1()
	allCPUs = append(allCPUs, cpus1...)

	task2 := uuid.New()
	_, cpus2, cancel2 := cs.Acquire(4000, 0, task2)
	defer cancel2()
	allCPUs = append(allCPUs, cpus2...)

	assert.ElementsMatch(t, []int{0, 1, 2, 3, 128, 129, 130, 131}, allCPUs)

	task3 := uuid.New()
	_, cpus3, cancel3 := cs.Acquire(8000, 0, task3)
	defer cancel3()
	assert.Len(t, cpus3, 8)
}

func TestNumaNodePinning(t *testing.T) {
	nodeCPUs := map[int][]int{
		0: {0, 1, 2, 3},
		1: {128, 129, 130, 131},
	}
	for _, test := range []struct {
		name             string
		leaserEnabled    bool
		minOverhead      int
		milliCPU         int64
		memoryBytes      int64
		expectedNumNodes int
		expectedNumCPUs  int
	}{
		{
			name:             "estimate fits on a node",
			leaserEnabled:    true,
			milliCPU:         4000,
			expectedNumNodes: 1,
			expectedNumCPUs:  4,
		},
		{
			name:             "estimate larger than a node",
			leaserEnabled:    true,
			milliCPU:         5000,
			expectedNumNodes: 2,
			expectedNumCPUs:  5,
		},
		{
			name:          "estimate fits on a node but overhead doesn't",
			leaserEnabled: true,
			// The task is pinned to one node because its 3 CPU estimate fits
			// there, and the lease is cut to the node's 4 CPUs instead of the
			// 5 CPUs that include overhead.
			minOverhead:      2,
			milliCPU:         3000,
			expectedNumNodes: 1,
			expectedNumCPUs:  4,
		},
		{
			name:          "memory estimate larger than a node",
			leaserEnabled: true,
			milliCPU:      1000,
			// Each node has 128 GiB of memory.
			memoryBytes:      129 << 30,
			expectedNumNodes: 2,
			expectedNumCPUs:  1,
		},
		{
			name:             "no CPU estimate",
			leaserEnabled:    true,
			milliCPU:         0,
			expectedNumNodes: 1,
			expectedNumCPUs:  0,
		},
		{
			name:             "estimate larger than the machine",
			leaserEnabled:    true,
			milliCPU:         16000,
			expectedNumNodes: 2,
			expectedNumCPUs:  8,
		},
		{
			name:             "leaser disabled, estimate fits on a node",
			milliCPU:         1000,
			expectedNumNodes: 1,
			expectedNumCPUs:  4,
		},
		{
			name:             "leaser disabled, estimate larger than a node",
			milliCPU:         5000,
			expectedNumNodes: 2,
			expectedNumCPUs:  8,
		},
		{
			name:             "leaser disabled, memory estimate larger than a node",
			milliCPU:         1000,
			memoryBytes:      129 << 30,
			expectedNumNodes: 2,
			expectedNumCPUs:  8,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "executor.cpu_leaser.enable", test.leaserEnabled)
			flags.Set(t, "executor.cpu_leaser.overhead", 0)
			flags.Set(t, "executor.cpu_leaser.min_overhead", test.minOverhead)
			flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:128-131")
			cs, err := cpuset.NewLeaser(getTestLeaserOpts())
			require.NoError(t, err)

			nodes, cpus, cancel := cs.Acquire(test.milliCPU, test.memoryBytes, uuid.New())
			defer cancel()

			require.Len(t, nodes, test.expectedNumNodes)
			require.Len(t, cpus, test.expectedNumCPUs)
			var pinnedCPUs []int
			for _, node := range nodes {
				require.Contains(t, nodeCPUs, node)
				pinnedCPUs = append(pinnedCPUs, nodeCPUs[node]...)
			}
			require.Subset(t, pinnedCPUs, cpus)
		})
	}
}

func TestPinToFewestLeastLoadedNodes(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)

	// Three nodes with four CPUs each.
	var systemCPUs []cpuset.CPUInfo
	for i := range 12 {
		systemCPUs = append(systemCPUs, cpuset.CPUInfo{Processor: i, NumaNode: i / 4})
	}
	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{
		SystemCPUs:      systemCPUs,
		NodeMemoryBytes: map[int]int64{0: 128 << 30, 1: 128 << 30, 2: 128 << 30},
	})
	require.NoError(t, err)

	// Hold a lease that fills a node.
	nodes, cpus, cancel := cs.Acquire(4000, 0, uuid.New())
	defer cancel()
	require.Len(t, nodes, 1)
	require.Len(t, cpus, 4)
	loadedNode := nodes[0]

	// A 5 CPU task doesn't fit on a single node, so it is pinned to the two
	// idle nodes rather than the loaded one.
	nodes, cpus, cancel = cs.Acquire(5000, 0, uuid.New())
	defer cancel()
	require.Len(t, nodes, 2)
	require.NotContains(t, nodes, loadedNode)
	require.Len(t, cpus, 5)
	for _, cpu := range cpus {
		require.NotEqual(t, loadedNode, cpu/4)
	}
}

func TestPinToFewestNodesWhenNodeSizesDiffer(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)

	// Nodes 0 and 1 have four CPUs each, and nodes 2 and 3 have eight CPUs
	// each.
	var systemCPUs []cpuset.CPUInfo
	for node, numCPUs := range []int{4, 4, 8, 8} {
		for range numCPUs {
			systemCPUs = append(systemCPUs, cpuset.CPUInfo{Processor: len(systemCPUs), NumaNode: node})
		}
	}
	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{
		SystemCPUs:      systemCPUs,
		NodeMemoryBytes: map[int]int64{0: 128 << 30, 1: 128 << 30, 2: 128 << 30, 3: 128 << 30},
	})
	require.NoError(t, err)

	nodes, cpus, cancel := cs.Acquire(10_000, 0, uuid.New())
	defer cancel()
	assert.ElementsMatch(t, []int{2, 3}, nodes)
	assert.Len(t, cpus, 10)
}

func TestSmallTasksAlternateNumaNodes(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-7,1:128-135")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)

	var nodes []int
	for range 4 {
		n, _, cancel := cs.Acquire(2000, 0, uuid.New())
		defer cancel()
		require.Len(t, n, 1)
		nodes = append(nodes, n[0])
	}
	assert.Equal(t, []int{0, 1, 0, 1}, nodes)
}

func TestNumaNodesWithoutMemoryAreNotReturned(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)

	// Two nodes with four CPUs each, where only node 1 has memory.
	var systemCPUs []cpuset.CPUInfo
	for i := range 8 {
		systemCPUs = append(systemCPUs, cpuset.CPUInfo{Processor: i, NumaNode: i / 4})
	}
	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{
		SystemCPUs:      systemCPUs,
		NodeMemoryBytes: map[int]int64{1: 128 << 30},
	})
	require.NoError(t, err)

	nodes, cpus, cancel := cs.Acquire(8000, 0, uuid.New())
	defer cancel()
	assert.Equal(t, []int{1}, nodes)
	assert.Len(t, cpus, 8)
}

func TestNumaNodesAreNotSplit(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:128-131")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)
	task1 := uuid.New()
	_, cpus1, cancel1 := cs.Acquire(4000, 0, task1)
	defer cancel1()
	assert.Equal(t, 4, len(cpus1))
}

func TestNonContiguousNumaNodes(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:128-131")

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)

	task1 := uuid.New()
	numa1, cpus1, cancel1 := cs.Acquire(4000, 0, task1)
	defer cancel1()

	task2 := uuid.New()
	numa2, cpus2, cancel2 := cs.Acquire(4000, 0, task2)
	defer cancel2()

	assert.NotElementsMatch(t, cpus1, cpus2)
	assert.NotEqual(t, numa1, numa2)
}

func TestMaxNumberOfLeases(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0")
	// Don't log warnings to avoid spamming test logs.
	flags.Set(t, "executor.cpu_leaser.warn_about_leaks", false)

	cs, err := cpuset.NewLeaser(getTestLeaserOpts())
	require.NoError(t, err)

	numLeases := cpuset.MaxNumLeases * 3
	taskIDs := make([]string, numLeases)
	cancels := make([]func(), numLeases)
	for i := range numLeases {
		task := uuid.New()
		_, _, cancel := cs.Acquire(1000, 0, task)
		taskIDs[i] = task
		cancels[i] = cancel
	}

	require.Equal(t, cpuset.MaxNumLeases, len(cs.TestOnlyGetOpenLeases()))
	for _, cancel := range cancels {
		cancel()
	}

	// There should be no open leases, and no active load after
	// cancelling all open leases.
	require.Equal(t, 0, len(cs.TestOnlyGetOpenLeases()))
	for _, load := range cs.TestOnlyGetLoads() {
		require.Equal(t, 0, load)
	}
}

// Returns test CPU and memory info:
//
// 0-127: physical node 0
// 128-255: physical node 1
// 256-383: physical node 0
// 384-511: physical node 1
//
// Each node has 128 GiB of memory.
func getTestLeaserOpts() cpuset.LeaserOpts {
	var cpus []cpuset.CPUInfo
	for i := range 512 {
		cpus = append(cpus, cpuset.CPUInfo{
			Processor: i,
			NumaNode:  (i / 128) % 2,
		})
	}
	return cpuset.LeaserOpts{
		SystemCPUs:      cpus,
		NodeMemoryBytes: map[int]int64{0: 128 << 30, 1: 128 << 30},
	}
}
