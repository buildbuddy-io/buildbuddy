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
			expectedError: `invalid node ID 1 for CPU 0: does not match OS-reported ID 0`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "executor.cpu_leaser.enable", true)
			flags.Set(t, "executor.cpu_leaser.cpuset", test.input)
			cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
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
				node, cpus, cancel := cs.Acquire(1000, task, cpuset.WithNoOverhead())
				defer cancel()
				for _, cpu := range cpus {
					actualCPUs[cpu] = node
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

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)
	task1 := uuid.New()
	_, cpus1, cancel1 := cs.Acquire(3000, task1)
	defer cancel1()
	assert.Equal(t, 3, len(cpus1))

	task2 := uuid.New()
	_, cpus2, cancel2 := cs.Acquire(1000, task2)
	defer cancel2()
	require.Equal(t, 1, len(cpus2))

	assert.NotContains(t, cpus1, cpus2[0])
}

func TestEvenDistributionUnderLoad(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3")

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)

	counts := make(map[int]int, 4)
	for i := 0; i < 100; i++ {
		task := uuid.New()
		_, cpus, cancel := cs.Acquire(1000, task)
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

func TestCPUSetOverhead(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", .20)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 2)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-47")

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)
	task1 := uuid.New()
	_, cpus1, cancel1 := cs.Acquire(1100, task1)
	defer cancel1()
	assert.Equal(t, 4, len(cpus1))

	task2 := uuid.New()
	_, cpus2, cancel2 := cs.Acquire(1800, task2)
	defer cancel2()
	require.Equal(t, 4, len(cpus2))

	assert.NotContains(t, cpus1, cpus2[0])

	task3 := uuid.New()
	_, cpus3, cancel3 := cs.Acquire(20_000, task3)
	defer cancel3()
	require.Equal(t, 24, len(cpus3))

	// Test that setting NoOverhead also works correctly, even when overhead
	// is configured.
	task4 := uuid.New()
	_, cpus4, cancel4 := cs.Acquire(3000, task4, cpuset.WithNoOverhead())
	defer cancel4()
	require.Equal(t, 3, len(cpus4))
}

func TestCPUSetDisabled(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", false)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-47")

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)
	task1 := uuid.New()
	numa1, cpus1, cancel1 := cs.Acquire(1100, task1)
	defer cancel1()

	assert.Equal(t, 0, numa1)
	assert.Equal(t, 48, len(cpus1))
}

func TestCPUSetDisabledNumaBalancing(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", false)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:130-133")

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)
	nodeFrequency := make(map[int]int, 0)
	for i := 0; i < 1000; i++ {
		task := uuid.New()
		numa, _, cancel := cs.Acquire(1100, task)
		nodeFrequency[numa]++
		cancel()
	}
	assert.Equal(t, 2, len(nodeFrequency))
	// TODO: figure out why the node frequencies are sometimes slightly
	// different, e.g. 498/502 instead of an even 500/500 split.
	assert.InDelta(t, nodeFrequency[0], nodeFrequency[1], 4)
}

func TestCPUSetDisabledManualCPUSet(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", false)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-1,3")

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)
	task1 := uuid.New()
	numa1, cpus1, cancel1 := cs.Acquire(1100, task1)
	defer cancel1()
	assert.Equal(t, 0, numa1)
	// Should return all configured CPUs.
	assert.Equal(t, []int{0, 1, 3}, cpus1)
}

func TestNumaNodeFairness(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-1,1:128-131,0:2-3")

	var allCPUs []int

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)
	task1 := uuid.New()
	_, cpus1, cancel1 := cs.Acquire(4000, task1)
	defer cancel1()
	allCPUs = append(allCPUs, cpus1...)

	task2 := uuid.New()
	_, cpus2, cancel2 := cs.Acquire(4000, task2)
	defer cancel2()
	allCPUs = append(allCPUs, cpus2...)

	assert.ElementsMatch(t, []int{0, 1, 2, 3, 128, 129, 130, 131}, allCPUs)

	task3 := uuid.New()
	_, cpus3, cancel3 := cs.Acquire(8000, task3)
	defer cancel3()
	assert.Equal(t, len(cpus3), 4)
}

func TestNumaNodesAreNotSplit(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:128-131")

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)
	task1 := uuid.New()
	_, cpus1, cancel1 := cs.Acquire(4000, task1)
	defer cancel1()
	assert.Equal(t, 4, len(cpus1))
}

func TestNonContiguousNumaNodes(t *testing.T) {
	flags.Set(t, "executor.cpu_leaser.enable", true)
	flags.Set(t, "executor.cpu_leaser.overhead", 0)
	flags.Set(t, "executor.cpu_leaser.min_overhead", 0)
	flags.Set(t, "executor.cpu_leaser.cpuset", "0:0-3,1:128-131")

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)

	task1 := uuid.New()
	numa1, cpus1, cancel1 := cs.Acquire(4000, task1)
	defer cancel1()

	task2 := uuid.New()
	numa2, cpus2, cancel2 := cs.Acquire(4000, task2)
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

	cs, err := cpuset.NewLeaser(cpuset.LeaserOpts{SystemCPUs: getTestCPUs()})
	require.NoError(t, err)

	numLeases := cpuset.MaxNumLeases * 3
	taskIDs := make([]string, numLeases)
	cancels := make([]func(), numLeases)
	for i := 0; i < numLeases; i++ {
		task := uuid.New()
		_, _, cancel := cs.Acquire(1000, task)
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

// Returns test CPU info:
//
// 0-127: physical node 0
// 128-255: physical node 1
// 256-383: physical node 0
// 384-511: physical node 1
func getTestCPUs() []cpuset.CPUInfo {
	var out []cpuset.CPUInfo
	for i := 0; i < 512; i++ {
		out = append(out, cpuset.CPUInfo{
			Processor:  i,
			PhysicalID: (i / 128) % 2,
		})
	}
	return out
}
