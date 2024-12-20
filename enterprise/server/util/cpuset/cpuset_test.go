package cpuset_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCPUSetAcquireRelease(t *testing.T) {
	cs, err := cpuset.NewLeaser(cpuset.WithTestOnlySetNumCPUs(4))
	require.NoError(t, err)
	task1 := uuid.New()
	cpus1, cancel1 := cs.Acquire(3, task1)
	defer cancel1()
	assert.Equal(t, 3, len(cpus1))

	task2 := uuid.New()
	cpus2, cancel2 := cs.Acquire(1, task2)
	defer cancel2()
	require.Equal(t, 1, len(cpus2))

	assert.NotContains(t, cpus1, cpus2[0])
}

func TestEvenDistributionUnderLoad(t *testing.T) {
	cs, err := cpuset.NewLeaser(cpuset.WithTestOnlySetNumCPUs(4))
	require.NoError(t, err)

	counts := make([]int, 4)
	for i := 0; i < 100; i++ {
		task := uuid.New()
		cpus, cancel := cs.Acquire(3, task)
		for _, cpu := range cpus {
			counts[cpu] += 1
		}
		// Cancel when the test is over.
		defer cancel()
	}
	for _, count := range counts {
		assert.Equal(t, 75, count)
	}
}
