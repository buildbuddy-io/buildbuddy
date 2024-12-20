package cpuset_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/cpuset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCPUSetAcquireRelease(t *testing.T) {
	cs, err := cpuset.NewLeaser(cpuset.WithTestOnlySetNumCPUs(4))
	require.NoError(t, err)
	cancel1, cpus1 := cs.Acquire(3)
	defer cancel1()
	assert.Equal(t, 3, len(cpus1))

	cancel2, cpus2 := cs.Acquire(1)
	defer cancel2()
	require.Equal(t, 1, len(cpus2))

	assert.NotContains(t, cpus1, cpus2[0])
}
