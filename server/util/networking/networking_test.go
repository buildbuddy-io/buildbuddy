package networking_test

import (
	"math/rand"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostNetAllocator(t *testing.T) {
	const n = 1000
	a := &networking.HostNetAllocator{}
	var nets [n]*networking.HostNet
	var err error
	uniqueCIDRs := map[string]struct{}{}

	// Reserve all possible CIDRs
	for vmIdx := 0; vmIdx < n; vmIdx++ {
		nets[vmIdx], err = a.Get(vmIdx)
		require.NoError(t, err, "Get(%d)", vmIdx)
		uniqueCIDRs[nets[vmIdx].String()] = struct{}{}
	}

	// All CIDRs should be unique
	require.Equal(t, n, len(uniqueCIDRs))

	// Spot check some CIDRs for expected values
	assert.Equal(t, "192.168.0.5/30", nets[0].String())
	assert.Equal(t, "192.168.0.13/30", nets[1].String())
	assert.Equal(t, "192.168.33.69/30", nets[n-2].String())
	assert.Equal(t, "192.168.33.77/30", nets[n-1].String())

	// Attempting to get a new host net should now fail
	for vmIdx := 0; vmIdx < n; vmIdx++ {
		net, err := a.Get(vmIdx)
		require.Error(t, err)
		require.Nil(t, net)
	}

	// Unlock an arbitrary network - subsequent Get() for any index should then
	// return the newly unlocked address
	vmIdx := rand.Intn(n)
	unlockedAddr := nets[vmIdx].String()
	nets[vmIdx].Unlock()

	net, err := a.Get(rand.Intn(n))
	require.NoError(t, err)
	require.Equal(t, unlockedAddr, net.String())
}
