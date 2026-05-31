package grpc_forward

import (
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func cleanup() {
	for _, pool := range backendConnectionPools {
		pool.Close()
	}
	backendConnectionPools = nil
}

func TestGetConnectionPool_DedupesConcurrentDialsForSameTarget(t *testing.T) {
	t.Cleanup(cleanup)

	dial := func(_ string, _ ...grpc.DialOption) (*grpc_client.ClientConnPool, error) {
		// Sleep for a bit to increase the chance of race conditions.
		time.Sleep(10 * time.Millisecond)
		return &grpc_client.ClientConnPool{}, nil
	}

	pools := make([]*grpc_client.ClientConnPool, 10)
	var wg sync.WaitGroup
	for i := range pools {
		wg.Go(func() {
			p, err := getConnectionPool(dial, "remote.example.com")
			require.NotNil(t, p)
			require.NoError(t, err)
			pools[i] = p
		})
	}
	wg.Wait()

	require.NotNil(t, pools[0])
	for _, pool := range pools[1:] {
		require.Same(t, pools[0], pool)
	}
}
