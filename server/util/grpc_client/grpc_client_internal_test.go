package grpc_client

import (
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

// testPool builds a pool of connections with the given in-flight counts. The
// connections have no underlying grpc.ClientConn, which is fine because getConn
// only reads the pending counter and index.
func testPool(pending ...int64) *ClientConnPool {
	conns := make([]*clientConn, len(pending))
	for i, n := range pending {
		c := &clientConn{index: strconv.Itoa(i)}
		c.pending.Store(n)
		conns[i] = c
	}
	return &ClientConnPool{conns: conns}
}

func TestGetConn_LeastPending_PicksLessLoadedOfTwo(t *testing.T) {
	flags.Set(t, "grpc_client.conn_pick_policy", connPickLeastPendingRPCs)
	p := testPool(10, 3)
	for i := 0; i < 100; i++ {
		require.Same(t, p.conns[1], p.getConn())
	}
}

func TestGetConn_LeastPending_AvoidsBackedUpConnection(t *testing.T) {
	flags.Set(t, "grpc_client.conn_pick_policy", connPickLeastPendingRPCs)
	// Connection 2 is badly backed up; the rest are idle.
	p := testPool(0, 0, 1000, 0, 0)
	counts := make([]int, len(p.conns))
	for i := 0; i < 10_000; i++ {
		idx, err := strconv.Atoi(p.getConn().index)
		require.NoError(t, err)
		counts[idx]++
	}
	// Power of two choices samples two distinct connections and takes the
	// less-loaded one, so the uniquely most-loaded connection is never picked.
	require.Zero(t, counts[2], "backed-up connection should never be selected")
	// Every other connection should still receive traffic.
	for i, c := range counts {
		if i == 2 {
			continue
		}
		require.NotZero(t, c, "connection %d should receive traffic", i)
	}
}

func TestGetConn_RoundRobinByDefault(t *testing.T) {
	// Under the round-robin policy, selection cycles through connections in
	// order and ignores the pending counts entirely (connection 1 is heavily
	// loaded but still gets its turn).
	flags.Set(t, "grpc_client.conn_pick_policy", connPickRoundRobin)
	p := testPool(0, 100, 0, 0)
	want := []int{1, 2, 3, 0, 1, 2, 3, 0}
	for _, w := range want {
		require.Equal(t, strconv.Itoa(w), p.getConn().index)
	}
}

func TestGetConn_SingleConnection(t *testing.T) {
	for _, policy := range []string{connPickRoundRobin, connPickLeastPendingRPCs} {
		flags.Set(t, "grpc_client.conn_pick_policy", policy)
		p := testPool(5)
		require.Same(t, p.conns[0], p.getConn())
	}
}
