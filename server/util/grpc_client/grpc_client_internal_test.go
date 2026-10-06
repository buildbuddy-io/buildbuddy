package grpc_client

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testPool builds Ready connections with the given in-flight counts, so these
// tests exercise the selection policies over the Ready subset.
func testPool(t *testing.T, pending ...int64) *ClientConnPool {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conns := make([]*clientConn, len(pending))
	for i, n := range pending {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		c := &clientConn{ClientConn: conn, index: strconv.Itoa(i)}
		c.pending.Store(n)
		conns[i] = c
	}
	return &ClientConnPool{conns: conns}
}

func TestGetConn_LeastPending_PicksLessLoadedOfTwo(t *testing.T) {
	flags.Set(t, "grpc_client.conn_pick_policy", connPickLeastPendingRPCs)
	p := testPool(t, 10, 3)
	for range 100 {
		require.Same(t, p.conns[1], p.getConn())
	}
}

func TestGetConn_LeastPending_AvoidsBackedUpConnection(t *testing.T) {
	flags.Set(t, "grpc_client.conn_pick_policy", connPickLeastPendingRPCs)
	// Connection 2 is badly backed up; the rest are idle.
	p := testPool(t, 0, 0, 1000, 0, 0)
	counts := make([]int, len(p.conns))
	for range 10_000 {
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
	p := testPool(t, 0, 100, 0, 0)
	want := []int{1, 2, 3, 0, 1, 2, 3, 0}
	for _, w := range want {
		require.Equal(t, strconv.Itoa(w), p.getConn().index)
	}
}

func TestGetConn_SingleConnection(t *testing.T) {
	for _, policy := range []string{connPickRoundRobin, connPickLeastPendingRPCs} {
		flags.Set(t, "grpc_client.conn_pick_policy", policy)
		p := testPool(t, 5)
		require.Same(t, p.conns[0], p.getConn())
	}
}
