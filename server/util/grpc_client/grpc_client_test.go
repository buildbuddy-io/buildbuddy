package grpc_client_test

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health"
	hpb "google.golang.org/grpc/health/grpc_health_v1"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
	dto "github.com/prometheus/client_model/go"
)

type TestService struct {
	client   *grpc_client.ClientConnPool
	requests atomic.Int64
}

func (ts *TestService) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	ts.requests.Add(1)
	return &pspb.PingResponse{Tag: req.GetTag()}, nil
}

func startServer(t *testing.T, env environment.Env) *TestService {
	port := testport.FindFree(t)
	server, err := grpc_server.New(env, port, false /*=ssl*/, grpc_server.GRPCServerConfig{})
	require.NoError(t, err)
	ts := TestService{requests: atomic.Int64{}}
	pspb.RegisterApiServer(server.GetServer(), &ts)
	require.NoError(t, server.Start())
	client, err := grpc_client.DialInternal(env, fmt.Sprintf("grpc://localhost:%d", port))
	require.NoError(t, err)
	ts.client = client
	return &ts
}

func requireTraffic(t *testing.T, percent, numRequests, margin, actual int) {
	if percent == 0 {
		require.Equal(t, 0, actual, fmt.Sprintf("Expected server receiving 0%% of traffic to receive 0 requests (actually received %d)", actual))
	} else if percent == 100 {
		require.Equal(t, numRequests, actual, fmt.Sprintf("Expected server receiving 100%% of traffic to receive %d requests (actually received %d)", numRequests, actual))
	} else {
		lowerBound := max(int(math.Floor(float64(numRequests)*float64(percent)/100.0))-margin, 0)
		upperBound := min(int(math.Ceil(float64(numRequests)*float64(percent)/100.0))+margin, numRequests)
		require.True(t, actual <= upperBound && actual >= lowerBound,
			fmt.Sprintf("Expected server receiving %d%% of traffic to receive between [%d, %d] requests (actually received %d)", percent, lowerBound, upperBound, actual))
	}
}

func TestClientConnPoolSplitter(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	first := startServer(t, te)
	second := startServer(t, te)

	type testCase struct {
		firstPercent  int
		secondPercent int
	}

	testCases := []testCase{
		{firstPercent: 0, secondPercent: 100},
		{firstPercent: 1, secondPercent: 99},
		{firstPercent: 10, secondPercent: 90},
		{firstPercent: 50, secondPercent: 50},
		{firstPercent: 100, secondPercent: 0},
	}

	numRequests := 2_500
	margin := 150

	for _, tc := range testCases {
		splitter, err := grpc_client.NewClientConnPoolSplitter(
			map[*grpc_client.ClientConnPool]int{
				first.client:  tc.firstPercent,
				second.client: tc.secondPercent,
			})
		require.NoError(t, err)

		splitterClient := pspb.NewApiClient(splitter)
		for range numRequests {
			_, err := splitterClient.Ping(ctx, &pspb.PingRequest{})
			require.NoError(t, err)
		}

		requireTraffic(t, tc.firstPercent, numRequests, margin, int(first.requests.Load()))
		requireTraffic(t, tc.secondPercent, numRequests, margin, int(second.requests.Load()))

		first.requests.Store(int64(0))
		second.requests.Store(int64(0))
	}
}

// pendingRPCSeriesCount returns the number of series in the
// PendingClientRPCsPerConnection gauge vec that carry the given target label.
// A series persists at value 0 after its RPC finishes, so this observes which
// (target, pool, method, connection) combinations ever carried an RPC.
func pendingRPCSeriesCount(t *testing.T, target string) int {
	ch := make(chan prometheus.Metric)
	go func() {
		metrics.PendingClientRPCsPerConnection.Collect(ch)
		close(ch)
	}()
	count := 0
	for m := range ch {
		d := &dto.Metric{}
		require.NoError(t, m.Write(d))
		for _, lp := range d.GetLabel() {
			if lp.GetName() == metrics.GRPCTargetLabel && lp.GetValue() == target {
				count++
			}
		}
	}
	return count
}

func TestCheck(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)

	// A pool aimed at a live server reports healthy. Ping first: it blocks
	// until a connection is ready, making the check deterministic.
	ts := startServer(t, te)
	_, err := pspb.NewApiClient(ts.client).Ping(ctx, &pspb.PingRequest{})
	require.NoError(t, err)
	require.NoError(t, ts.client.Check(ctx))

	// A pool that has never managed to connect gets the benefit of the doubt:
	// RPCs dispatched to it ride the connection attempts (and fail fast
	// themselves if the target is down), so Check stays healthy even as the
	// connections cycle through CONNECTING and TRANSIENT_FAILURE.
	deadPool, err := grpc_client.DialSimpleWithPoolSize(fmt.Sprintf("grpc://localhost:%d", testport.FindFree(t)), 2)
	require.NoError(t, err)
	require.Never(t, func() bool { return deadPool.Check(ctx) != nil }, 2*time.Second, 100*time.Millisecond)

	// Once a connection has been ready, losing the server makes the pool
	// unhealthy.
	port := testport.FindFree(t)
	server, err := grpc_server.New(te, port, false /*=ssl*/, grpc_server.GRPCServerConfig{})
	require.NoError(t, err)
	pspb.RegisterApiServer(server.GetServer(), &TestService{})
	require.NoError(t, server.Start())
	pool, err := grpc_client.DialInternalWithPoolSize(te, fmt.Sprintf("grpc://localhost:%d", port), 1)
	require.NoError(t, err)
	_, err = pspb.NewApiClient(pool).Ping(ctx, &pspb.PingRequest{})
	require.NoError(t, err)
	// This Check observes the connection in the Ready state, which is what
	// disqualifies it from benefit-of-the-doubt treatment after the stop.
	require.NoError(t, pool.Check(ctx))
	server.GetServer().Stop()
	require.Eventually(t, func() bool {
		err := pool.Check(ctx)
		return err != nil && status.IsUnavailableError(err)
	}, 15*time.Second, 50*time.Millisecond)
}

func TestClose_DeletesPendingRPCMetricSeries(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	port := testport.FindFree(t)
	server, err := grpc_server.New(te, port, false /*=ssl*/, grpc_server.GRPCServerConfig{})
	require.NoError(t, err)
	pspb.RegisterApiServer(server.GetServer(), &TestService{})
	require.NoError(t, server.Start())

	target := fmt.Sprintf("grpc://localhost:%d", port)
	pool, err := grpc_client.DialInternal(te, target)
	require.NoError(t, err)
	client := pspb.NewApiClient(pool)
	for range 10 {
		_, err := client.Ping(ctx, &pspb.PingRequest{})
		require.NoError(t, err)
	}
	require.NotZero(t, pendingRPCSeriesCount(t, target))

	require.NoError(t, pool.Close())
	require.Zero(t, pendingRPCSeriesCount(t, target))
}

func TestClientConnPool_SkipsUnavailableConnections(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy string
	}{
		{name: "round_robin", policy: "round-robin"},
		{name: "least_pending_rpcs", policy: "least-pending-rpcs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "grpc_client.conn_pick_policy", tc.policy)
			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			pspb.RegisterApiServer(server, &TestService{})
			hpb.RegisterHealthServer(server, health.NewServer())
			go server.Serve(listener)
			t.Cleanup(server.Stop)

			// Hold every dial until we have observed all pool members. None can
			// be Ready yet, so WaitForConn exposes the full pool under either policy.
			dialGate := make(chan struct{})
			var dialCount atomic.Int64
			var allowRecovery atomic.Bool
			const poolSize = 4
			target := "grpc://" + listener.Addr().String()
			pool, err := grpc_client.DialSimpleWithPoolSize(target, poolSize, grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
				select {
				case <-dialGate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				if dialCount.Add(1) != 1 && !allowRecovery.Load() {
					return nil, status.UnavailableError("connection temporarily unavailable")
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			}))
			require.NoError(t, err)
			t.Cleanup(func() { pool.Close() })
			members := make(map[*grpc.ClientConn]bool)
			require.Eventually(t, func() bool {
				members[pool.WaitForConn()] = true
				return len(members) == poolSize
			}, 5*time.Second, time.Millisecond)
			close(dialGate)
			require.Eventually(t, func() bool {
				ready, failed := 0, 0
				for conn := range members {
					switch conn.GetState() {
					case connectivity.Ready:
						ready++
					case connectivity.TransientFailure:
						failed++
					case connectivity.Idle, connectivity.Connecting, connectivity.Shutdown:
						return false
					}
				}
				return ready == 1 && failed == poolSize-1
			}, 5*time.Second, time.Millisecond)

			client := pspb.NewApiClient(pool)
			healthClient := hpb.NewHealthClient(pool)
			for range 20 {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := client.Ping(ctx, &pspb.PingRequest{})
				cancel()
				require.NoError(t, err, "unary RPC should use the Ready connection")
				ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				stream, err := healthClient.Watch(ctx, &hpb.HealthCheckRequest{})
				if err == nil {
					_, err = stream.Recv()
				}
				cancel()
				require.NoError(t, err, "streaming RPC should use the Ready connection")
			}

			// Failed members reconnect normally and rejoin selection when Ready.
			allowRecovery.Store(true)
			for conn := range members {
				conn.ResetConnectBackoff()
			}
			require.Eventually(t, func() bool {
				for conn := range members {
					if conn.GetState() != connectivity.Ready {
						return false
					}
				}
				return true
			}, 5*time.Second, time.Millisecond)
			metrics.PendingClientRPCsPerConnection.DeletePartialMatch(prometheus.Labels{metrics.GRPCTargetLabel: target})
			for range 100 {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := client.Ping(ctx, &pspb.PingRequest{})
				cancel()
				require.NoError(t, err)
			}
			require.Equal(t, poolSize, pendingRPCSeriesCount(t, target), "recovered connections should receive RPCs again")
		})
	}
}

func TestClientConnPool_ColdStart(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy string
	}{
		{name: "round_robin", policy: "round-robin"},
		{name: "least_pending_rpcs", policy: "least-pending-rpcs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "grpc_client.conn_pick_policy", tc.policy)
			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			pspb.RegisterApiServer(server, &TestService{})
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			dialGate := make(chan struct{})
			pool, err := grpc_client.DialSimpleWithPoolSize("grpc://"+listener.Addr().String(), 4, grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
				select {
				case <-dialGate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			}))
			require.NoError(t, err)
			t.Cleanup(func() { pool.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := pspb.NewApiClient(pool).Ping(ctx, &pspb.PingRequest{}, grpc.WaitForReady(true))
				result <- err
			}()
			// The RPC has been dispatched while no connection can be Ready.
			target := "grpc://" + listener.Addr().String()
			require.Eventually(t, func() bool { return pendingRPCSeriesCount(t, target) == 1 }, 5*time.Second, time.Millisecond)
			close(dialGate)
			require.NoError(t, <-result)
		})
	}
}

func TestClientConnPool_ReconnectsIdleConnections(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy string
	}{
		{name: "round_robin", policy: "round-robin"},
		{name: "least_pending_rpcs", policy: "least-pending-rpcs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "grpc_client.conn_pick_policy", tc.policy)
			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			pspb.RegisterApiServer(server, &TestService{})
			go server.Serve(listener)
			t.Cleanup(server.Stop)

			const poolSize = 4
			initialGate := make(chan struct{})
			reconnectGate := make(chan struct{})
			transports := make(chan net.Conn, poolSize)
			var dialCount atomic.Int64
			pool, err := grpc_client.DialSimpleWithPoolSize("grpc://"+listener.Addr().String(), poolSize, grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
				attempt := dialCount.Add(1)
				gate := initialGate
				if attempt > poolSize {
					gate = reconnectGate
				}
				select {
				case <-gate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
				if err == nil && attempt <= poolSize {
					transports <- conn
				}
				return conn, err
			}))
			require.NoError(t, err)
			t.Cleanup(func() { pool.Close() })
			members := make(map[*grpc.ClientConn]bool)
			require.Eventually(t, func() bool {
				members[pool.WaitForConn()] = true
				return len(members) == poolSize
			}, 5*time.Second, time.Millisecond)
			close(initialGate)
			require.Eventually(t, func() bool {
				for conn := range members {
					if conn.GetState() != connectivity.Ready {
						return false
					}
				}
				return true
			}, 5*time.Second, time.Millisecond)

			// Losing an unused transport leaves its connection Idle. Hold its
			// redial so RPCs must continue using the other Ready members.
			require.NoError(t, (<-transports).Close())
			require.Eventually(t, func() bool {
				for conn := range members {
					if conn.GetState() == connectivity.Idle {
						return true
					}
				}
				return false
			}, 5*time.Second, time.Millisecond)
			client := pspb.NewApiClient(pool)
			for range 20 {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := client.Ping(ctx, &pspb.PingRequest{})
				cancel()
				require.NoError(t, err, "RPC should not wait for the Idle member's redial")
			}
			require.Eventually(t, func() bool { return dialCount.Load() > poolSize }, 5*time.Second, time.Millisecond,
				"selection should initiate an Idle member's reconnection")
			close(reconnectGate)
			require.Eventually(t, func() bool {
				for conn := range members {
					if conn.GetState() != connectivity.Ready {
						return false
					}
				}
				return true
			}, 5*time.Second, time.Millisecond)
		})
	}
}
