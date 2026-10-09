package grpc_client_test

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testleak"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	t.Cleanup(func() { _ = client.Close() })
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
	t.Cleanup(func() { _ = deadPool.Close() })
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
	t.Cleanup(func() { _ = pool.Close() })
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

const (
	connExpiration    = 5 * time.Minute
	connCheckInterval = 1 * time.Minute
)

func newConnCache(t *testing.T) (*grpc_client.ConnCache, *clockwork.FakeClock) {
	testleak.CheckGoroutines(t)
	env := testenv.GetTestEnv(t)
	clock := clockwork.NewFakeClock()
	c, err := grpc_client.NewConnCache(env, grpc_client.ConnCacheOpts{
		PoolSize:      1,
		Expiration:    connExpiration,
		CheckInterval: connCheckInterval,
		Clock:         clock,
	})
	require.NoError(t, err)
	t.Cleanup(c.CloseForTesting)
	return c, clock
}

func TestConnCache_InvalidOpts(t *testing.T) {
	env := testenv.GetTestEnv(t)
	valid := grpc_client.ConnCacheOpts{PoolSize: 1, Expiration: connExpiration, CheckInterval: connCheckInterval}
	for name, mutate := range map[string]func(*grpc_client.ConnCacheOpts){
		"zero pool size":      func(o *grpc_client.ConnCacheOpts) { o.PoolSize = 0 },
		"zero expiration":     func(o *grpc_client.ConnCacheOpts) { o.Expiration = 0 },
		"zero check interval": func(o *grpc_client.ConnCacheOpts) { o.CheckInterval = 0 },
		"negative interval":   func(o *grpc_client.ConnCacheOpts) { o.CheckInterval = -time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			opts := valid
			mutate(&opts)
			_, err := grpc_client.NewConnCache(env, opts)
			require.Error(t, err)
			assert.True(t, status.IsInvalidArgumentError(err), "expected invalid argument, got: %v", err)
		})
	}
}

func TestConnCache_StopExpiring(t *testing.T) {
	c, clock := newConnCache(t)
	before, err := c.Get("a:1")
	require.NoError(t, err)

	c.StopExpiring()
	clock.Advance(2 * connExpiration)

	after, err := c.Get("a:1")
	require.NoError(t, err)
	assert.Same(t, before, after)
}

func TestConnCache_ReusesConnections(t *testing.T) {
	c, _ := newConnCache(t)

	a1, err := c.Get("a:1")
	require.NoError(t, err)
	a2, err := c.Get("a:1")
	require.NoError(t, err)
	b, err := c.Get("b:1")
	require.NoError(t, err)

	assert.Same(t, a1, a2)
	assert.NotSame(t, a1, b)
}

func TestConnCache_ExpiresUnusedConnections(t *testing.T) {
	c, clock := newConnCache(t)

	unused, err := c.Get("unused:1")
	require.NoError(t, err)
	used, err := c.Get("used:1")
	require.NoError(t, err)

	// Keep "used" fresh while "unused" ages past the expiration.
	for elapsed := time.Duration(0); elapsed <= connExpiration; elapsed += connCheckInterval {
		require.NoError(t, clock.BlockUntilContext(t.Context(), 1))
		clock.Advance(connCheckInterval)
		_, err = c.Get("used:1")
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		return grpc_client.CachedConnCountForTesting(c) == 1
	}, 2*time.Second, 10*time.Millisecond, "unused connection was not expired")

	redialed, err := c.Get("unused:1")
	require.NoError(t, err)
	assert.NotSame(t, unused, redialed)
	stillUsed, err := c.Get("used:1")
	require.NoError(t, err)
	assert.Same(t, used, stillUsed)
}
