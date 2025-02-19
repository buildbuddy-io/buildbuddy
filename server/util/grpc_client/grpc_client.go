package grpc_client

import (
	"context"
	"math"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/google"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/mem"
)

const (
	// Log a warning if a new streaming RPC cannot be initialized within
	// this time.
	stuckStreamWarningPeriod = 15 * time.Second

	// Default protocol to use when a target is missing a protocol.
	defaultProtocol = "grpcs://"
)

var (
	poolSize = flag.Int("grpc_client.pool_size", 15, "Number of connections to create to each target.")
)

type clientConn struct {
	*grpc.ClientConn
	wasEverReady atomic.Bool
}

type ClientConnPool struct {
	conns []*clientConn
	idx   atomic.Uint64
}

func (p *ClientConnPool) Check(ctx context.Context) error {
	goodConns := 0
	for _, c := range p.conns {
		connState := c.GetState()
		if connState == connectivity.Ready {
			goodConns++
			continue
		}
		if connState == connectivity.Idle {
			c.Connect()
			goodConns++
			continue
		}
	}
	if goodConns == 0 {
		return status.UnavailableError("No ready connections in gRPC connection pool")
	}
	return nil
}

func (p *ClientConnPool) Close() error {
	for _, c := range p.conns {
		// In practice, this only errors out if you call Close twice.
		if err := c.Close(); err != nil {
			log.Warningf("could not close connection: %s", err)
		}
	}
	return nil
}

func (p *ClientConnPool) getConn() *grpc.ClientConn {
	idx := p.idx.Add(1)
	return p.conns[idx%uint64(len(p.conns))].ClientConn
}

func (p *ClientConnPool) WaitForConn() *grpc.ClientConn {
	return p.getConn()
}

// GetReadyConnection returns a connection from the pool that is known to be
// in a ready state. If no ready connections are available, an error is
// returned. This function is useful when it's preferable to get an error
// rather than potentially blocking while a connection becomes ready.
func (p *ClientConnPool) GetReadyConnection() (*grpc.ClientConn, error) {
	idx := (p.idx.Add(1)) % uint64(len(p.conns))
	startIdx := idx
	for {
		conn := p.conns[idx]
		// The gRPC client library causes RPCs to block while a connection
		// stays in CONNECTING state, regardless of the failfast setting. This
		// can happen when a replica goes away due to a rollout (or any
		// other reason). For use cases where an error is preferred to blocking
		// we skip over connections that have become unready.
		isReady := conn.GetState() == connectivity.Ready
		wasEverReady := conn.wasEverReady.Load()
		if wasEverReady && !isReady {
			conn.Connect()
			idx = (idx + 1) % uint64(len(p.conns))
			if idx == startIdx {
				return nil, status.UnavailableErrorf("no ready connections available")
			}
			continue
		}
		conn.wasEverReady.CompareAndSwap(wasEverReady, wasEverReady || isReady)
		return conn.ClientConn, nil
	}
}

func (p *ClientConnPool) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	return p.getConn().Invoke(ctx, method, args, reply, opts...)
}

func (p *ClientConnPool) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	cancel := canary.StartWithLateFn(
		stuckStreamWarningPeriod,
		func(timeTaken time.Duration) {
			log.CtxWarningf(ctx, "Streaming RPC %q has not been established after %q.", method, timeTaken)
		},
		func(timeTaken time.Duration) {
			log.CtxWarningf(ctx, "Streaming RPC %q was established after %q.", method, timeTaken)
		},
	)
	defer cancel()
	return p.getConn().NewStream(ctx, desc, method, opts...)
}

// ClientConnPoolSplitter wraps two (or more) ClientConnPools and routes
// traffic randomly to different pools based on a client-provided traffic
// allocation.
type ClientConnPoolSplitter struct {
	pools       [100]*ClientConnPool
	uniquePools []*ClientConnPool
}

// Creates a new ClientConnPoolSplitter with the provided traffic allocation.
// The traffic allocation is a map from a ClientConnPool to the percent of
// traffic (as an integer in [0, 100]) that should be sent to that pool. E.g.:
//
//	*pool 1*: 25
//	*pool 2*: 25
//	*pool 3*: 50
//
// Will send 25% of traffic to pool 1, 25% to pool 2, and 50% to pool 3.
//
// TODO(iain): Support decimal percentages (e.g. 1/3 of traffic) by keeping a
// list of pools and float cumulative traffic proportions and then generating
// random floats and selecting the pool corresponding to that random float.
func NewClientConnPoolSplitter(trafficAllocation map[*ClientConnPool]int) (*ClientConnPoolSplitter, error) {
	totalTrafficPercent := 0
	poolIdx := 0
	pools := [100]*ClientConnPool{}
	uniquePools := make([]*ClientConnPool, 0, len(trafficAllocation))
	for pool, trafficPercent := range trafficAllocation {
		totalTrafficPercent += trafficPercent
		for i := 0; i < trafficPercent; i++ {
			pools[poolIdx] = pool
			poolIdx++
		}
		uniquePools = append(uniquePools, pool)
	}
	if totalTrafficPercent != 100 {
		return nil, status.InvalidArgumentErrorf("ClientConnPoolSplitter traffic percentages must add to 100 (was %d)", totalTrafficPercent)
	}
	return &ClientConnPoolSplitter{pools: pools, uniquePools: uniquePools}, nil
}

func (p *ClientConnPoolSplitter) Check(ctx context.Context) error {
	for _, pool := range p.uniquePools {
		if err := pool.Check(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *ClientConnPoolSplitter) Close() error {
	for _, pool := range p.uniquePools {
		pool.Close()
	}
	return nil
}

func (p *ClientConnPoolSplitter) getPool() *ClientConnPool {
	return p.pools[rand.IntN(100)]
}

func (p *ClientConnPoolSplitter) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	return p.getPool().getConn().Invoke(ctx, method, args, reply, opts...)
}

func (p *ClientConnPoolSplitter) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return p.getPool().getConn().NewStream(ctx, desc, method, opts...)
}

// DialSimple handles some of the logic around detecting the correct GRPC
// connection type and applying relevant options when connecting.
//
// This function should be used when dialing from outside of BuildBuddy servers
// such as from cli tools and the like. When dialing from BuildBuddy servers
// (app, executor) you should use DialInternal.
func DialSimple(target string, extraOptions ...grpc.DialOption) (*ClientConnPool, error) {
	var mu sync.Mutex
	var conns []*clientConn

	eg, _ := errgroup.WithContext(context.Background())
	for i := 0; i < *poolSize; i++ {
		eg.Go(func() error {
			conn, err := DialSimpleWithoutPooling(target, extraOptions...)
			if err != nil {
				return err
			}
			mu.Lock()
			conns = append(conns, &clientConn{ClientConn: conn})
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return &ClientConnPool{conns: conns}, nil
}

// DialSimpleWithoutPooling is a variant of DialSimple that disables connection
// pooling. Only one connection will be created and that connection RPC
// throughput will be limited by the concurrent stream limit of the server.
//
// This function should not be used outside of tests and currently remains to
// integrate with a third-party library (grpc-proxy).
func DialSimpleWithoutPooling(target string, extraOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	target = normalizeTarget(target)

	dialOptions := CommonGRPCClientOptions()
	dialOptions = append(dialOptions, extraOptions...)
	u, err := url.Parse(target)
	if err == nil {
		if u.User != nil {
			dialOptions = append(dialOptions, grpc.WithPerRPCCredentials(newRPCCredentials(u.User.String())))
		}
		if u.Scheme == "grpcs" {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(google.NewDefaultCredentials().TransportCredentials()))
		} else {
			dialOptions = append(dialOptions, grpc.WithInsecure())
		}

		if u.Scheme == "grpcs" && u.Port() == "" {
			u.Host += ":443"
		}

		if u.Scheme != "unix" {
			target = u.Host
		}
	}

	// Connect to host/port and create a new client
	return grpc.Dial(target, dialOptions...)
}

// DialInternal is similar to DialSimple, but it adds additional interceptors
// (such as client identity) based on the specified environment.
//
// Outside of BuildBuddy servers, DialSimple should be used instead.
func DialInternal(env environment.Env, target string, extraOptions ...grpc.DialOption) (*ClientConnPool, error) {
	opts := []grpc.DialOption{interceptors.GetUnaryClientIdentityInterceptor(env), interceptors.GetStreamClientIdentityInterceptor(env)}
	opts = append(opts, extraOptions...)
	return DialSimple(target, opts...)
}

// DialInternalWithoutPooling is a variant of DialInternal that disables
// connection pooling. Only one connection will be created and that connection
// RPC throughput will be limited by the concurrent stream limit of the server.
//
// This function should not be used unless you need finer control over
// connection state, which should not generally be the case.
func DialInternalWithoutPooling(env environment.Env, target string, extraOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{interceptors.GetUnaryClientIdentityInterceptor(env), interceptors.GetStreamClientIdentityInterceptor(env)}
	opts = append(opts, extraOptions...)
	return DialSimpleWithoutPooling(target, opts...)
}

func normalizeTarget(target string) string {
	if strings.Contains(target, "://") {
		return target
	}
	return defaultProtocol + target
}

type rpcCredentials struct {
	authorization string
}

func newRPCCredentials(authorization string) *rpcCredentials {
	return &rpcCredentials{
		authorization: authorization,
	}
}

func (c *rpcCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.authorization,
	}, nil
}

func (c *rpcCredentials) RequireTransportSecurity() bool {
	return false
}

func CommonGRPCClientOptions() []grpc.DialOption {
	return []grpc.DialOption{
		interceptors.GetUnaryClientInterceptor(),
		interceptors.GetStreamClientInterceptor(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		experimental.WithBufferPool(mem.DefaultBufferPool()),
		grpc.WithSharedWriteBuffer(true),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// After a duration of this time if the client doesn't see any activity it
			// pings the server to see if the transport is still alive.
			Time: 30 * time.Second,

			// After having pinged for keepalive check, the client waits for a duration
			// of Timeout and if no activity is seen even after that the connection is
			// closed.
			Timeout: 20 * time.Second,

			// If true, client sends keepalive pings even with no active RPCs.
			PermitWithoutStream: true,
		}),
	}
}
