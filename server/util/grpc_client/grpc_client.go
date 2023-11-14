package grpc_client

import (
	"context"
	"math"
	"net/url"
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
	"google.golang.org/grpc/keepalive"
)

const (
	// Log a warning if a new streaming RPC cannot be initialized within
	// this time.
	stuckStreamWarningPeriod = 15 * time.Second
)

var (
	poolSize = flag.Int("grpc_client.pool_size", 10, "Number of connections to create to each target.")
)

type ClientConnPool struct {
	conns []*grpc.ClientConn
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
	return p.conns[idx%uint64(len(p.conns))]
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

// DialSimple handles some of the logic around detecting the correct GRPC
// connection type and applying relevant options when connecting.
//
// This function should be used when dialing from outside of BuildBuddy servers
// such as from cli tools and the like. When dialing from BuildBuddy servers
// (app, executor) you should use DialInternal.
func DialSimple(target string, extraOptions ...grpc.DialOption) (*ClientConnPool, error) {
	var mu sync.Mutex
	var conns []*grpc.ClientConn

	eg, _ := errgroup.WithContext(context.Background())
	for i := 0; i < *poolSize; i++ {
		eg.Go(func() error {
			conn, err := DialSimpleWithoutPooling(target, extraOptions...)
			if err != nil {
				return err
			}
			mu.Lock()
			conns = append(conns, conn)
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
// TODO(vadim): determine if cacheproxy still needs the extra functionality
func DialInternalWithoutPooling(env environment.Env, target string, extraOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{interceptors.GetUnaryClientIdentityInterceptor(env), interceptors.GetStreamClientIdentityInterceptor(env)}
	opts = append(opts, extraOptions...)
	return DialSimpleWithoutPooling(target, opts...)
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
		grpc.WithRecvBufferPool(grpc.NewSharedBufferPool()),
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
