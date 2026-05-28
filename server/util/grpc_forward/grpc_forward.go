package grpc_forward

import (
	"context"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// proxyPair defines a prefix to match against the incoming grpc method name
// and a target to proxy this traffic to.
type proxyPair struct {
	Prefix string `yaml:"prefix" json:"prefix" usage:"The gRPC method prefix to match."`
	Target string `yaml:"target" json:"target" usage:"The gRPC target to forward requests to."`
}

var (
	proxyTargets = flag.Slice("app.proxy_targets", []proxyPair{}, "")
	poolSize     = flag.Int("app.proxy_pool_size", 0, "Number of gRPC connections to create for proxying unknown RPCs.")

	mu                     sync.RWMutex
	backendConnectionPools = map[string]*grpc_client.ClientConnPool{}
)

func lookupProxyTarget(fullMethodName string) (string, error) {
	for _, pair := range *proxyTargets {
		if strings.HasPrefix(fullMethodName, pair.Prefix) {
			return pair.Target, nil
		}
	}
	return "", status.UnimplementedErrorf("unknown service %s", fullMethodName)
}

type dialFn = func(string, ...grpc.DialOption) (*grpc_client.ClientConnPool, error)

func dial(target string, opts ...grpc.DialOption) (*grpc_client.ClientConnPool, error) {
	if *poolSize < 0 {
		return nil, status.InvalidArgumentErrorf("Invalid pool size: %d", *poolSize)
	}
	if *poolSize == 0 {
		return grpc_client.DialSimple(target, opts...)
	}
	return grpc_client.DialSimpleWithPoolSize(target, *poolSize, opts...)
}

func getConnectionPool(dialer dialFn, target string) (*grpc_client.ClientConnPool, error) {
	// Fast path: take a non-exclusive lock and check for an existing
	// connection.
	mu.RLock()
	pool, ok := backendConnectionPools[target]
	mu.RUnlock()
	if ok {
		return pool, nil
	}

	mu.Lock()
	defer mu.Unlock()

	// Check the map again since we briefly released the lock.
	pool, ok = backendConnectionPools[target]
	if ok {
		return pool, nil
	}

	// Note: dial should be non-blocking, so it's fine to do it with the mutex
	// held.
	newPool, err := dialer(target)
	if err != nil {
		return nil, err
	}
	backendConnectionPools[target] = newPool
	return newPool, nil
}

func getProxyDirector() proxy.StreamDirector {
	if len(*proxyTargets) == 0 {
		return nil
	}
	return func(ctx context.Context, fullMethodName string) (context.Context, grpc.ClientConnInterface, error) {
		target, err := lookupProxyTarget(fullMethodName)
		if err != nil {
			return nil, nil, err
		}

		pool, err := getConnectionPool(dial, target)
		if err != nil {
			return nil, nil, err
		}

		cc := pool.WaitForConn()
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md.Copy())
		}
		return ctx, cc, nil
	}
}

func GetForwardingServerOption() grpc.ServerOption {
	if director := getProxyDirector(); director != nil {
		return grpc.UnknownServiceHandler(proxy.TransparentHandler(proxy.StreamDirector(director)))
	}
	return nil
}
