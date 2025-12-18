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

	once                   sync.Once
	mu                     sync.RWMutex
	backendConnectionPools map[string]*grpc_client.ClientConnPool
)

func lookupProxyTarget(fullMethodName string) (string, error) {
	for _, pair := range *proxyTargets {
		if strings.HasPrefix(fullMethodName, pair.Prefix) {
			return pair.Target, nil
		}
	}
	return "", status.UnimplementedErrorf("unknown service %s", fullMethodName)
}

func getConnectionPool(target string) (*grpc_client.ClientConnPool, error) {
	once.Do(func() {
		mu.Lock()
		backendConnectionPools = make(map[string]*grpc_client.ClientConnPool)
		mu.Unlock()
	})

	mu.RLock()
	pool, ok := backendConnectionPools[target]
	mu.RUnlock()

	if !ok {
		newPool, err := grpc_client.DialSimple(target)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		backendConnectionPools[target] = newPool
		mu.Unlock()
		pool = newPool
	}
	return pool, nil
}

type directorFunc func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error)

func getProxyDirector() directorFunc {
	if len(*proxyTargets) == 0 {
		return nil
	}
	return func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
		target, err := lookupProxyTarget(fullMethodName)
		if err != nil {
			return nil, nil, err
		}

		pool, err := getConnectionPool(target)
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
