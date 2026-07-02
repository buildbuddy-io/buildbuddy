package grpc_forward

import (
	"context"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
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
	if pool, ok := backendConnectionPools[target]; ok {
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

// ctxWithClientIP forwards the incoming request metadata to the backend,
// overwriting the client-IP header with the IP this proxy resolved
// (clientip.Get) and attaching a grpc-proxy identity that attests to it. Any
// client-supplied client-IP header is stripped first: the proxy is the sole
// authority for this value, so a caller can't smuggle an allowed IP past the
// backend's IP-rule checks. All of the caller's other headers are propagated
// verbatim.
//
// This composes across proxy hops without trusting raw headers: each hop's
// clientIP interceptor only honors an incoming client-IP header when it carries
// a verified grpc-proxy identity, so clientip.Get already reflects an upstream
// proxy's attested IP, and we re-attest it here.
func ctxWithClientIP(ctx context.Context, cis interfaces.ClientIdentityService) (context.Context, error) {
	// Propagate the caller's incoming metadata to the backend. This is a
	// blanket copy so that arbitrary headers survive the proxy hop; we then
	// overwrite only the client-IP and client-identity headers below.
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		md = md.Copy()
	} else {
		md = metadata.MD{}
	}

	// Never trust a client-supplied client-IP header; we set it ourselves below.
	delete(md, clientip.HeaderName)

	clientIP := clientip.Get(ctx)
	if clientIP == "" {
		return metadata.NewOutgoingContext(ctx, md), nil
	}
	md.Set(clientip.HeaderName, clientIP)

	// Attach the grpc-proxy identity that attests to the client IP. The signed
	// header is cached and refreshed by the client identity service. Set (not
	// append) so a duplicate identity header can't be produced.
	if cis != nil {
		header, err := cis.CachedIdentityHeader(&interfaces.ClientIdentity{
			Origin: interfaces.ClientIdentityInternalOrigin,
			Client: interfaces.ClientIdentityGRPCProxy,
		})
		if err != nil {
			return nil, err
		}
		md.Set(authutil.ClientIdentityHeaderName, header)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}

func newDirector(env environment.Env) proxy.StreamDirector {
	cis := env.GetClientIdentityService()
	return func(ctx context.Context, fullMethodName string) (context.Context, grpc.ClientConnInterface, error) {
		target, err := lookupProxyTarget(fullMethodName)
		if err != nil {
			return nil, nil, err
		}

		pool, err := getConnectionPool(dial, target)
		if err != nil {
			return nil, nil, err
		}

		ctx, err = ctxWithClientIP(ctx, cis)
		if err != nil {
			return nil, nil, err
		}
		return ctx, pool, nil
	}
}

// GetForwardingServerOption returns a gRPC server option that proxies unknown
// RPCs to the configured app.proxy_targets.
func GetForwardingServerOption(env environment.Env) grpc.ServerOption {
	if len(*proxyTargets) == 0 {
		return nil
	}
	return grpc.UnknownServiceHandler(proxy.TransparentHandler(newDirector(env)))
}
