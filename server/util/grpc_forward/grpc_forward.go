package grpc_forward

import (
	"context"
	"strings"
	"sync"
	"time"

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

const (
	// proxyIdentityExpiration is how long a minted grpc-proxy identity header is
	// valid. It must exceed proxyIdentityCacheTTL so the cached header is never
	// expired by the time it is reused.
	proxyIdentityExpiration = 5 * time.Minute
	// proxyIdentityCacheTTL is how long a signed identity header is reused before
	// it is re-minted.
	proxyIdentityCacheTTL = 1 * time.Minute
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

// identityHeader mints and caches a client identity header signed as the
// grpc-proxy identity. We attach the grpc-proxy identity (rather than the
// binary's configured client identity) because the backend only honors a
// forwarded client IP when it is accompanied by a verified grpc-proxy identity.
// The header is re-minted at most once per proxyIdentityCacheTTL.
type identityHeader struct {
	cis interfaces.ClientIdentityService

	mu       sync.RWMutex
	header   string
	mintedAt time.Time
}

func newIdentityHeader(cis interfaces.ClientIdentityService) *identityHeader {
	if cis == nil {
		return nil
	}
	return &identityHeader{cis: cis}
}

func (h *identityHeader) get() (string, error) {
	h.mu.RLock()
	if h.header != "" && time.Since(h.mintedAt) < proxyIdentityCacheTTL {
		header := h.header
		h.mu.RUnlock()
		return header, nil
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	// Re-check now that we hold the write lock, in case another goroutine
	// refreshed it while we were waiting.
	if h.header != "" && time.Since(h.mintedAt) < proxyIdentityCacheTTL {
		return h.header, nil
	}
	header, err := h.cis.IdentityHeader(&interfaces.ClientIdentity{
		Origin: interfaces.ClientIdentityInternalOrigin,
		Client: interfaces.ClientIdentityGRPCProxy,
	}, proxyIdentityExpiration)
	if err != nil {
		return "", err
	}
	h.header = header
	h.mintedAt = time.Now()
	return h.header, nil
}

func attestClientIP(ctx context.Context, idHeader *identityHeader, md metadata.MD) (context.Context, error) {
	clientIP := clientip.Get(ctx)
	if clientIP == "" {
		return ctx, nil
	}

	md.Set(clientip.HeaderName, clientIP)

	// Attach the grpc-proxy identity that attests to the client IP. We use Set
	// rather than append so an identity header that's somehow already present
	// can't produce a duplicate, which the backend would reject.
	if idHeader != nil {
		header, err := idHeader.get()
		if err != nil {
			return nil, err
		}
		md.Set(authutil.ClientIdentityHeaderName, header)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}

// ctxWithClientIP augments the outgoing metadata with the resolved client IP and
// a grpc-proxy identity that attests to it. It deliberately only adds to the
// existing outgoing metadata rather than copying the incoming metadata: the
// caller's headers are forwarded by the server's metadata-propagation
// interceptor (see GetForwardingServerOption), and this proxy must not attest to
// a client IP it didn't resolve itself, so a client-supplied client-IP header is
// left untouched.
func ctxWithClientIP(ctx context.Context, idHeader *identityHeader) (context.Context, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	if len(md.Get(clientip.HeaderName)) > 0 {
		return ctx, nil
	}
	return attestClientIP(ctx, idHeader, md)
}

func newDirector(env environment.Env) proxy.StreamDirector {
	idHeader := newIdentityHeader(env.GetClientIdentityService())
	return func(ctx context.Context, fullMethodName string) (context.Context, grpc.ClientConnInterface, error) {
		target, err := lookupProxyTarget(fullMethodName)
		if err != nil {
			return nil, nil, err
		}

		pool, err := getConnectionPool(dial, target)
		if err != nil {
			return nil, nil, err
		}

		ctx, err = ctxWithClientIP(ctx, idHeader)
		if err != nil {
			return nil, nil, err
		}
		return ctx, pool, nil
	}
}

// GetForwardingServerOption returns a gRPC server option that proxies unknown
// RPCs to the configured app.proxy_targets, asserting the original client's IP so
// the backend's IP-rule enforcement sees the real caller rather than this proxy.
//
// The forwarding handler only adds the client IP + identity to the outgoing
// metadata; it does NOT copy the incoming metadata. The server it's installed on
// must therefore propagate the caller's headers (auth, request metadata, etc.)
// into the outgoing context itself — e.g. via interceptors.PropagateMetadata*
// with proxy_util.HeadersToPropagate (as the cache proxy does). Enabling
// app.proxy_targets on a server without such propagation will forward requests
// with the caller's auth headers stripped.
func GetForwardingServerOption(env environment.Env) grpc.ServerOption {
	if len(*proxyTargets) == 0 {
		return nil
	}
	return grpc.UnknownServiceHandler(proxy.TransparentHandler(newDirector(env)))
}
