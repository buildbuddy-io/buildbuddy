package grpc_forward

import (
	"context"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// proxyRule forwards RPCs whose method name starts with Prefix to Target, over
// a connection pool of its own.
type proxyRule struct {
	Prefix   string `yaml:"prefix" json:"prefix" usage:"The gRPC method prefix to match."`
	Target   string `yaml:"target" json:"target" usage:"The gRPC target to forward requests to."`
	PoolSize int    `yaml:"pool_size" json:"pool_size" usage:"Number of gRPC connections for this rule's pool. Defaults to app.proxy_pool_size."`
}

var (
	proxyTargets = flag.Slice("app.proxy_targets", []proxyRule{}, "")
	poolSize     = flag.Int("app.proxy_pool_size", 0, "Number of gRPC connections to create for proxying unknown RPCs.")

	mu sync.RWMutex
	// Keyed by value, not by index or pointer into proxyTargets, so that a
	// config reload (SIGHUP) can't map a rule onto another rule's pool and
	// unchanged rules keep their pools.
	backendConnectionPools = map[proxyRule]*grpc_client.ClientConnPool{}
)

func lookupProxyRule(fullMethodName string) (proxyRule, error) {
	for _, rule := range *proxyTargets {
		if strings.HasPrefix(fullMethodName, rule.Prefix) {
			return rule, nil
		}
	}
	return proxyRule{}, status.UnimplementedErrorf("unknown service %s", fullMethodName)
}

type dialFn = func(target string, poolSize int, opts ...grpc.DialOption) (*grpc_client.ClientConnPool, error)

func dial(target string, poolSize int, opts ...grpc.DialOption) (*grpc_client.ClientConnPool, error) {
	if poolSize < 0 {
		return nil, status.InvalidArgumentErrorf("Invalid pool size: %d", poolSize)
	}
	if poolSize == 0 {
		return grpc_client.DialSimple(target, opts...)
	}
	return grpc_client.DialSimpleWithPoolSize(target, poolSize, opts...)
}

// getConnectionPool returns the pool for the first rule matching the method.
// Each rule gets its own pool, even if rules share a target, so that
// long-lived streams on one rule can't starve RPCs matched by another.
func getConnectionPool(dialer dialFn, fullMethodName string) (*grpc_client.ClientConnPool, error) {
	rule, err := lookupProxyRule(fullMethodName)
	if err != nil {
		return nil, err
	}

	// Fast path: take a non-exclusive lock and check for an existing
	// connection.
	mu.RLock()
	pool, ok := backendConnectionPools[rule]
	mu.RUnlock()
	if ok {
		return pool, nil
	}

	mu.Lock()
	defer mu.Unlock()

	// Check the map again since we briefly released the lock.
	if pool, ok := backendConnectionPools[rule]; ok {
		return pool, nil
	}

	size := rule.PoolSize
	if size == 0 {
		size = *poolSize
	}
	// Note: dial should be non-blocking, so it's fine to do it with the mutex
	// held.
	newPool, err := dialer(rule.Target, size)
	if err != nil {
		return nil, err
	}
	backendConnectionPools[rule] = newPool
	return newPool, nil
}

// ctxForBackend forwards the incoming request metadata to the backend,
// overwriting the client-IP and subdomain headers with the values this proxy
// resolved (clientip.Get and subdomain.Get). When the caller has no client
// identity of its own, it also attaches a grpc-proxy identity that attests to
// those values; a caller that already carries a signed identity (e.g. a
// workflow) keeps it so the backend still authorizes on it. Every
// client-supplied header with the authutil.InternalHeaderPrefix is stripped
// first: the proxy is the sole authority for these internal trust signals, so a
// caller can't smuggle an allowed IP past the backend's IP-rule checks or a
// matching subdomain past its API key checks. All of the caller's other headers
// are propagated verbatim.
//
// This composes across proxy hops without trusting raw headers: each hop's
// interceptors only honor these incoming headers when they carry a verified
// grpc-proxy identity, so clientip.Get and subdomain.Get already reflect an
// upstream proxy's attested values, and we re-attest them here.
func ctxForBackend(ctx context.Context, cis interfaces.ClientIdentityService) (context.Context, error) {
	// Propagate the caller's incoming metadata to the backend. This is a
	// blanket copy so that arbitrary headers survive the proxy hop; we then
	// overwrite only the headers asserted below.
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		md = md.Copy()
	} else {
		md = metadata.MD{}
	}

	// Never trust client-supplied internal headers; we assert the ones we're
	// authoritative for ourselves below.
	for k := range md {
		if strings.HasPrefix(k, authutil.InternalHeaderPrefix) {
			delete(md, k)
		}
	}

	modified := false
	if clientIP := clientip.Get(ctx); clientIP != "" {
		md.Set(clientip.HeaderName, clientIP)
		modified = true
	}
	if sd := subdomain.Get(ctx); sd != "" {
		md.Set(subdomain.HeaderName, sd)
		modified = true
	}
	if !modified {
		return metadata.NewOutgoingContext(ctx, md), nil
	}

	// Only attest as grpc-proxy when the caller has no identity of its own.
	// Internal callers (workflows, executors, the app) forward their own signed
	// identity, which the backend authorizes on -- e.g. workflows bypass IP
	// rules by identity. Overwriting it with grpc-proxy would strip that bypass
	// and get the request checked against the forwarded client IP instead.
	// External callers have no identity, so we attach the grpc-proxy identity
	// (cached and refreshed by the client identity service) that attests to the
	// values above. Set (not append) so a duplicate identity header can't be
	// produced.
	if cis != nil && len(md.Get(authutil.ClientIdentityHeaderName)) == 0 {
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
		pool, err := getConnectionPool(dial, fullMethodName)
		if err != nil {
			return nil, nil, err
		}

		ctx, err = ctxForBackend(ctx, cis)
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
	return grpc.UnknownServiceHandler(trackActiveStreams(proxy.TransparentHandler(newDirector(env))))
}

func trackActiveStreams(h grpc.StreamHandler) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		groupID := interfaces.AuthAnonymousUser
		if c, err := claims.ClaimsFromContext(stream.Context()); err == nil && c.GetGroupID() != "" {
			groupID = c.GetGroupID()
		}
		g := metrics.GRPCForwardActiveStreams.WithLabelValues(method, groupID)
		g.Inc()
		defer g.Dec()
		return h(srv, stream)
	}
}
