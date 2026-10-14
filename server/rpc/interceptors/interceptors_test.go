package interceptors_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	rtrace "runtime/trace"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	exptrace "golang.org/x/exp/trace"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type pingServer struct {
	lastClientIP   string
	lastSubdomain  string
	lastIncomingMD metadata.MD
	lastOutgoingMD metadata.MD
}

func (p *pingServer) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	p.lastClientIP = clientip.Get(ctx)
	p.lastSubdomain = subdomain.Get(ctx)
	p.lastIncomingMD, _ = metadata.FromIncomingContext(ctx)
	p.lastOutgoingMD, _ = metadata.FromOutgoingContext(ctx)
	return &pspb.PingResponse{
		Tag: req.GetTag(),
	}, nil
}

func TestClientIPInterceptor_FallsBackToPeerAddress(t *testing.T) {
	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))

	env := testenv.GetTestEnv(t)
	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	grpcServer := grpc.NewServer(grpcOptions...)
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	require.NoError(t, err)
	go func() {
		grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	client := pspb.NewApiClient(conn)
	_, err = client.Ping(ctx, &pspb.PingRequest{Tag: 123})
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1", ps.lastClientIP)
}

func BenchmarkBare(b *testing.B) {
	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(b))
	grpcServer := grpc.NewServer()
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		grpcServer.Serve(lis)
	}()

	conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
	if err != nil {
		b.Fatal(err)
	}
	client := pspb.NewApiClient(conn)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		req := &pspb.PingRequest{
			Tag: random.RandUint64(),
		}
		b.StartTimer()
		rsp, err := client.Ping(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if rsp.GetTag() != req.GetTag() {
			b.Fatal("tag mismatch")
		}
	}
	grpcServer.Stop()
}

func BenchmarkInstrumentedClient(b *testing.B) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(b))

	grpcServer := grpc.NewServer()
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		grpcServer.Serve(lis)
	}()

	conn, err := grpc_client.DialSimple("grpc://" + listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	client := pspb.NewApiClient(conn)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		req := &pspb.PingRequest{
			Tag: random.RandUint64(),
		}
		b.StartTimer()
		rsp, err := client.Ping(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if rsp.GetTag() != req.GetTag() {
			b.Fatal("tag mismatch")
		}
	}
	grpcServer.Stop()
}

func BenchmarkInstrumentedServer(b *testing.B) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(b))

	env := testenv.GetTestEnv(b)
	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	grpcServer := grpc.NewServer(grpcOptions...)
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		grpcServer.Serve(lis)
	}()

	conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
	if err != nil {
		b.Fatal(err)
	}
	client := pspb.NewApiClient(conn)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		req := &pspb.PingRequest{
			Tag: random.RandUint64(),
		}
		b.StartTimer()
		rsp, err := client.Ping(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if rsp.GetTag() != req.GetTag() {
			b.Fatal("tag mismatch")
		}
	}
	grpcServer.Stop()
}

func BenchmarkInstrumentedAll(b *testing.B) {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()

	ctx := context.Background()
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(b))

	env := testenv.GetTestEnv(b)
	grpcOptions := grpc_server.CommonGRPCServerOptions(env)
	grpcServer := grpc.NewServer(grpcOptions...)
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	go func() {
		grpcServer.Serve(lis)
	}()

	conn, err := grpc_client.DialSimple("grpc://" + listenAddr)
	if err != nil {
		b.Fatal(err)
	}
	client := pspb.NewApiClient(conn)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		req := &pspb.PingRequest{
			Tag: random.RandUint64(),
		}
		b.StartTimer()
		rsp, err := client.Ping(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
		if rsp.GetTag() != req.GetTag() {
			b.Fatal("tag mismatch")
		}
	}
	grpcServer.Stop()
}

// fakeValidatedIdentityKey is the context key under which the fake stores the
// validated identity, mirroring the real service's private context key.
const fakeValidatedIdentityKey = "test-validated-client-identity"

// fakeClientIdentityService mimics the real client-identity service's
// validate-then-read contract: ValidateIncomingIdentity reads the identity from
// the incoming header and stores it in the context, and IdentityFromContext
// reads it back. This lets the test verify that the identity interceptor runs
// (populating the context) before addClientIPToContext reads the identity — i.e.
// that the interceptor ordering is correct; if they were reordered,
// IdentityFromContext would return NotFound and the override would not happen.
//
// The header carries the bare client name rather than a signed JWT; JWT
// signing/validation is covered by the clientidentity package's own tests.
type fakeClientIdentityService struct{}

func (f *fakeClientIdentityService) AddIdentityToContext(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (f *fakeClientIdentityService) NewIdentityHeader(si *interfaces.ClientIdentity, expiration time.Duration) (string, error) {
	return "", nil
}

func (f *fakeClientIdentityService) CachedIdentityHeader(si *interfaces.ClientIdentity) (string, error) {
	return "", nil
}

func (f *fakeClientIdentityService) ValidateIncomingIdentity(ctx context.Context) (context.Context, error) {
	vals := metadata.ValueFromIncomingContext(ctx, authutil.ClientIdentityHeaderName)
	if len(vals) == 0 {
		return ctx, nil
	}
	return context.WithValue(ctx, fakeValidatedIdentityKey, &interfaces.ClientIdentity{Client: vals[0]}), nil
}

func (f *fakeClientIdentityService) IdentityFromContext(ctx context.Context) (*interfaces.ClientIdentity, error) {
	si, ok := ctx.Value(fakeValidatedIdentityKey).(*interfaces.ClientIdentity)
	if !ok {
		return nil, status.NotFoundError("identity not presented")
	}
	return si, nil
}

func TestTrustedClientIPInterceptor(t *testing.T) {
	const assertedIP = "10.1.2.3"

	for _, tc := range []struct {
		name string
		// clientIdentity is sent in the client-identity header; "" sends none.
		clientIdentity string
		wantIP         string
	}{
		{
			name:           "proxy identity may assert client IP",
			clientIdentity: interfaces.ClientIdentityGRPCProxy,
			wantIP:         assertedIP,
		},
		{
			name:           "app identity may not assert client IP",
			clientIdentity: interfaces.ClientIdentityApp,
			wantIP:         "127.0.0.1",
		},
		{
			name:           "cache-proxy identity may not assert client IP",
			clientIdentity: interfaces.ClientIdentityCacheProxy,
			wantIP:         "127.0.0.1",
		},
		{
			name:           "untrusted caller without identity may not assert client IP",
			clientIdentity: "",
			wantIP:         "127.0.0.1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))

			env := testenv.GetTestEnv(t)
			env.SetClientIdentityService(&fakeClientIdentityService{})

			grpcServer := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
			ps := &pingServer{}
			pspb.RegisterApiServer(grpcServer, ps)

			lis, err := net.Listen("tcp", listenAddr)
			require.NoError(t, err)
			go func() { grpcServer.Serve(lis) }()
			t.Cleanup(grpcServer.Stop)

			conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			ctx := metadata.AppendToOutgoingContext(context.Background(), clientip.HeaderName, assertedIP)
			if tc.clientIdentity != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, tc.clientIdentity)
			}
			_, err = pspb.NewApiClient(conn).Ping(ctx, &pspb.PingRequest{Tag: 123})
			require.NoError(t, err)

			assert.Equal(t, tc.wantIP, ps.lastClientIP)
		})
	}
}

func TestTrustedSubdomainInterceptor(t *testing.T) {
	const assertedSubdomain = "acme"

	for _, tc := range []struct {
		name string
		// clientIdentity is sent in the client-identity header; "" sends none.
		clientIdentity string
		wantSubdomain  string
	}{
		{
			name:           "proxy identity may assert subdomain",
			clientIdentity: interfaces.ClientIdentityGRPCProxy,
			wantSubdomain:  assertedSubdomain,
		},
		{
			name:           "app identity may not assert subdomain",
			clientIdentity: interfaces.ClientIdentityApp,
			wantSubdomain:  "",
		},
		{
			name:           "cache-proxy identity may not assert subdomain",
			clientIdentity: interfaces.ClientIdentityCacheProxy,
			wantSubdomain:  "",
		},
		{
			name:           "untrusted caller without identity may not assert subdomain",
			clientIdentity: "",
			wantSubdomain:  "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "app.enable_subdomain_matching", true)
			listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))

			env := testenv.GetTestEnv(t)
			env.SetClientIdentityService(&fakeClientIdentityService{})

			grpcServer := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
			ps := &pingServer{}
			pspb.RegisterApiServer(grpcServer, ps)

			lis, err := net.Listen("tcp", listenAddr)
			require.NoError(t, err)
			go func() { grpcServer.Serve(lis) }()
			t.Cleanup(grpcServer.Stop)

			conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			ctx := metadata.AppendToOutgoingContext(context.Background(), subdomain.HeaderName, assertedSubdomain)
			if tc.clientIdentity != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, tc.clientIdentity)
			}
			_, err = pspb.NewApiClient(conn).Ping(ctx, &pspb.PingRequest{Tag: 123})
			require.NoError(t, err)

			assert.Equal(t, tc.wantSubdomain, ps.lastSubdomain)
		})
	}
}

// TestSubdomainInterceptorIgnoresHeaderWhenMatchingDisabled verifies that a
// trusted proxy can't turn on subdomain restrictions for a server that has
// subdomain matching disabled.
func TestSubdomainInterceptorIgnoresHeaderWhenMatchingDisabled(t *testing.T) {
	flags.Set(t, "app.enable_subdomain_matching", false)
	listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))

	env := testenv.GetTestEnv(t)
	env.SetClientIdentityService(&fakeClientIdentityService{})

	grpcServer := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
	ps := &pingServer{}
	pspb.RegisterApiServer(grpcServer, ps)

	lis, err := net.Listen("tcp", listenAddr)
	require.NoError(t, err)
	go func() { grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx := metadata.AppendToOutgoingContext(context.Background(), subdomain.HeaderName, "acme")
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, interfaces.ClientIdentityGRPCProxy)
	_, err = pspb.NewApiClient(conn).Ping(ctx, &pspb.PingRequest{Tag: 123})
	require.NoError(t, err)

	assert.Equal(t, "", ps.lastSubdomain)
}

func TestInternalHeadersAreStrippedBeforePropagation(t *testing.T) {
	const internalHeader = authutil.InternalHeaderPrefix + "test"

	for _, tc := range []struct {
		name           string
		clientIdentity string // sent in the client-identity header; "" sends none
		wantStripped   bool
	}{
		{
			name:           "untrusted caller without identity is stripped",
			clientIdentity: "",
			wantStripped:   true,
		},
		{
			name:           "untrusted caller with unknown identity is stripped",
			clientIdentity: "some-untrusted-client",
			wantStripped:   true,
		},
		{
			name:           "grpc-proxy identity is preserved",
			clientIdentity: interfaces.ClientIdentityGRPCProxy,
			wantStripped:   false,
		},
		{
			name:           "app identity is preserved",
			clientIdentity: interfaces.ClientIdentityApp,
			wantStripped:   false,
		},
		{
			name:           "executor identity is preserved",
			clientIdentity: interfaces.ClientIdentityExecutor,
			wantStripped:   false,
		},
		{
			name:           "cache proxy identity is preserved",
			clientIdentity: interfaces.ClientIdentityCacheProxy,
			wantStripped:   false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listenAddr := fmt.Sprintf("localhost:%d", testport.FindFree(t))

			env := testenv.GetTestEnv(t)
			env.SetClientIdentityService(&fakeClientIdentityService{})

			grpcServer := grpc.NewServer(grpc_server.CommonGRPCServerOptionsWithConfig(env, grpc_server.GRPCServerConfig{
				ExtraChainedUnaryInterceptors: []grpc.UnaryServerInterceptor{
					interceptors.PropagateMetadataUnaryInterceptor(internalHeader),
				},
			})...)
			ps := &pingServer{}
			pspb.RegisterApiServer(grpcServer, ps)

			lis, err := net.Listen("tcp", listenAddr)
			require.NoError(t, err)
			go func() { grpcServer.Serve(lis) }()
			t.Cleanup(grpcServer.Stop)

			conn, err := grpc.Dial(listenAddr, grpc.WithInsecure())
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			ctx := metadata.AppendToOutgoingContext(context.Background(), internalHeader, "spoofed")
			if tc.clientIdentity != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, tc.clientIdentity)
			}
			_, err = pspb.NewApiClient(conn).Ping(ctx, &pspb.PingRequest{Tag: 123})
			require.NoError(t, err)

			if tc.wantStripped {
				assert.Empty(t, ps.lastIncomingMD.Get(internalHeader))
				assert.Empty(t, ps.lastOutgoingMD.Get(internalHeader))
			} else {
				assert.Equal(t, []string{"spoofed"}, ps.lastIncomingMD.Get(internalHeader))
				assert.Equal(t, []string{"spoofed"}, ps.lastOutgoingMD.Get(internalHeader))
			}
		})
	}
}

const originMethodHeader = authutil.InternalHeaderPrefix + "origin-method"

type hopObservation struct {
	grpcMethod   string
	originMethod string
	// header is the origin-method header seen in the handler's incoming metadata.
	header []string
}

func observe(ctx context.Context, method string) hopObservation {
	om, _ := pprof.Label(ctx, pprofutil.OriginMethod)
	return hopObservation{
		grpcMethod:   method,
		originMethod: om,
		header:       metadata.ValueFromIncomingContext(ctx, originMethodHeader),
	}
}

// hopServerA serves Ping and forwards each call to server B's ByteStream.Read.
type hopServerA struct {
	pspb.UnimplementedApiServer
	bClient bspb.ByteStreamClient
	seen    chan hopObservation
	// callB is whether Ping calls server B.
	callB bool
}

func (s *hopServerA) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	s.seen <- observe(ctx, pingMethod)
	if !s.callB {
		return &pspb.PingResponse{}, nil
	}
	// Server B only honors internal headers from trusted clients.
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, interfaces.ClientIdentityApp)
	stream, err := s.bClient.Read(ctx, &bspb.ReadRequest{})
	if err != nil {
		return nil, err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return &pspb.PingResponse{}, nil
			}
			return nil, err
		}
	}
}

type hopServerB struct {
	bspb.UnimplementedByteStreamServer
	seen chan hopObservation
}

func (s *hopServerB) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	s.seen <- observe(stream.Context(), readMethod)
	return nil
}

func startHopServer(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	env := testenv.GetTestEnv(t)
	env.SetClientIdentityService(&fakeClientIdentityService{})
	srv := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
	register(srv)
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", testport.FindFree(t)))
	require.NoError(t, err)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(),
		interceptors.GetUnaryClientInterceptor(), interceptors.GetStreamClientInterceptor())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// startHops starts servers A (Ping) and B (Read), where A's Ping calls B.
func startHops(t *testing.T, callB bool) (pspb.ApiClient, *hopServerA, *hopServerB) {
	b := &hopServerB{seen: make(chan hopObservation, 10)}
	connB := startHopServer(t, func(s *grpc.Server) { bspb.RegisterByteStreamServer(s, b) })
	a := &hopServerA{bClient: bspb.NewByteStreamClient(connB), seen: make(chan hopObservation, 10), callB: callB}
	connA := startHopServer(t, func(s *grpc.Server) { pspb.RegisterApiServer(s, a) })
	return pspb.NewApiClient(connA), a, b
}

func TestOriginMethodPropagatesToTrustedPeer(t *testing.T) {
	enableLabels(t)
	client, a, b := startHops(t, true)

	_, err := client.Ping(context.Background(), &pspb.PingRequest{})
	require.NoError(t, err)

	require.Equal(t, hopObservation{grpcMethod: pingMethod, originMethod: pingMethod}, <-a.seen)
	got := <-b.seen
	require.Equal(t, readMethod, got.grpcMethod)
	require.Equal(t, pingMethod, got.originMethod)
	require.Equal(t, []string{pingMethod}, got.header)
}

func TestOriginMethodIgnoresSpoofedHeader(t *testing.T) {
	enableLabels(t)
	client, a, b := startHops(t, true)

	ctx := metadata.AppendToOutgoingContext(context.Background(), originMethodHeader, "/fake/Method")
	_, err := client.Ping(ctx, &pspb.PingRequest{})
	require.NoError(t, err)

	require.Equal(t, hopObservation{grpcMethod: pingMethod, originMethod: pingMethod}, <-a.seen)
	got := <-b.seen
	require.Equal(t, pingMethod, got.originMethod)
	require.Equal(t, []string{pingMethod}, got.header)
}

func TestOriginMethodNotForwardedWhenLabelsDisabled(t *testing.T) {
	client, a, b := startHops(t, true)

	// A trusted caller's header reaches A, but A must not forward it.
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		authutil.ClientIdentityHeaderName, interfaces.ClientIdentityApp,
		originMethodHeader, "/fake/Method")
	_, err := client.Ping(ctx, &pspb.PingRequest{})
	require.NoError(t, err)

	<-a.seen
	got := <-b.seen
	require.Empty(t, got.originMethod)
	require.Empty(t, got.header)
}

// handlerTimeout bounds waits for events that are guaranteed to happen, so that
// only a stalled machine hits it.
const handlerTimeout = 60 * time.Second

const (
	pingMethod = "/ping.Api/Ping"
	readMethod = "/google.bytestream.ByteStream/Read"
	findMethod = "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs"
)

var labelKeys = []string{
	pprofutil.GRPCMethod, pprofutil.Client, pprofutil.Origin,
	pprofutil.InvocationID, pprofutil.ActionMnemonic, pprofutil.GroupID,
}

// labelsOf returns the pprof labels carried by ctx.
func labelsOf(ctx context.Context) map[string]string {
	m := map[string]string{}
	for _, k := range labelKeys {
		if v, ok := pprof.Label(ctx, k); ok {
			m[k] = v
		}
	}
	return m
}

// labelServer implements the services used by these tests, recording the
// labels each handler's ctx carries.
type labelServer struct {
	pspb.UnimplementedApiServer
	bspb.UnimplementedByteStreamServer
	repb.UnimplementedContentAddressableStorageServer

	// onPing, if set, runs in the Ping handler.
	onPing func(ctx context.Context)
	// bigDigests is the number of digests FindMissingBlobs returns.
	bigDigests int
	labels     chan map[string]string
	// ended receives a value each time an RPC finishes, after the profiling
	// stats handler has reset the worker goroutine's labels.
	ended chan struct{}
}

// endNotifier is registered after ProfilingStatsHandler, so by the time it sees
// an RPC's End event that handler has already ended the RPC's trace task and
// reset the worker goroutine's labels. Unlike a response arriving at the
// client, which happens before the End event, this is a reliable sync point
// for tests that inspect state after an RPC.
type endNotifier struct{ ended chan struct{} }

func (e endNotifier) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (e endNotifier) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (e endNotifier) HandleConn(context.Context, stats.ConnStats) {}
func (e endNotifier) HandleRPC(_ context.Context, rs stats.RPCStats) {
	if _, ok := rs.(*stats.End); !ok {
		return
	}
	select {
	case e.ended <- struct{}{}:
	default: // Benchmarks don't wait for ends.
	}
}

// waitForEnds blocks until n more RPCs have fully finished on the server. Each
// is guaranteed to, so this only times out if the machine is stalled.
func waitForEnds(t testing.TB, s *labelServer, n int) {
	for range n {
		select {
		case <-s.ended:
		case <-time.After(handlerTimeout):
			require.FailNow(t, "RPC did not finish")
		}
	}
}

func (s *labelServer) record(ctx context.Context) {
	select {
	case s.labels <- labelsOf(ctx):
	default:
	}
}

func (s *labelServer) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	s.record(ctx)
	if s.onPing != nil {
		s.onPing(ctx)
	}
	return &pspb.PingResponse{Tag: req.GetTag()}, nil
}

func (s *labelServer) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	s.record(stream.Context())
	return stream.Send(&bspb.ReadResponse{Data: []byte("x")})
}

func (s *labelServer) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	s.record(ctx)
	return &repb.FindMissingBlobsResponse{MissingBlobDigests: makeDigests(s.bigDigests)}, nil
}

func makeDigests(n int) []*repb.Digest {
	ds := make([]*repb.Digest, n)
	for i := range ds {
		ds[i] = &repb.Digest{Hash: fmt.Sprintf("%064d", i), SizeBytes: int64(i)}
	}
	return ds
}

type testClients struct {
	ping pspb.ApiClient
	bs   bspb.ByteStreamClient
	cas  repb.ContentAddressableStorageClient
}

// startLabelServer starts a server with the full interceptor chain. Set the
// pprof flag before calling.
func startLabelServer(t testing.TB, s *labelServer) testClients {
	s.labels = make(chan map[string]string, 1000)
	s.ended = make(chan struct{}, 1000)
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1", "US2", "GR2")))
	grpcServer := grpc.NewServer(grpc_server.CommonGRPCServerOptionsWithConfig(env, grpc_server.GRPCServerConfig{
		ExtraStatsHandlers: []stats.Handler{endNotifier{s.ended}},
	})...)
	pspb.RegisterApiServer(grpcServer, s)
	bspb.RegisterByteStreamServer(grpcServer, s)
	repb.RegisterContentAddressableStorageServer(grpcServer, s)

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", testport.FindFree(t)))
	require.NoError(t, err)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<30)))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return testClients{
		ping: pspb.NewApiClient(conn),
		bs:   bspb.NewByteStreamClient(conn),
		cas:  repb.NewContentAddressableStorageClient(conn),
	}
}

// requestCtx returns an outgoing context with the given user's API key (if
// any) and Bazel request metadata.
func requestCtx(t testing.TB, apiKey string, invocationID string, extra ...string) context.Context {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey)
	}
	rmd, err := proto.Marshal(&repb.RequestMetadata{
		ToolInvocationId: invocationID,
		ActionMnemonic:   "GoCompile",
	})
	require.NoError(t, err)
	ctx = metadata.AppendToOutgoingContext(ctx, bazel_request.RequestMetadataKey, string(rmd))
	if len(extra) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, extra...)
	}
	return ctx
}

func next(t *testing.T, s *labelServer) map[string]string {
	select {
	case l := <-s.labels:
		return l
	case <-time.After(handlerTimeout):
		require.FailNow(t, "handler did not run")
		return nil
	}
}

func readStream(t *testing.T, ctx context.Context, c bspb.ByteStreamClient) {
	stream, err := c.Read(ctx, &bspb.ReadRequest{ResourceName: "x"})
	require.NoError(t, err)
	for {
		if _, err := stream.Recv(); err != nil {
			require.Equal(t, io.EOF, err)
			return
		}
	}
}

func enableLabels(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
}

func TestUnaryRequestLabels(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	c := startLabelServer(t, s)

	ctx := requestCtx(t, "US1", "inv1", usageutil.ClientHeaderName, "ios", usageutil.OriginHeaderName, "web")
	_, err := c.ping.Ping(ctx, &pspb.PingRequest{})
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		pprofutil.GRPCMethod:     pingMethod,
		pprofutil.Client:         "ios",
		pprofutil.Origin:         "web",
		pprofutil.InvocationID:   "inv1",
		pprofutil.ActionMnemonic: "GoCompile",
		pprofutil.GroupID:        "GR1",
	}, next(t, s))
}

func TestStreamRequestLabels(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	c := startLabelServer(t, s)

	ctx := requestCtx(t, "US1", "inv1", usageutil.ClientHeaderName, "ios", usageutil.OriginHeaderName, "web")
	readStream(t, ctx, c.bs)

	require.Equal(t, map[string]string{
		pprofutil.GRPCMethod:     readMethod,
		pprofutil.Client:         "ios",
		pprofutil.Origin:         "web",
		pprofutil.InvocationID:   "inv1",
		pprofutil.ActionMnemonic: "GoCompile",
		pprofutil.GroupID:        "GR1",
	}, next(t, s))
}

func TestNoLabelsWhenFlagOff(t *testing.T) {
	s := &labelServer{}
	c := startLabelServer(t, s)

	_, err := c.ping.Ping(requestCtx(t, "US1", "inv1"), &pspb.PingRequest{})
	require.NoError(t, err)
	readStream(t, requestCtx(t, "US1", "inv1"), c.bs)

	require.Empty(t, next(t, s))
	require.Empty(t, next(t, s))
}

var burnSink int

// burnCPU does a fixed amount of CPU work, so the number of profile samples it
// yields depends on the CPU time it gets rather than on wall time.
//
//go:noinline
func burnCPU() {
	x := 0
	for i := range 100_000_000 {
		x += i * i
	}
	burnSink = x
}

// profileCPU runs f under the CPU profiler and returns the parsed profile.
func profileCPU(t *testing.T, f func()) *profile.Profile {
	var buf bytes.Buffer
	require.NoError(t, pprof.StartCPUProfile(&buf))
	// Make sure the profile is stopped even if f fails the test.
	var once sync.Once
	stop := func() { once.Do(pprof.StopCPUProfile) }
	defer stop()
	f()
	stop()
	p, err := profile.Parse(&buf)
	require.NoError(t, err)
	return p
}

// sampleStats returns the number of samples whose stack satisfies match, and
// how many of those carry all of the wanted labels.
func sampleStats(p *profile.Profile, match func(funcNames []string) bool, want map[string]string) (matched, labeled int) {
	for _, s := range p.Sample {
		var names []string
		for _, loc := range s.Location {
			for _, line := range loc.Line {
				names = append(names, line.Function.Name)
			}
		}
		if !match(names) {
			continue
		}
		matched++
		ok := true
		for k, v := range want {
			if len(s.Label[k]) != 1 || s.Label[k][0] != v {
				ok = false
			}
		}
		if ok {
			labeled++
		}
	}
	return matched, labeled
}

// maxProfileRuns bounds collectSamples. Each run yields samples in proportion
// to the CPU time f uses, whatever the load on the machine, so this is only
// reached if match never matches.
const maxProfileRuns = 200

// collectSamples repeatedly profiles f until at least minMatched samples
// satisfy match, and returns the total matched and labeled counts.
func collectSamples(t *testing.T, f func(), match func(funcNames []string) bool, want map[string]string, minMatched int) (matched, labeled int) {
	for run := 0; matched < minMatched; run++ {
		require.Less(t, run, maxProfileRuns, "only %d of %d samples matched", matched, minMatched)
		m, l := sampleStats(profileCPU(t, f), match, want)
		matched += m
		labeled += l
	}
	return matched, labeled
}

func anyContains(names []string, substr string) bool {
	for _, n := range names {
		if strings.Contains(n, substr) {
			return true
		}
	}
	return false
}

func TestCPUProfileLabelsHandlerWork(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	// Burn in a child goroutine, which must inherit the handler's labels.
	s.onPing = func(ctx context.Context) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			burnCPU()
		}()
		<-done
	}
	c := startLabelServer(t, s)

	matched, labeled := collectSamples(t, func() {
		_, err := c.ping.Ping(requestCtx(t, "US1", "inv1"), &pspb.PingRequest{})
		require.NoError(t, err)
	},
		func(names []string) bool { return anyContains(names, ".burnCPU") },
		map[string]string{pprofutil.GRPCMethod: pingMethod, pprofutil.GroupID: "GR1"},
		10)
	// burnCPU only runs in the goroutine the handler starts, which inherits the
	// handler's labels, so every sample must carry them.
	require.Equal(t, matched, labeled, "labeled %d of %d samples", labeled, matched)
}

func TestCPUProfileLabelsUnaryDecodeAndEncode(t *testing.T) {
	enableLabels(t)
	s := &labelServer{bigDigests: 50000}
	c := startLabelServer(t, s)
	req := &repb.FindMissingBlobsRequest{BlobDigests: makeDigests(50000)}

	// Server-side marshal/unmarshal runs under (*Server).processRPC; the
	// client's does not. The match is narrowed to processRPC rather than
	// handleStream because handleStream also runs TagRPC (grpc v1.84
	// server.go:1578), which parses the request metadata before the labels are
	// set. processRPC (called at server.go:1595) starts only after TagRPC
	// returns, unary decode and encode run inside it, and its deferred End
	// handling (server.go:1330-1351) resets the labels without marshaling.
	matched, labeled := collectSamples(t, func() {
		_, err := c.cas.FindMissingBlobs(requestCtx(t, "US1", "inv1"), req)
		require.NoError(t, err)
	},
		func(names []string) bool {
			return anyContains(names, "(*Server).processRPC") && anyContains(names, "arshal")
		},
		map[string]string{pprofutil.GRPCMethod: findMethod},
		10)
	// The labels are set before processRPC starts and reset only at End, after
	// the response is encoded, so every sample under processRPC must carry the
	// method.
	require.Equal(t, matched, labeled, "labeled %d of %d samples", labeled, matched)
}

func BenchmarkInstrumentedServerPprofLabels(b *testing.B) {
	for _, tc := range []struct{ labels, tasks bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		b.Run(fmt.Sprintf("labels=%v,tasks=%v", tc.labels, tc.tasks), func(b *testing.B) {
			flags.Set(b, "monitoring.enable_pprof_labels", tc.labels)
			flags.Set(b, "monitoring.enable_trace_tasks", tc.tasks)
			c := startLabelServer(b, &labelServer{})
			ctx := requestCtx(b, "US1", "inv1", usageutil.ClientHeaderName, "ios")
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err := c.ping.Ping(ctx, &pspb.PingRequest{Tag: uint64(n)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// traceInfo summarizes the trace tasks of a given type found in an execution
// trace.
type traceInfo struct {
	begins, ends int
	// regionBegins and regionEnds count regions of the same type inside tasks
	// of the type.
	regionBegins, regionEnds int
	// logs maps log category to messages, for logs attached to tasks of the type.
	logs map[string][]string
}

func readTraceTasks(t *testing.T, buf *bytes.Buffer, taskType string) traceInfo {
	r, err := exptrace.NewReader(buf)
	require.NoError(t, err)
	info := traceInfo{logs: map[string][]string{}}
	ids := map[exptrace.TaskID]bool{}
	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		switch ev.Kind() {
		case exptrace.EventTaskBegin:
			if ev.Task().Type == taskType {
				info.begins++
				ids[ev.Task().ID] = true
			}
		case exptrace.EventTaskEnd:
			if ev.Task().Type == taskType {
				info.ends++
			}
		case exptrace.EventRegionBegin:
			if r := ev.Region(); ids[r.Task] && r.Type == taskType {
				info.regionBegins++
			}
		case exptrace.EventRegionEnd:
			if r := ev.Region(); ids[r.Task] && r.Type == taskType {
				info.regionEnds++
			}
		case exptrace.EventLog:
			if l := ev.Log(); ids[l.Task] {
				info.logs[l.Category] = append(info.logs[l.Category], l.Message)
			}
		default:
		}
	}
	return info
}

func tracePing(t *testing.T) traceInfo {
	s := &labelServer{}
	c := startLabelServer(t, s)
	var buf bytes.Buffer
	require.NoError(t, rtrace.Start(&buf))
	// Make sure the trace is stopped even if the test fails.
	var once sync.Once
	stop := func() { once.Do(rtrace.Stop) }
	t.Cleanup(stop)
	_, err := c.ping.Ping(requestCtx(t, "US1", ""), &pspb.PingRequest{})
	// The task ends in the End stats event, which runs after the response is
	// sent.
	waitForEnds(t, s, 1)
	stop()
	require.NoError(t, err)
	return readTraceTasks(t, &buf, pingMethod)
}

func TestTraceTaskPerRequest(t *testing.T) {
	for _, labels := range []bool{false, true} {
		t.Run(fmt.Sprintf("labels=%t", labels), func(t *testing.T) {
			flags.Set(t, "monitoring.enable_trace_tasks", true)
			flags.Set(t, "monitoring.enable_pprof_labels", labels)

			info := tracePing(t)

			require.Equal(t, 1, info.begins)
			require.Equal(t, 1, info.ends)
			require.Equal(t, 1, info.regionBegins)
			require.Equal(t, 1, info.regionEnds)
			require.Equal(t, []string{"GR1"}, info.logs[pprofutil.GroupID])
		})
	}
}

func TestNoTraceTaskWhenFlagOff(t *testing.T) {
	// Labels on, trace tasks off: the stats handler is registered but must not
	// create tasks.
	flags.Set(t, "monitoring.enable_pprof_labels", true)

	info := tracePing(t)

	require.Equal(t, 0, info.begins)
	require.Equal(t, 0, info.ends)
	require.Equal(t, 0, info.regionBegins)
	require.Equal(t, 0, info.regionEnds)
	require.Empty(t, info.logs)
}
