package interceptors_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
)

type pingServer struct {
	lastClientIP string
}

func (p *pingServer) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	p.lastClientIP = clientip.Get(ctx)
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

func (f *fakeClientIdentityService) IdentityHeader(si *interfaces.ClientIdentity, expiration time.Duration) (string, error) {
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
