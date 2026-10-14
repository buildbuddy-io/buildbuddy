package interceptors_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime/pprof"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

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

func TestOriginMethodHonorsTrustedHeader(t *testing.T) {
	enableLabels(t)
	client, a, _ := startHops(t, false)

	ctx := metadata.AppendToOutgoingContext(context.Background(),
		authutil.ClientIdentityHeaderName, interfaces.ClientIdentityApp,
		originMethodHeader, "/orig/Method")
	_, err := client.Ping(ctx, &pspb.PingRequest{})
	require.NoError(t, err)

	got := <-a.seen
	require.Equal(t, pingMethod, got.grpcMethod)
	require.Equal(t, "/orig/Method", got.originMethod)
}

func TestOriginMethodSingleHop(t *testing.T) {
	enableLabels(t)
	client, a, _ := startHops(t, false)

	_, err := client.Ping(context.Background(), &pspb.PingRequest{})
	require.NoError(t, err)

	require.Equal(t, hopObservation{grpcMethod: pingMethod, originMethod: pingMethod}, <-a.seen)
}

func TestOriginMethodNotForwardedWhenLabelsDisabled(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trusted bool
	}{
		{name: "untrusted caller", trusted: false},
		{name: "trusted caller", trusted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, a, b := startHops(t, true)

			ctx := metadata.AppendToOutgoingContext(context.Background(), originMethodHeader, "/fake/Method")
			if tc.trusted {
				ctx = metadata.AppendToOutgoingContext(ctx, authutil.ClientIdentityHeaderName, interfaces.ClientIdentityApp)
			}
			_, err := client.Ping(ctx, &pspb.PingRequest{})
			require.NoError(t, err)

			<-a.seen
			got := <-b.seen
			require.Empty(t, got.originMethod)
			require.Empty(t, got.header)
		})
	}
}
