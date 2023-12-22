package testgrpc

import (
	"context"
	"math/rand"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mwitkow/grpc-proxy/proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// Server is a gRPC server used for testing.
//
// Example usage:
//
//	testServer := testgrpc.NewServer(t, env)
//	foopb.RegisterFooServer(testServer.Server, env.GetFooServer())
//	testServer.Start()
//	conn := ts.Dial(ctx)
//	foo := foopb.NewFooClient(conn)
type Server struct {
	*grpc.Server
	t   testing.TB
	lis *bufconn.Listener
}

// NewServer returns a gRPC server for testing, configured with the common
// BuildBuddy gRPC options. It gracefully stops the server as part of test
// cleanup.
func NewServer(t testing.TB, env environment.Env) *Server {
	server := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
	return &Server{
		Server: server,
		t:      t,
		lis:    bufconn.Listen(1024 * 1024),
	}
}

// Start starts the test server.
// This should be called after registering services.
func (s *Server) Start() {
	go func() {
		s.t.Cleanup(func() {
			s.Server.GracefulStop()
		})
		err := s.Server.Serve(s.lis)
		require.NoError(s.t, err)
	}()
}

// Dial connects to the server. It returns a connection that can be used to
// create gRPC clients.
func (s *Server) Dial(ctx context.Context, opts ...grpc.DialOption) *grpc.ClientConn {
	bufDialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return s.lis.DialContext(ctx)
	}
	dialOptions := grpc_client.CommonGRPCClientOptions()
	dialOptions = append(dialOptions, grpc.WithContextDialer(bufDialer))
	dialOptions = append(dialOptions, grpc.WithInsecure())
	dialOptions = append(dialOptions, opts...)
	conn, err := grpc.DialContext(ctx, "bufnet", dialOptions...)
	require.NoError(s.t, err)
	return conn
}

// Proxy is a basic in-process gRPC L7 proxy for use in tests.
//
// A proxy must be configured with a Director, which is just a function that
// decides how to connect RPCs to backends. You can use RandomDialer to load
// balance randomly (similar to a "real" LB), or define a custom function. A
// custom function is useful for unit testing specific connectivity scenarios.
//
// For example, if you want to test that a client automatically retries broken
// stream connections, you can write a director function which ensures that any
// requests to "MyStreamingRPC" are connected to app1, then start a streaming
// RPC, then atomically (1) shut down app1 and (2) signal the director to start
// directing new "MyStreamingRPC" requests to app2.
//
// Example usage:
//
//	// Start some apps
//	app1 := buildbuddy.Run(t)
//	app2 := buildbuddy.Run(t)
//	// Define a director
//	director := testgrpc.RandomDialer(app1.GRPCAddress(), app2.GRPCAddress())
//	// Start the proxy server
//	proxy := testgrpc.StartProxy(t, director)
//	// Connect to the proxy and make an RPC
//	conn := proxy.Dial()
//	client := examplepb.NewExampleClient(conn)
//	client.SomeRPCMethod(/* ... */)
type Proxy struct {
	t        *testing.T
	Addr     net.Addr
	Director Director
	Conn     *grpc.ClientConn
}

// Director decides how to connect a client request to a backend.
type Director func(ctx context.Context, fullMethodName string) (ctxOut context.Context, conn *grpc.ClientConn, err error)

// StartProxy runs a test-scoped gRPC proxy. The given director func decides how
// to connect a client request to a backend. If needed, the func can be nil
// initially and reconfigured later by setting Director on the returned proxy.
func StartProxy(t *testing.T, director Director) *Proxy {
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	p := &Proxy{
		t:        t,
		Addr:     lis.Addr(),
		Director: director,
	}
	director = func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
		require.NotNil(t, p.Director, "Proxy.Director is nil")
		return p.Director(ctx, fullMethodName)
	}
	handler := grpc.UnknownServiceHandler(proxy.TransparentHandler(proxy.StreamDirector(director)))
	server := grpc.NewServer(handler)
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	return p
}

func (p *Proxy) GRPCTarget() string {
	return "grpc://" + p.Addr.String()
}

func (p *Proxy) Dial() *grpc.ClientConn {
	conn, err := grpc_client.DialSimpleWithoutPooling(p.GRPCTarget())
	require.NoError(p.t, err)
	return conn
}

// RandomDialer returns a Director that randomly picks one of the given targets.
func RandomDialer(targets ...string) Director {
	return func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
		var cc *grpc.ClientConn
		var err error
		r := rand.Intn(len(targets))
		for i := 0; i < len(targets); i++ {
			target := targets[(r+i)%len(targets)]
			cc, err = grpc_client.DialSimpleWithoutPooling(target)
			if status.IsUnavailableError(err) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			break
		}
		if cc == nil {
			return nil, nil, status.UnavailableError("RandomDialer: all backends are unavailable")
		}
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md.Copy())
		}
		return ctx, cc, nil
	}
}
