package testgrpc

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mwitkow/grpc-proxy/proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

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
	director Director
	mu       sync.Mutex // protects director
}

// Director decides how to connect a client request to a backend. The proxy
// takes ownership of the returned conn and closes it when the RPC finishes.
type Director func(ctx context.Context, fullMethodName string) (ctxOut context.Context, conn *grpc.ClientConn, err error)

// StartProxy runs a test-scoped gRPC proxy. The given director func decides how
// to connect a client request to a backend. If needed, the func can be nil
// initially and reconfigured later by setting Director on the returned proxy.
func StartProxy(t *testing.T) *Proxy {
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	p := &Proxy{
		t:        t,
		Addr:     lis.Addr(),
		director: nil,
	}
	// Each RPC gets its own handler so that it can close the conn the
	// director returned once the RPC finishes.
	handler := func(srv any, stream grpc.ServerStream) error {
		var conn *grpc.ClientConn
		director := func(ctx context.Context, fullMethodName string) (context.Context, grpc.ClientConnInterface, error) {
			p.mu.Lock()
			defer p.mu.Unlock()
			require.NotNil(t, p.director, "Proxy.Director is nil")
			outCtx, c, err := p.director(ctx, fullMethodName)
			if err != nil {
				return nil, nil, err
			}
			conn = c
			return outCtx, c, nil
		}
		defer func() {
			if conn != nil {
				conn.Close()
			}
		}()
		return proxy.TransparentHandler(director)(srv, stream)
	}
	server := grpc.NewServer(grpc.UnknownServiceHandler(handler))
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	return p
}

func (p *Proxy) SetDirector(director Director) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.director = director
}

func (p *Proxy) GRPCTarget() string {
	return "grpc://" + p.Addr.String()
}

// Dial returns a connection to the proxy, which is closed when the test that
// started the proxy ends.
func (p *Proxy) Dial() *grpc.ClientConn {
	conn, err := grpc_client.DialSimpleWithoutPooling(p.GRPCTarget())
	require.NoError(p.t, err)
	p.t.Cleanup(func() { conn.Close() })
	return conn
}

// RandomDialer returns a Director that randomly picks one of the given targets.
func RandomDialer(targets []string) Director {
	return func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
		var cc *grpc.ClientConn
		var err error
		r := rand.Intn(len(targets))
		for i := range targets {
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

// Simulates an outgoing RPC by copying outgoing metadatam from the provided
// context into new incoming metadata which is added to the provided context
// and returned.
func OutgoingToIncomingContext(t *testing.T, ctx context.Context) context.Context {
	outgoingMD, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	return metadata.NewIncomingContext(ctx, outgoingMD)
}
