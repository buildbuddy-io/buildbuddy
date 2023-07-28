package testgrpc

import (
	"context"
	"math/rand"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mwitkow/grpc-proxy/proxy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// StreamDirector decides how to connect a client request to a backend.
type StreamDirector func(ctx context.Context, fullMethodName string) (ctxOut context.Context, conn *grpc.ClientConn, err error)

// Proxy is a basic in-process gRPC L7 proxy for use in tests.
type Proxy struct {
	t        *testing.T
	Addr     net.Addr
	Director StreamDirector
	Conn     *grpc.ClientConn
}

// StartProxy runs a test-scoped gRPC proxy. The given director func decides how
// to connect a client request to a backend. If needed, the func can be nil
// initially and reconfigured later by setting Director on the returned proxy.
func StartProxy(t *testing.T, director StreamDirector) *Proxy {
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
	conn, err := grpc_client.DialTarget(p.GRPCTarget())
	require.NoError(p.t, err)
	return conn
}

// RandomDialer returns a StreamDirector that dials a random backend from the
// given list.
func RandomDialer(targets ...string) proxy.StreamDirector {
	return func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
		var cc *grpc.ClientConn
		var err error
		r := rand.Intn(len(targets))
		for i := 0; i < len(targets); i++ {
			target := targets[(r+i)%len(targets)]
			cc, err = grpc_client.DialTarget(target)
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
