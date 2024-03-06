package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/mwitkow/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	port    = flag.Int("port", 3080, "TCP port to serve gRPC traffic on")
	targets = flag.Slice("target", []string{}, "gRPC targets, e.g. grpc://localhost:1985")
)

// Director decides how to connect a client request to a backend.
type Director func(ctx context.Context, fullMethodName string) (ctxOut context.Context, conn *grpc.ClientConn, err error)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		return err
	}
	targets := *targets
	director := func(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error) {
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
	handler := grpc.UnknownServiceHandler(proxy.TransparentHandler(proxy.StreamDirector(director)))
	server := grpc.NewServer(handler)
	log.Infof("gRPC proxy listening on %s routing randomly to %s", lis.Addr(), targets)
	return server.Serve(lis)
}
