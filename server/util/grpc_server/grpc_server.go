package grpc_server

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/filters"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	hlpb "github.com/buildbuddy-io/buildbuddy/proto/health"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
)

var (
	gRPCOverHTTPPortEnabled = flag.Bool("app.grpc_over_http_port_enabled", false, "Cloud-Only")
	// Support large BEP messages: https://github.com/bazelbuild/bazel/issues/12050
	gRPCMaxRecvMsgSizeBytes = flag.Int("app.grpc_max_recv_msg_size_bytes", 50000000, "Configures the max GRPC receive message size [bytes]")

	gRPCPort  = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")
	gRPCSPort = flag.Int("grpcs_port", 1986, "The port to listen for gRPCS traffic on")

	internalGRPCPort  = flag.Int("internal_grpc_port", 1987, "The port to listen for internal gRPC traffic on")
	internalGRPCSPort = flag.Int("internal_grpcs_port", 1988, "The port to listen for internal gRPCS traffic on")

	enablePrometheusHistograms = flag.Bool("app.enable_prometheus_histograms", true, "If true, collect prometheus histograms for all RPCs")
)

func Port() int {
	return *gRPCPort
}

type RegisterServices func(server *grpc.Server, env environment.Env)

func RegisterGRPCServer(env environment.Env, regServices RegisterServices) error {
	grpcServer, err := NewGRPCServer(env, *gRPCPort, nil, regServices)
	if err != nil {
		return err
	}
	env.SetGRPCServer(grpcServer)
	if *gRPCOverHTTPPortEnabled {
		env.SetMux(&gRPCMux{
			env.GetMux(),
			grpcServer,
		})
	}
	return nil
}

func RegisterGRPCSServer(env environment.Env, regServices RegisterServices) error {
	if !env.GetSSLService().IsEnabled() {
		return nil
	}
	creds, err := env.GetSSLService().GetGRPCSTLSCreds()
	if err != nil {
		return status.InternalErrorf("Error getting SSL creds: %s", err)
	}
	grpcsServer, err := NewGRPCServer(env, *gRPCSPort, grpc.Creds(creds), regServices)
	if err != nil {
		return err
	}
	env.SetGRPCSServer(grpcsServer)
	return nil
}

func RegisterInternalGRPCServer(env environment.Env, regServices RegisterServices) error {
	if *internalGRPCPort == 0 {
		return nil
	}

	grpcServer, err := NewGRPCServer(env, *internalGRPCPort, nil, regServices)
	if err != nil {
		return err
	}
	env.SetInternalGRPCServer(grpcServer)
	return nil
}

func RegisterInternalGRPCSServer(env environment.Env, regServices RegisterServices) error {
	if *internalGRPCSPort == 0 {
		return nil
	}

	if !env.GetSSLService().IsEnabled() {
		return nil
	}
	creds, err := env.GetSSLService().GetGRPCSTLSCreds()
	if err != nil {
		return status.InternalErrorf("Error getting SSL creds: %s", err)
	}
	grpcsServer, err := NewGRPCServer(env, *internalGRPCSPort, grpc.Creds(creds), regServices)
	if err != nil {
		return err
	}
	env.SetInternalGRPCSServer(grpcsServer)
	return nil
}

func NewGRPCServer(env environment.Env, port int, credentialOption grpc.ServerOption, regServices RegisterServices) (*grpc.Server, error) {
	// Initialize our gRPC server (and fail early if that doesn't happen).
	hostAndPort := fmt.Sprintf("%s:%d", env.GetListenAddr(), port)

	lis, err := net.Listen("tcp", hostAndPort)
	if err != nil {
		return nil, status.InternalErrorf("Failed to listen: %s", err)
	}

	grpcOptions := CommonGRPCServerOptions(env)
	if credentialOption != nil {
		grpcOptions = append(grpcOptions, credentialOption)
		log.Infof("gRPCS listening on %s", hostAndPort)
	} else {
		log.Infof("gRPC listening on %s", hostAndPort)
	}

	grpcServer := grpc.NewServer(grpcOptions...)

	// Support reflection so that tools like grpc-cli (aka stubby) can
	// enumerate our services and call them.
	reflection.Register(grpcServer)

	// Support prometheus grpc metrics.
	grpc_prometheus.Register(grpcServer)

	if *enablePrometheusHistograms {
		grpc_prometheus.EnableHandlingTimeHistogram()
	}

	// Register health check service.
	hlpb.RegisterHealthServer(grpcServer, env.GetHealthChecker())

	regServices(grpcServer, env)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	env.GetHealthChecker().RegisterShutdownFunction(GRPCShutdownFunc(grpcServer))
	return grpcServer, nil
}

func GRPCShutdown(ctx context.Context, grpcServer *grpc.Server) error {
	// Attempt to graceful stop this grpcServer. Graceful stop will
	// disallow new connections, but existing ones are allowed to
	// finish. To ensure this doesn't hang forever, we also kick off
	// a goroutine that will hard stop the server 100ms before the
	// shutdown function deadline.
	deadline, ok := ctx.Deadline()
	if !ok {
		grpcServer.Stop()
		return nil
	}
	delay := deadline.Sub(time.Now()) - (100 * time.Millisecond)
	ctx, cancel := context.WithTimeout(ctx, delay)
	go func() {
		select {
		case <-ctx.Done():
			log.Infof("Graceful stop of GRPC server succeeded.")
			grpcServer.Stop()
		case <-time.After(delay):
			log.Warningf("Hard-stopping GRPC Server!")
			grpcServer.Stop()
		}
	}()
	grpcServer.GracefulStop()
	cancel()
	return nil
}

func GRPCShutdownFunc(grpcServer *grpc.Server) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		return GRPCShutdown(ctx, grpcServer)
	}
}

func propagateInvocationIDToSpan(ctx context.Context) {
	invocationId := bazel_request.GetInvocationID(ctx)
	if invocationId == "" {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("invocation_id", invocationId))
}

func propagateInvocationIDToSpanUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		propagateInvocationIDToSpan(ctx)
		return handler(ctx, req)
	}
}

func propagateInvocationIDToSpanStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		propagateInvocationIDToSpan(stream.Context())
		return handler(srv, stream)
	}
}

func CommonGRPCServerOptions(env environment.Env) []grpc.ServerOption {
	return []grpc.ServerOption{
		filters.GetUnaryInterceptor(env),
		filters.GetStreamInterceptor(env),
		grpc.ChainUnaryInterceptor(otelgrpc.UnaryServerInterceptor(), propagateInvocationIDToSpanUnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(otelgrpc.StreamServerInterceptor(), propagateInvocationIDToSpanStreamServerInterceptor()),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.MaxRecvMsgSize(*gRPCMaxRecvMsgSizeBytes),
		// Set to avoid errors: Bandwidth exhausted HTTP/2 error code: ENHANCE_YOUR_CALM Received Goaway too_many_pings
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second, // If a client pings more than once every 10 seconds, terminate the connection
			PermitWithoutStream: true,             // Allow pings even when there are no active streams
		}),
	}
}

func MaxRecvMsgSizeBytes() int {
	return *gRPCMaxRecvMsgSizeBytes
}

type gRPCMux struct {
	interfaces.HttpServeMux
	grpcServer *grpc.Server
}

func (g *gRPCMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor == 2 && strings.HasPrefix(
		r.Header.Get("Content-Type"), "application/grpc") {
		g.grpcServer.ServeHTTP(w, r)
	} else {
		g.HttpServeMux.ServeHTTP(w, r)
	}
}

func GRPCPort() int {
	return *gRPCPort
}

func GRPCSPort() int {
	return *gRPCSPort
}
