package grpc_server

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_forward"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/reflection"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
	hlpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	gRPCOverHTTPPortEnabled = flag.Bool("app.grpc_over_http_port_enabled", true, "Enables grpc traffic to be served over the http port.")

	// Support large BEP messages: https://github.com/bazelbuild/bazel/issues/12050
	gRPCMaxRecvMsgSizeBytes           = flag.Int("grpc_max_recv_msg_size_bytes", 50_000_000, "Configures the max GRPC receive message size [bytes]")
	deprecatedGRPCMaxRecvMsgSizeBytes = flag.Int("app.grpc_max_recv_msg_size_bytes", 50_000_000, "DEPRECATED: use --grpc_max_recv_msg_size_bytes instead")

	gRPCPort  = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")
	gRPCSPort = flag.Int("grpcs_port", 1986, "The port to listen for gRPCS traffic on")

	internalGRPCPort  = flag.Int("internal_grpc_port", 1987, "The port to listen for internal gRPC traffic on")
	internalGRPCSPort = flag.Int("internal_grpcs_port", 1988, "The port to listen for internal gRPCS traffic on")

	enablePrometheusHistograms = flag.Bool("app.enable_prometheus_histograms", true, "If true, collect prometheus histograms for all RPCs")
)

func GRPCPort() int {
	return *gRPCPort
}

func GRPCSPort() int {
	return *gRPCSPort
}

func InternalGRPCPort() int {
	return *internalGRPCPort
}

func InternalGRPCSPort() int {
	return *internalGRPCSPort
}

func MaxRecvMsgSizeBytes() int {
	if *deprecatedGRPCMaxRecvMsgSizeBytes > *gRPCMaxRecvMsgSizeBytes {
		return *deprecatedGRPCMaxRecvMsgSizeBytes
	}
	return *gRPCMaxRecvMsgSizeBytes
}

type GRPCServerConfig struct {
	ExtraChainedUnaryInterceptors  []grpc.UnaryServerInterceptor
	ExtraChainedStreamInterceptors []grpc.StreamServerInterceptor
}

type GRPCServer struct {
	env      environment.Env
	hostPort string
	server   *grpc.Server
}

func (b *GRPCServer) GetServer() *grpc.Server {
	return b.server
}

func New(env environment.Env, port int, ssl bool, config GRPCServerConfig) (*GRPCServer, error) {
	b := &GRPCServer{env: env}
	if ssl && !env.GetSSLService().IsEnabled() {
		return nil, status.InvalidArgumentError("GRPCS requires SSL Service")
	}
	b.hostPort = fmt.Sprintf("%s:%d", b.env.GetListenAddr(), port)

	var credentialOption grpc.ServerOption = nil
	if ssl {
		creds, err := b.env.GetSSLService().GetGRPCSTLSCreds()
		if err != nil {
			return nil, status.InternalErrorf("Error getting SSL creds: %s", err)
		}
		credentialOption = grpc.Creds(creds)
	}

	grpcOptions := CommonGRPCServerOptionsWithConfig(env, config)
	if credentialOption != nil {
		grpcOptions = append(grpcOptions, credentialOption)
		log.Infof("gRPCS listening on %s", b.hostPort)
	} else {
		log.Infof("gRPC listening on %s", b.hostPort)
	}
	if fwdingOptions := grpc_forward.GetForwardingServerOption(); fwdingOptions != nil {
		grpcOptions = append(grpcOptions, fwdingOptions)
	}
	b.server = grpc.NewServer(grpcOptions...)

	// Support reflection so that tools like grpc-cli (aka stubby) can
	// enumerate our services and call them.
	reflection.Register(b.server)

	// Initialize timeseries for all known grpc methods.
	Metrics().InitializeMetrics(b.server)

	// Register health check service.
	hlpb.RegisterHealthServer(b.server, b.env.GetHealthChecker())
	return b, nil
}

func (b *GRPCServer) Start() error {
	lis, err := net.Listen("tcp", b.hostPort)
	if err != nil {
		return status.InternalErrorf("Failed to listen: %s", err)
	}

	go func() {
		_ = b.server.Serve(lis)
	}()
	b.env.GetHealthChecker().RegisterShutdownFunction(GRPCShutdownFunc(b.server))
	return nil
}

func EnableGRPCOverHTTP(env *real_environment.RealEnv, grpcServer *grpc.Server) {
	if *gRPCOverHTTPPortEnabled {
		env.SetMux(&gRPCMux{
			env.GetMux(),
			grpcServer,
		})
	}
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
	delay := time.Until(deadline) - (100 * time.Millisecond)
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

func propagateActionIDToSpan(ctx context.Context) {
	actionId := bazel_request.GetActionID(ctx)
	if actionId == "" {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("action_id", actionId))
}

func propagateInvocationIDToSpan(ctx context.Context) {
	invocationId := bazel_request.GetInvocationID(ctx)
	if invocationId == "" {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("invocation_id", invocationId))
}

func propagateRequestMetadataIDsToSpanUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		propagateInvocationIDToSpan(ctx)
		propagateActionIDToSpan(ctx)
		return handler(ctx, req)
	}
}

func propagateRequestMetadataIDsToSpanStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		propagateInvocationIDToSpan(ctx)
		propagateActionIDToSpan(ctx)
		return handler(srv, stream)
	}
}

func CommonGRPCServerOptions(env environment.Env) []grpc.ServerOption {
	return CommonGRPCServerOptionsWithConfig(env, GRPCServerConfig{})
}

// Metrics returns middleware that can be used to obtain gRPC interceptors
// that add prometheus metrics for handled RPCs.
//
// e.g. grpc_server.Metrics().UnaryServerInterceptor()
//
// N.B. OnceValue is used to ensure that prometheus.MustRegister is only called
// once.
var Metrics = sync.OnceValue(func() *grpc_prometheus.ServerMetrics {
	var opts []grpc_prometheus.ServerMetricsOption
	if *enablePrometheusHistograms {
		opts = append(opts, grpc_prometheus.WithServerHandlingTimeHistogram(
			grpc_prometheus.WithHistogramBuckets(
				[]float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10})))
	}
	ms := grpc_prometheus.NewServerMetrics(opts...)
	prometheus.MustRegister(ms)
	return ms
})

func CommonGRPCServerOptionsWithConfig(env environment.Env, config GRPCServerConfig) []grpc.ServerOption {
	return []grpc.ServerOption{
		interceptors.GetUnaryInterceptor(env, config.ExtraChainedUnaryInterceptors...),
		interceptors.GetStreamInterceptor(env, config.ExtraChainedStreamInterceptors...),
		grpc.ChainUnaryInterceptor(
			otelgrpc.UnaryServerInterceptor(otelgrpc.WithMeterProvider(noop.NewMeterProvider())),
			propagateRequestMetadataIDsToSpanUnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(
			otelgrpc.StreamServerInterceptor(otelgrpc.WithMeterProvider(noop.NewMeterProvider())),
			propagateRequestMetadataIDsToSpanStreamServerInterceptor()),
		grpc.StreamInterceptor(Metrics().StreamServerInterceptor()),
		grpc.UnaryInterceptor(Metrics().UnaryServerInterceptor()),
		experimental.BufferPool(mem.DefaultBufferPool()),
		grpc.MaxRecvMsgSize(MaxRecvMsgSizeBytes()),
		KeepaliveEnforcementPolicy(),
	}
}

func KeepaliveEnforcementPolicy() grpc.ServerOption {
	// Set to avoid errors: Bandwidth exhausted HTTP/2 error code: ENHANCE_YOUR_CALM Received Goaway too_many_pings
	return grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             10 * time.Second, // If a client pings more than once every 10 seconds, terminate the connection
		PermitWithoutStream: true,             // Allow pings even when there are no active streams
	})
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
