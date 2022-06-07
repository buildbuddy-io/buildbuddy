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
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	hlpb "github.com/buildbuddy-io/buildbuddy/proto/health"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	_ "google.golang.org/grpc/encoding/gzip" // imported for side effects; DO NOT REMOVE.
)

var (
	gRPCOverHTTPPortEnabled = flag.Bool("app.grpc_over_http_port_enabled", false, "Cloud-Only")
	// Support large BEP messages: https://github.com/bazelbuild/bazel/issues/12050
	gRPCMaxRecvMsgSizeBytes = flag.Int("app.grpc_max_recv_msg_size_bytes", 50000000, "Configures the max GRPC receive message size [bytes]")

	gRPCPort  = flag.Int("grpc_port", 1985, "The port to listen for gRPC traffic on")
	gRPCSPort = flag.Int("grpcs_port", 1986, "The port to listen for gRPCS traffic on")
)

func RegisterGRPCServer(env environment.Env) error {
	grpcServer, err := NewGRPCServer(env, *gRPCPort, nil)
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

func RegisterGRPCSServer(env environment.Env) error {
	if !env.GetSSLService().IsEnabled() {
		return nil
	}
	creds, err := env.GetSSLService().GetGRPCSTLSCreds()
	if err != nil {
		return status.InternalErrorf("Error getting SSL creds: %s", err)
	}
	grpcsServer, err := NewGRPCServer(env, *gRPCSPort, grpc.Creds(creds))
	if err != nil {
		return err
	}
	env.SetGRPCSServer(grpcsServer)
	return nil
}

func NewGRPCServer(env environment.Env, port int, credentialOption grpc.ServerOption) (*grpc.Server, error) {
	// Initialize our gRPC server (and fail early if that doesn't happen).
	hostAndPort := fmt.Sprintf("%s:%d", env.GetListenAddr(), port)

	lis, err := net.Listen("tcp", hostAndPort)
	if err != nil {
		return nil, status.InternalErrorf("Failed to listen: %s", err)
	}

	grpcOptions := CommonGRPCServerOptions(env)
	if credentialOption != nil {
		grpcOptions = append(grpcOptions, credentialOption)
		log.Printf("gRPCS listening on http://%s", hostAndPort)
	} else {
		log.Printf("gRPC listening on http://%s", hostAndPort)
	}

	grpcServer := grpc.NewServer(grpcOptions...)

	// Support reflection so that tools like grpc-cli (aka stubby) can
	// enumerate our services and call them.
	reflection.Register(grpcServer)

	// Support prometheus grpc metrics.
	grpc_prometheus.Register(grpcServer)

	// DISABLED in prod: enabling these causes unnecessary allocations
	// that substantially (50%+ QPS) impact performance.
	// grpc_prometheus.EnableHandlingTimeHistogram()

	// Start Build-Event-Protocol and Remote-Cache services.
	pepb.RegisterPublishBuildEventServer(grpcServer, env.GetBuildEventServer())

	if casServer := env.GetCASServer(); casServer != nil {
		// Register to handle content addressable storage (CAS) messages.
		repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	}
	if bsServer := env.GetByteStreamServer(); bsServer != nil {
		// Register to handle bytestream (upload and download) messages.
		bspb.RegisterByteStreamServer(grpcServer, bsServer)
	}
	if acServer := env.GetActionCacheServer(); acServer != nil {
		// Register to handle action cache (upload and download) messages.
		repb.RegisterActionCacheServer(grpcServer, acServer)
	}
	if pushServer := env.GetPushServer(); pushServer != nil {
		rapb.RegisterPushServer(grpcServer, pushServer)
	}
	if fetchServer := env.GetFetchServer(); fetchServer != nil {
		rapb.RegisterFetchServer(grpcServer, fetchServer)
	}
	if rexec := env.GetRemoteExecutionService(); rexec != nil {
		repb.RegisterExecutionServer(grpcServer, rexec)
	}
	if scheduler := env.GetSchedulerService(); scheduler != nil {
		scpb.RegisterSchedulerServer(grpcServer, scheduler)
	}
	repb.RegisterCapabilitiesServer(grpcServer, env.GetCapabilitiesServer())

	bbspb.RegisterBuildBuddyServiceServer(grpcServer, env.GetBuildBuddyServer())

	// Register API Server as a gRPC service.
	if api := env.GetAPIService(); api != nil {
		apipb.RegisterApiServiceServer(grpcServer, api)
	}

	// Register health check service.
	hlpb.RegisterHealthServer(grpcServer, env.GetHealthChecker())

	go func() {
		grpcServer.Serve(lis)
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

func CommonGRPCServerOptions(env environment.Env) []grpc.ServerOption {
	return []grpc.ServerOption{
		filters.GetUnaryInterceptor(env),
		filters.GetStreamInterceptor(env),
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
