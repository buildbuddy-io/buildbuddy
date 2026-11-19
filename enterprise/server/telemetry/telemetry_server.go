package telemetry

import (
	"context"
	"flag"
	"fmt"
	"net"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/rpc/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"gorm.io/gorm/clause"

	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

var (
	telemetryPort   = flag.Int("telemetry_port", 9099, "The port on which to listen for telemetry events")
	serveOnGRPCPort = flag.Bool("telemetry.serve_on_grpc_port", false, "If true, also accept telemetry events on the main gRPC port(s), in addition to the telemetry port.")
	_               = flag.Bool("verbose_telemetry_server", false, "If true; print telemetry server information")
)

type TelemetryServer struct {
	env environment.Env
	h   interfaces.DBHandle
}

func NewTelemetryServer(env environment.Env, h interfaces.DBHandle) *TelemetryServer {
	return &TelemetryServer{
		env: env,
		h:   h,
	}
}

// Register starts the telemetry server on the telemetry port and, if
// telemetry.serve_on_grpc_port is set, also serves it on the main gRPC port(s).
func Register(env *real_environment.RealEnv) error {
	t := NewTelemetryServer(env, env.GetDBHandle())
	if err := t.startOnTelemetryPort(); err != nil {
		return err
	}
	if *serveOnGRPCPort {
		env.SetTelemetryServer(t)
	}
	return nil
}

func (t *TelemetryServer) startOnTelemetryPort() error {
	if *telemetryPort < 0 {
		log.Debug("Telemetry port disabled")
		return nil
	}
	log.Debug("Telemetry port enabled")

	grpcOptions := []grpc.ServerOption{
		interceptors.GetUnaryInterceptor(t.env),
		grpc.UnaryInterceptor(grpc_server.Metrics().UnaryServerInterceptor()),
	}

	grpcServer := grpc.NewServer(grpcOptions...)
	telpb.RegisterTelemetryServer(grpcServer, t)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *telemetryPort))
	if err != nil {
		return status.UnavailableErrorf("listen on telemetry port: %s", err)
	}
	go func() {
		err := grpcServer.Serve(lis)
		log.Fatal(err.Error())
	}()
	return nil
}

func (t *TelemetryServer) LogTelemetry(ctx context.Context, req *telpb.LogTelemetryRequest) (*telpb.LogTelemetryResponse, error) {
	log.Debugf("Telemetry data received: %+v", req)

	status := &statuspb.Status{
		Code:    int32(codes.OK),
		Message: "Success",
	}
	for _, logProto := range req.Log {
		log := recordFromLogProto(logProto)
		if err := t.insertLogIfNotExists(ctx, log); err != nil {
			status = &statuspb.Status{
				Code:    int32(codes.DataLoss),
				Message: err.Error(),
			}
		}
	}

	return &telpb.LogTelemetryResponse{
		Status: status,
	}, nil
}

func (t *TelemetryServer) insertLogIfNotExists(ctx context.Context, telemetryLog *tables.TelemetryLog) error {
	return t.h.GORM(ctx, "telemetry_server_create_log").Clauses(clause.OnConflict{DoNothing: true}).Create(telemetryLog).Error
}

func recordFromLogProto(logProto *telpb.TelemetryLog) *tables.TelemetryLog {
	telemetryLog := &tables.TelemetryLog{
		InstallationUUID: logProto.InstallationUuid,
		InstanceUUID:     logProto.InstanceUuid,
		TelemetryLogUUID: logProto.LogUuid,
		RecordedAtUsec:   logProto.RecordedAtUsec,
		AppVersion:       logProto.AppVersion,
		AppURL:           logProto.AppUrl,
		Hostname:         logProto.Hostname,
	}

	if logProto.TelemetryStat != nil {
		telemetryLog.InvocationCount = logProto.TelemetryStat.InvocationCount
		telemetryLog.RegisteredUserCount = logProto.TelemetryStat.RegisteredUserCount
		telemetryLog.BazelUserCount = logProto.TelemetryStat.BazelUserCount
		telemetryLog.BazelHostCount = logProto.TelemetryStat.BazelHostCount
	}

	if logProto.TelemetryFeature != nil {
		telemetryLog.FeatureCacheEnabled = logProto.TelemetryFeature.CacheEnabled
		telemetryLog.FeatureRBEEnabled = logProto.TelemetryFeature.RbeEnabled
		telemetryLog.FeatureAPIEnabled = logProto.TelemetryFeature.ApiEnabled
		telemetryLog.FeatureAuthEnabled = logProto.TelemetryFeature.AuthEnabled
	}

	return telemetryLog
}
