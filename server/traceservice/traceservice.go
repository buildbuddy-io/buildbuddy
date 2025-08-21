package traceservice

import (
	"context"
	"encoding/hex"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	ottpb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

var (
	// XXX: this should just use the same flag as the tracing lib
	bazelTraceEndpoint = flag.String("app.bazel_trace_endpoint", "", "")
)

type service struct {
	ottpb.UnimplementedTraceServiceServer

	client otlptrace.Client
}

func Register(env *real_environment.RealEnv) error {
	if *bazelTraceEndpoint == "" {
		return status.InvalidArgumentError("bazel_trace_endpoint is not set")
	}

	c := otlptracegrpc.NewClient(otlptracegrpc.WithEndpoint(*bazelTraceEndpoint), otlptracegrpc.WithInsecure())
	if err := c.Start(context.Background()); err != nil {
		return status.WrapError(err, "otlptrace.Start")
	}

	s := &service{client: c}
	env.SetTraceServer(s)
	return nil
}

func (s *service) Export(ctx context.Context, request *ottpb.ExportTraceServiceRequest) (*ottpb.ExportTraceServiceResponse, error) {
	//log.Infof("Received Export:\n%v", prototext.Format(request))
	for _, rs := range request.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				log.Infof("SPAN %s trace ID %s span id %s", s.GetName(), hex.EncodeToString(s.GetTraceId()), hex.EncodeToString(s.GetSpanId()))
			}
		}
	}

	if err := s.client.UploadTraces(ctx, request.GetResourceSpans()); err != nil {
		log.Warningf("Failed to upload traces: %s", err)
	}

	log.CtxInfof(ctx, "Successfully uploaded %d traces", len(request.GetResourceSpans()))

	return &ottpb.ExportTraceServiceResponse{}, nil
}
