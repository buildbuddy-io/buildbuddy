package tracing

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	tpb "github.com/buildbuddy-io/buildbuddy/proto/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var (
	// TODO: use this project ID or deprecate it. It is currently unreferenced.
	traceProjectID       = flag.String("app.trace_project_id", "", "Optional GCP project ID to export traces to. If not specified, determined from default credentials or metadata server if running on GCP.")
	traceJaegerCollector = flag.String("app.trace_jaeger_collector", "", "Address of the Jager collector endpoint where traces will be sent.")
	traceServiceName     = flag.String("app.trace_service_name", "", "Name of the service to associate with traces.")
)

const (
	resourceDetectionTimeout      = 5 * time.Second
	buildBuddyInstrumentationName = "buildbuddy.io"
)

func Configure(healthChecker interfaces.HealthChecker) error {
	if *traceJaegerCollector == "" {
		return status.InvalidArgumentErrorf("Tracing enabled but Jaeger collector endpoint is not set.")
	}

	traceExporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(*traceJaegerCollector)))
	if err != nil {
		log.Warningf("Could not initialize Cloud Trace exporter: %s", err)
		return nil
	}

	var resourceAttrs []attribute.KeyValue
	if *traceServiceName != "" {
		resourceAttrs = append(resourceAttrs, semconv.ServiceNameKey.String(*traceServiceName))
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	healthChecker.RegisterShutdownFunction(func(ctx context.Context) error {
		return bsp.Shutdown(ctx)
	})

	ctx, cancel := context.WithTimeout(context.Background(), resourceDetectionTimeout)
	defer cancel()
	res, err := resource.New(ctx,
		resource.WithDetectors(&gcp.GKE{}, &gcp.GCE{}),
		resource.WithAttributes(resourceAttrs...))
	if err != nil {
		log.Warningf("Could not automatically detect resource information for tracing: %s", err)
		res = resource.NewSchemaless(resourceAttrs...)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	// Re-enable this if GCS tracing is fixed to not include blob names in span names
	// Necessary imports: "go.opentelemetry.io/otel/bridge/opencensus"
	//                    octrace "go.opencensus.io/trace"
	//
	// tracer := otel.GetTracerProvider().Tracer(buildBuddyInstrumentationName)
	// octrace.DefaultTracer = opencensus.NewTracer(tracer)
	propagator := propagation.NewCompositeTextMapPropagator(propagation.Baggage{}, propagation.TraceContext{})
	otel.SetTextMapPropagator(propagator)
	log.Info("Tracing enabled.")
	return nil
}

type SetMetadata func(m *tpb.Metadata)

type traceMetadataProtoCarrier struct {
	metadata    map[string]string
	setMetadata SetMetadata
}

func newTraceMetadataProtoCarrier(metadata *tpb.Metadata, setMetadata SetMetadata) *traceMetadataProtoCarrier {
	return &traceMetadataProtoCarrier{
		metadata:    metadata.GetEntries(),
		setMetadata: setMetadata,
	}
}

func (c *traceMetadataProtoCarrier) Get(key string) string {
	return c.metadata[key]
}

func (c *traceMetadataProtoCarrier) Set(key string, value string) {
	if c.metadata == nil {
		c.metadata = make(map[string]string)
		if c.setMetadata == nil {
			// Should never happen since this is only called via Inject which always sets setMetadata function.
			log.Errorf("Can't set metadata w/o setMetadata function")
			return
		}
		c.setMetadata(&tpb.Metadata{Entries: c.metadata})
	}
	c.metadata[key] = value
}

func (c *traceMetadataProtoCarrier) Keys() []string {
	var keys []string
	for k := range c.metadata {
		keys = append(keys, k)
	}
	return keys
}

func InjectProtoTraceMetadata(ctx context.Context, metadata *tpb.Metadata, setMetadata SetMetadata) {
	p := otel.GetTextMapPropagator()
	p.Inject(ctx, newTraceMetadataProtoCarrier(metadata, setMetadata))
}

func ExtractProtoTraceMetadata(ctx context.Context, metadata *tpb.Metadata) context.Context {
	p := otel.GetTextMapPropagator()
	return p.Extract(ctx, newTraceMetadataProtoCarrier(metadata, nil))
}

type HttpServeMux struct {
	delegateMux    *http.ServeMux
	tracingHandler http.Handler
}

func NewHttpServeMux(delegate *http.ServeMux) *HttpServeMux {
	// By default otelhttp names all the spans with the passed operation name.
	// We pass an empty name here since we override the span name in http handler.
	handler := otelhttp.NewHandler(delegate, "" /* operation= */)
	return &HttpServeMux{delegateMux: delegate, tracingHandler: handler}
}

func (m *HttpServeMux) Handle(pattern string, handler http.Handler) {
	m.delegateMux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		span := trace.SpanFromContext(r.Context())
		span.SetName(fmt.Sprintf("%s %s", r.Method, pattern))
		handler.ServeHTTP(w, r)
	})
}

func copyHeader(m metadata.MD, h http.Header, k string) {
	if _, ok := h[k]; !ok {
		return
	}
	m[k] = make([]string, len(h[k]))
	copy(m[k], h[k])
}

func headersToContext(h http.Header) metadata.MD {
	m := make(metadata.MD, 0)
	for key, _ := range h {
		switch strings.ToLower(key) {
		case "x-buildbuddy-trace":
			copyHeader(m, h, key)
		}
	}
	return m
}

func (m *HttpServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Filter and convert any HTTP headers to grpc MD, and attach
	// that metadata to the context. This happens here because this is
	// early enough in the call chain to influence tracing and other
	// decisions.
	ctx := metadata.NewIncomingContext(r.Context(), headersToContext(r.Header))
	r = r.WithContext(ctx)
	m.tracingHandler.ServeHTTP(w, r)
}

// StartSpan starts a new span named after the calling function.
func StartSpan(ctx context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx, span := otel.GetTracerProvider().Tracer(buildBuddyInstrumentationName).Start(ctx, "unknown_go_function", opts...)
	if !span.IsRecording() {
		return ctx, span
	}

	rpc := make([]uintptr, 1)
	n := runtime.Callers(2, rpc[:])
	if n > 0 {
		frame, _ := runtime.CallersFrames(rpc).Next()
		span.SetName(filepath.Base(frame.Function))
	}
	return ctx, span
}

func AddStringAttributeToCurrentSpan(ctx context.Context, key, value string) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.String(key, value))
}
