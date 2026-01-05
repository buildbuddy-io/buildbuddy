package tracing

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
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
	traceProjectID            = flag.String("app.trace_project_id", "", "Optional GCP project ID to export traces to. If not specified, determined from default credentials or metadata server if running on GCP.")
	traceJaegerCollector      = flag.String("app.trace_jaeger_collector", "", "Address of the Jaeger collector HTTP endpoint where traces will be sent, e.g. http://jaeger.svc.cluster.local:14268")
	traceOTLPCollector        = flag.String("app.trace_otlp_grpc_collector", "", "Address of the OTLP gRPC collector endpoint where traces will be sent, e.g. otel-collector.svc.cluster.local:4317")
	traceOTLPHTTPCollector    = flag.String("app.trace_otlp_http_collector", "", "Address of the OTLP HTTP collector endpoint where traces will be sent, e.g. http://otel-collector.svc.cluster.local:4318")
	traceServiceName          = flag.String("app.trace_service_name", "", "Name of the service to associate with traces.")
	traceFraction             = flag.Float64("app.trace_fraction", 0, "Fraction of requests to sample for tracing.")
	traceFractionOverrides    = flag.Slice("app.trace_fraction_overrides", []string{}, "Tracing fraction override based on name in format name=fraction.")
	ignoreForcedTracingHeader = flag.Bool("app.ignore_forced_tracing_header", false, "If set, we will not honor the forced tracing header.")
)

// Re-initialized in Configure. Set here so tests that don't call Configure
// still work.
var tracer = otel.GetTracerProvider().Tracer(buildBuddyInstrumentationName)

const (
	resourceDetectionTimeout      = 5 * time.Second
	buildBuddyInstrumentationName = "buildbuddy.io"
	traceHeader                   = "x-buildbuddy-trace"
	traceParentHeader             = "traceparent"
	forceTraceHeaderValue         = "force"
)

// fractionSampler allows specifying a default sampling fraction as well as overrides based on the span name.
// Based on TraceIDRatioBased sampler from the OpenTelemetry library.
type fractionSampler struct {
	traceIDUpperBound          uint64
	traceIDUpperBoundOverrides map[string]uint64
	description                string
	ignoreForcedTracingHeader  bool
}

func newFractionSampler(fraction float64, fractionOverrides map[string]float64, ignoreForcedTracingHeader bool) *fractionSampler {
	configDescription := fmt.Sprintf("default=%f", fraction)
	boundOverrides := make(map[string]uint64)
	for n, f := range fractionOverrides {
		boundOverrides[n] = uint64(f * math.MaxInt64)
		configDescription += fmt.Sprintf(",%s=%f", n, f)
	}

	return &fractionSampler{
		traceIDUpperBound:          uint64(fraction * math.MaxInt64),
		traceIDUpperBoundOverrides: boundOverrides,
		description:                fmt.Sprintf("FractionSampler(%s)", configDescription),
		ignoreForcedTracingHeader:  ignoreForcedTracingHeader,
	}
}

func (s *fractionSampler) checkForcedTrace(parameters sdktrace.SamplingParameters) bool {
	if s.ignoreForcedTracingHeader {
		return false
	}
	hdrs := metadata.ValueFromIncomingContext(parameters.ParentContext, traceHeader)
	forced := len(hdrs) > 0 && hdrs[0] == forceTraceHeaderValue
	return forced
}

func (s *fractionSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	psc := trace.SpanContextFromContext(parameters.ParentContext)

	// Check if sampling is forced via gRPC header.
	if s.checkForcedTrace(parameters) {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.RecordAndSample,
			Attributes: parameters.Attributes,
			Tracestate: psc.TraceState(),
		}
	}

	bound := s.traceIDUpperBound
	if boundOverride, ok := s.traceIDUpperBoundOverrides[parameters.Name]; ok {
		bound = boundOverride
	}

	x := binary.BigEndian.Uint64(parameters.TraceID[0:8]) >> 1
	decision := sdktrace.Drop
	if x < bound {
		decision = sdktrace.RecordAndSample
	}
	return sdktrace.SamplingResult{
		Decision:   decision,
		Attributes: parameters.Attributes,
		Tracestate: psc.TraceState(),
	}
}

func (s *fractionSampler) Description() string {
	return s.description
}

// noopExporter is a span exporter that does nothing, used for testing/benchmarking.
type noopExporter struct{}

func (e *noopExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return nil
}

func (e *noopExporter) Shutdown(ctx context.Context) error {
	return nil
}

func Configure(env environment.Env) error {
	if *traceFraction <= 0 {
		return nil
	}

	// Check early that exactly one collector is configured
	numCollectorsSet := 0
	if *traceJaegerCollector != "" {
		numCollectorsSet++
	}
	if *traceOTLPCollector != "" {
		numCollectorsSet++
	}
	if *traceOTLPHTTPCollector != "" {
		numCollectorsSet++
	}

	if numCollectorsSet > 1 {
		return status.InvalidArgumentErrorf("only one trace collector ('app.trace_*_collector') can be configured at a time.")
	}

	var traceExporter sdktrace.SpanExporter
	var err error

	if *traceJaegerCollector != "" {
		traceExporter, err = jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(*traceJaegerCollector)))
		if err != nil {
			log.Warningf("Could not initialize Jaeger exporter: %s", err)
			return nil
		}
	} else if *traceOTLPCollector != "" {
		traceExporter, err = otlptracegrpc.New(env.GetServerContext(), otlptracegrpc.WithEndpoint(*traceOTLPCollector), otlptracegrpc.WithInsecure())
		if err != nil {
			log.Warningf("Could not initialize OTEL exporter: %s", err)
			return nil
		}
	} else if *traceOTLPHTTPCollector != "" {
		opts, err := parseHTTPOptions(*traceOTLPHTTPCollector)
		if err != nil {
			return status.InvalidArgumentErrorf("parse OTEL HTTP collector endpoint: %s", err)
		}
		traceExporter, err = otlptracehttp.New(env.GetServerContext(), opts...)
		if err != nil {
			log.Warningf("Could not initialize OTEL exporter: %s", err)
			return nil
		}
	}

	if traceExporter == nil {
		return status.InvalidArgumentErrorf("no trace collector ('app.trace_{jaeger,otlp_grpc,otlp_http}_collector') configured")
	}

	return setupTracingWithExporter(env, traceExporter)
}

// ConfigureWithNoopExporter configures tracing with a no-op exporter for testing/benchmarking.
func ConfigureWithNoopExporter(env environment.Env) error {
	if *traceFraction <= 0 {
		return nil
	}

	return setupTracingWithExporter(env, &noopExporter{})
}

// setupTracingWithExporter sets up the tracing infrastructure with the provided exporter.
func setupTracingWithExporter(env environment.Env, traceExporter sdktrace.SpanExporter) error {

	fractionOverrides := make(map[string]float64)
	for _, override := range *traceFractionOverrides {
		parts := strings.Split(override, "=")
		if len(parts) != 2 {
			return status.InvalidArgumentErrorf("Trace fraction override %q has invalid format, expected name=fraction", override)
		}
		name := parts[0]
		fraction, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return status.InvalidArgumentErrorf("Trace fraction override %q has invalid format, fraction not valid: %s", override, err)
		}
		fractionOverrides[name] = fraction
	}
	sampler := newFractionSampler(*traceFraction, fractionOverrides, *ignoreForcedTracingHeader)

	var resourceAttrs []attribute.KeyValue
	if *traceServiceName != "" {
		resourceAttrs = append(resourceAttrs, semconv.ServiceNameKey.String(*traceServiceName))
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return bsp.Shutdown(ctx)
	})

	ctx, cancel := context.WithTimeout(env.GetServerContext(), resourceDetectionTimeout)
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
		sdktrace.WithSampler(sdktrace.ParentBased(sampler)),
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
	log.Infof("Tracing enabled with sampler: %s", sampler.Description())
	tracer = otel.GetTracerProvider().Tracer(buildBuddyInstrumentationName)
	return nil
}

func parseHTTPOptions(s string) ([]otlptracehttp.Option, error) {
	u, err := url.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("parse URL: %w", err)
	}
	var opts []otlptracehttp.Option
	opts = append(opts, otlptracehttp.WithEndpoint(u.Host))
	if u.Scheme == "http" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	if user := u.User.String(); user != "" {
		opts = append(opts, otlptracehttp.WithHeaders(map[string]string{
			"Authorization": fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(user))),
		}))
	}
	return opts, nil
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
	for key := range h {
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
	ctx, span := tracer.Start(ctx, "unknown_go_function", opts...)
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

// StartNamedSpan is like StartSpan, expect the caller specifies the name
// instead of using the call stack.
func StartNamedSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, opts...)
}

func AddStringAttributeToCurrentSpan(ctx context.Context, key, value string) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.String(key, value))
}

// RecordErrorToSpan records a non-nil error to the span; and does nothing if
// span is not recording or err is nil.
func RecordErrorToSpan(span trace.Span, err error) {
	if err == nil {
		return
	}
	if !span.IsRecording() {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
