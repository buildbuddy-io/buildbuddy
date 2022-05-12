package tracing

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
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
	traceProjectID            = flag.String("app.trace_project_id", "", "Optional GCP project ID to export traces to. If not specified, determined from default credentials or metadata server if running on GCP.")
	traceJaegerCollector      = flag.String("app.trace_jaeger_collector", "", "Address of the Jager collector endpoint where traces will be sent.")
	traceServiceName          = flag.String("app.trace_service_name", "", "Name of the service to associate with traces.")
	traceFraction             = flag.Float64("app.trace_fraction", 0, "Fraction of requests to sample for tracing.")
	traceFractionOverrides    = flagutil.Slice("app.trace_fraction_overrides", []string{}, "Tracing fraction override based on name in format name=fraction.")
	ignoreForcedTracingHeader = flag.Bool("app.ignore_forced_tracing_header", false, "If set, we will not honor the forced tracing header.")

	// bound overrides are parsed from the traceFractionOverrides flag.
	initOverrideFractions sync.Once
	overrideFractions     map[string]float64
)

const (
	resourceDetectionTimeout      = 5 * time.Second
	buildBuddyInstrumentationName = "buildbuddy.io"
	traceHeader                   = "x-buildbuddy-trace"
	traceParentHeader             = "traceparent"
	forceTraceHeaderValue         = "force"

	TracingDecisionHeader = "tracing-decision"
)

type fractionSampler struct{}

func (s *fractionSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	psc := trace.SpanContextFromContext(parameters.ParentContext)
	if ShouldTraceIncoming(parameters.ParentContext, parameters.Name) {
		return sdktrace.SamplingResult{
			Decision:   sdktrace.RecordAndSample,
			Attributes: parameters.Attributes,
			Tracestate: psc.TraceState(),
		}
	}
	return sdktrace.SamplingResult{
		Decision:   sdktrace.Drop,
		Attributes: parameters.Attributes,
		Tracestate: psc.TraceState(),
	}
}
func (s *fractionSampler) Description() string {
	return "FractionSampler"
}

func Configure(env environment.Env) error {
	if *traceJaegerCollector == "" {
		return nil
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

	sampler := &fractionSampler{}
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
	var provider trace.TracerProvider
	if decision, ok := tracingDecision(ctx); ok && !decision {
		provider = trace.NewNoopTracerProvider()
	} else {
		provider = otel.GetTracerProvider()
	}

	ctx, span := provider.Tracer(buildBuddyInstrumentationName).Start(ctx, "unknown_go_function", opts...)
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

func parseOverrides() error {
	overrideFractions = make(map[string]float64, 0)
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
		overrideFractions[name] = fraction
	}
	return nil
}

func shouldTraceMD(grpcMD metadata.MD, method string) bool {
	if len(grpcMD) > 0 {
		// Check if this was a force-traced request.
		forcedVals := grpcMD[traceHeader]
		if !*ignoreForcedTracingHeader && len(forcedVals) > 0 && forcedVals[0] == forceTraceHeaderValue {
			return true
		}

		// Check if this is a request from some other service which has
		// already enabled tracing.
		parentVals := grpcMD[traceParentHeader]
		if len(parentVals) > 0 && len(parentVals[0]) > 0 {
			return true
		}
	}
	initOverrideFractions.Do(func() {
		if err := parseOverrides(); err != nil {
			log.Errorf(err.Error())
		}
	})

	fraction := *traceFraction
	if f, ok := overrideFractions[method]; ok {
		fraction = f
	}
	return float64(random.RandUint64())/math.MaxUint64 < fraction
}

func tracingDecision(ctx context.Context) (bool, bool) {
	if v := ctx.Value(TracingDecisionHeader); v != nil {
		shouldTrace, ok := v.(bool)
		if ok {
			return shouldTrace, true
		}
	}
	return false, false
}

// ShouldTraceIncoming returns a boolean indicating if request should be traced
// because:
//  - tracing was forced
//  - some other service is tracing this request already
//  - sampling indicates this request should be traced
func ShouldTraceIncoming(ctx context.Context, method string) bool {
	// If this ctx was already selected for tracing by the rpc interceptor,
	// use that decision.
	if decision, ok := tracingDecision(ctx); ok {
		return decision
	}
	grpcMD, _ := metadata.FromIncomingContext(ctx)
	return shouldTraceMD(grpcMD, method)
}

//  ShouldTraceOutgoing returns a boolean indicating if request should be traced
//  because:
//  - tracing was forced
//  - some other service is tracing this request already
//  - sampling indicates this request should be traced
func ShouldTraceOutgoing(ctx context.Context, method string) bool {
	// If this ctx was already selected for tracing by the rpc interceptor,
	// use that decision.
	if decision, ok := tracingDecision(ctx); ok {
		return decision
	}
	grpcMD, _ := metadata.FromOutgoingContext(ctx)
	return shouldTraceMD(grpcMD, method)
}
