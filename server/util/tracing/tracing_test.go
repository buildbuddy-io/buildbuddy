package tracing_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	tpb "github.com/buildbuddy-io/buildbuddy/proto/trace"
)

const (
	// A Datadog-style trace ID: 32-bit unix timestamp, 32 zero bits, 64
	// random bits — as propagated by dd-trace clients.
	externalTraceID    = "6a629a3d000000004bef9c17468ee658"
	externalSpanID     = "61f914d398b5dab8"
	externalTraceState = "dd=s:2;p:61f914d398b5dab8;t.dm:-4"
)

func externalTraceParent() string {
	return "00-" + externalTraceID + "-" + externalSpanID + "-01"
}

func init() {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()
}

// fakeClientIdentityService is never called by the propagator gate, which
// only checks whether a client-identity service is registered; embedding the
// interface satisfies it without implementing any methods.
type fakeClientIdentityService struct {
	interfaces.ClientIdentityService
}

func setupTracing(t *testing.T, cis interfaces.ClientIdentityService) {
	flags.Set(t, "app.trace_fraction", 1.0)
	te := testenv.GetTestEnv(t)
	if cis != nil {
		te.SetClientIdentityService(cis)
	}
	require.NoError(t, tracing.ConfigureWithNoopExporter(te))
}

func externalCarrier(extraHeaders map[string]string) propagation.MapCarrier {
	c := propagation.MapCarrier{
		"traceparent": externalTraceParent(),
		"tracestate":  externalTraceState,
	}
	for k, v := range extraHeaders {
		c[k] = v
	}
	return c
}

func TestExternalParentContextIgnored(t *testing.T) {
	setupTracing(t, &fakeClientIdentityService{})

	// No client-identity header: the inbound context must not be adopted.
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), externalCarrier(nil))
	require.False(t, trace.SpanContextFromContext(ctx).IsValid())

	// New spans become roots of a fresh trace, sampled by our own sampler.
	_, span := tracing.StartNamedSpan(ctx, "test")
	defer span.End()
	require.NotEqual(t, externalTraceID, span.SpanContext().TraceID().String())
	require.True(t, span.SpanContext().IsSampled())
}

func TestInternalParentContextHonored(t *testing.T) {
	setupTracing(t, &fakeClientIdentityService{})

	// A caller presenting a client-identity header has its context adopted.
	carrier := externalCarrier(map[string]string{authutil.ClientIdentityHeaderName: "some-identity-header"})
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	sc := trace.SpanContextFromContext(ctx)
	require.True(t, sc.IsValid())
	require.True(t, sc.IsRemote())
	require.Equal(t, externalTraceID, sc.TraceID().String())
}

func TestExternalParentContextHonoredWithoutClientIdentityService(t *testing.T) {
	// Deployments without client identity configured retain the historical
	// behavior of honoring all inbound trace context.
	setupTracing(t, nil)

	ctx := otel.GetTextMapPropagator().Extract(context.Background(), externalCarrier(nil))
	sc := trace.SpanContextFromContext(ctx)
	require.True(t, sc.IsValid())
	require.Equal(t, externalTraceID, sc.TraceID().String())
}

func TestExtractProtoTraceMetadataBypassesGate(t *testing.T) {
	// Trace metadata embedded in internal protos (e.g. queued execution
	// tasks) has no client-identity header; it must still be extracted.
	setupTracing(t, &fakeClientIdentityService{})

	md := &tpb.Metadata{Entries: map[string]string{"traceparent": externalTraceParent()}}
	ctx := tracing.ExtractProtoTraceMetadata(context.Background(), md)
	sc := trace.SpanContextFromContext(ctx)
	require.True(t, sc.IsValid())
	require.Equal(t, externalTraceID, sc.TraceID().String())
}

// setupProdLikeTracing configures tracing the way production runs it: a tiny
// sampling fraction (so nothing samples by trace-ID ratio) and the client
// identity gate active.
func setupProdLikeTracing(t *testing.T) {
	flags.Set(t, "app.trace_fraction", 0.000001)
	te := testenv.GetTestEnv(t)
	te.SetClientIdentityService(&fakeClientIdentityService{})
	require.NoError(t, tracing.ConfigureWithNoopExporter(te))
}

// TestForcedTracePipeline simulates the full app->executor path:
//
//  1. app receives Execute with x-buildbuddy-trace:force -> forced-sampled span
//  2. scheduler injects span context into EnqueueTaskReservation.trace_metadata
//  3. executor extracts the proto trace metadata into its root context
//  4. executor starts the task-execution span
//
// The executor span must join the forced trace (same trace ID, sampled).
func TestForcedTracePipeline(t *testing.T) {
	setupProdLikeTracing(t)

	// App side: forced trace via gRPC header.
	appCtx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-buildbuddy-trace", "force"))
	appCtx, appSpan := tracing.StartNamedSpan(appCtx, "execute")
	defer appSpan.End()
	require.True(t, appSpan.SpanContext().IsSampled(), "app span should be force-sampled")

	// Scheduler side: inject into the reservation proto (as executorHandle does).
	var md *tpb.Metadata
	tracing.InjectProtoTraceMetadata(appCtx, md, func(m *tpb.Metadata) { md = m })
	require.NotNil(t, md, "trace metadata should have been injected")
	require.NotEmpty(t, md.GetEntries()["traceparent"], "traceparent should be set, got: %v", md.GetEntries())

	// Executor side: extract from proto into a fresh root context.
	execCtx := tracing.ExtractProtoTraceMetadata(context.Background(), md)
	sc := trace.SpanContextFromContext(execCtx)
	require.True(t, sc.IsValid(), "executor should extract a valid remote span context")
	require.True(t, sc.IsSampled(), "extracted remote parent should be sampled")
	require.Equal(t, appSpan.SpanContext().TraceID(), sc.TraceID())

	// Executor task-execution span must join the trace and be sampled.
	_, taskSpan := tracing.StartNamedSpan(execCtx, "ExecuteTaskAndStreamResults")
	defer taskSpan.End()
	require.Equal(t, appSpan.SpanContext().TraceID(), taskSpan.SpanContext().TraceID(),
		"executor task span should join the forced trace")
	require.True(t, taskSpan.SpanContext().IsSampled(), "executor task span should be sampled")
}

// TestForcedTraceRelayHop simulates the multi-app path where the app that
// receives ScheduleTask relays the enqueue over gRPC to the app that owns the
// executor connection. The receiving app's otelgrpc handler extracts the trace
// context from gRPC metadata through the identity-gated propagator, then
// starts a server span, and injects THAT into the reservation proto.
func TestForcedTraceRelayHop(t *testing.T) {
	setupProdLikeTracing(t)

	// App 1: forced-sampled span, as above.
	app1Ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-buildbuddy-trace", "force"))
	app1Ctx, app1Span := tracing.StartNamedSpan(app1Ctx, "ScheduleTask")
	defer app1Span.End()
	require.True(t, app1Span.SpanContext().IsSampled())

	// App1 -> App2 gRPC hop: otelgrpc client handler injects into metadata.
	// DialInternal attaches the client identity header.
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(app1Ctx, carrier)
	carrier[authutil.ClientIdentityHeaderName] = "app1-identity"

	// App 2: otelgrpc server handler extracts through the gated propagator.
	app2Ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	sc := trace.SpanContextFromContext(app2Ctx)
	require.True(t, sc.IsValid(), "app2 should adopt app1's context (identity header present)")
	require.True(t, sc.IsSampled())

	// App 2 starts its RPC server span (ParentBased sampler, remote parent sampled).
	app2Ctx, app2Span := tracing.StartNamedSpan(app2Ctx, "EnqueueTaskReservation")
	defer app2Span.End()
	require.True(t, app2Span.SpanContext().IsSampled(), "app2 server span should inherit sampling")

	// App 2 injects into the reservation proto.
	var md *tpb.Metadata
	tracing.InjectProtoTraceMetadata(app2Ctx, md, func(m *tpb.Metadata) { md = m })
	require.NotNil(t, md)

	// Executor joins.
	execCtx := tracing.ExtractProtoTraceMetadata(context.Background(), md)
	_, taskSpan := tracing.StartNamedSpan(execCtx, "ExecuteTaskAndStreamResults")
	defer taskSpan.End()
	require.Equal(t, app1Span.SpanContext().TraceID(), taskSpan.SpanContext().TraceID())
	require.True(t, taskSpan.SpanContext().IsSampled())
}

// TestRelayHopWithoutIdentityHeader documents the failure mode when the
// app-to-app hop is missing the client identity header: the receiving app
// discards the inbound trace context, so the executor never joins the trace.
func TestRelayHopWithoutIdentityHeader(t *testing.T) {
	setupProdLikeTracing(t)

	app1Ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-buildbuddy-trace", "force"))
	app1Ctx, app1Span := tracing.StartNamedSpan(app1Ctx, "ScheduleTask")
	defer app1Span.End()

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(app1Ctx, carrier)
	// No client identity header attached.

	app2Ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	require.False(t, trace.SpanContextFromContext(app2Ctx).IsValid(),
		"gate should discard inbound context without identity header")

	// App2's server span becomes a new, fraction-sampled (i.e. unsampled) root.
	app2Ctx, app2Span := tracing.StartNamedSpan(app2Ctx, "EnqueueTaskReservation")
	defer app2Span.End()
	require.False(t, app2Span.SpanContext().IsSampled())

	var md *tpb.Metadata
	tracing.InjectProtoTraceMetadata(app2Ctx, md, func(m *tpb.Metadata) { md = m })

	// The executor now inherits an unsampled, unrelated parent.
	execCtx := tracing.ExtractProtoTraceMetadata(context.Background(), md)
	_, taskSpan := tracing.StartNamedSpan(execCtx, "ExecuteTaskAndStreamResults")
	defer taskSpan.End()
	require.NotEqual(t, app1Span.SpanContext().TraceID(), taskSpan.SpanContext().TraceID())
	require.False(t, taskSpan.SpanContext().IsSampled())
}

func setupBench(b *testing.B) {
	flags.Set(b, "app.trace_fraction", 0.01)
	require.NoError(b, tracing.ConfigureWithNoopExporter(testenv.GetTestEnv(b)))
}

func BenchmarkStartSpan(b *testing.B) {
	setupBench(b)
	for b.Loop() {
		tracing.StartSpan(context.Background())
	}
}

func BenchmarkStartSpanParallel(b *testing.B) {
	setupBench(b)
	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tracing.StartSpan(context.Background())
		}
	})
}

func BenchmarkStartNamedSpan(b *testing.B) {
	setupBench(b)
	for b.Loop() {
		tracing.StartNamedSpan(context.Background(), "BenchmarkStartNamedSpan")
	}
}

func BenchmarkStartNamedSpanParallel(b *testing.B) {
	setupBench(b)
	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tracing.StartNamedSpan(context.Background(), "BenchmarkStartNamedSpanParallel")
		}
	})
}
