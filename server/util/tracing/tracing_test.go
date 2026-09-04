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
