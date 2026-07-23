package tracing_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func init() {
	*log.LogLevel = "error"
	*log.IncludeShortFileName = true
	log.Configure()
}

func setupBench(b *testing.B) {
	flags.Set(b, "app.trace_fraction", 0.01)
	require.NoError(b, tracing.ConfigureWithNoopExporter(testenv.GetTestEnv(b)))
}

// parentContext returns a context carrying a (non-recording) parent span with
// the given sampled/remote flags. firstByte controls the high bits of the
// trace ID, which drives the fraction sampler when a span is (re)sampled:
// 0xff => Drop, 0x00 => Sample (at any non-zero fraction).
func parentContext(sampled, remote bool, firstByte byte) (context.Context, trace.SpanID) {
	var tid trace.TraceID
	for i := 0; i < 8; i++ {
		tid[i] = firstByte
	}
	tid[15] = 1 // keep the trace ID valid (non-zero) even when firstByte is 0.
	sid := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	cfg := trace.SpanContextConfig{TraceID: tid, SpanID: sid, Remote: remote}
	if sampled {
		cfg.TraceFlags = trace.FlagsSampled
	}
	sc := trace.NewSpanContext(cfg)
	return trace.ContextWithSpanContext(context.Background(), sc), sid
}

func setupTracing(t *testing.T, fraction float64) {
	flags.Set(t, "app.trace_fraction", fraction)
	require.NoError(t, tracing.ConfigureWithNoopExporter(testenv.GetTestEnv(t)))
}

// An unsampled LOCAL parent: the fast path returns the parent span itself
// rather than allocating a new (would-be-dropped) child span. The trace ID
// (0x00) would sample if the fraction sampler ran, proving the child is
// dropped because of the local-not-sampled parent, not the fraction.
func TestStartNamedSpan_UnsampledLocalParent_ReturnsParent(t *testing.T) {
	setupTracing(t, 0.01)
	ctx, parentSID := parentContext(false /*sampled*/, false /*remote*/, 0x00)

	_, span := tracing.StartNamedSpan(ctx, "child")

	require.False(t, span.IsRecording(), "child of an unsampled local parent should not record")
	require.Equal(t, parentSID, span.SpanContext().SpanID(),
		"fast path should return the parent span (no new span created)")
}

// A sampled parent must still produce a real, recording child span.
func TestStartNamedSpan_SampledParent_CreatesChild(t *testing.T) {
	setupTracing(t, 0.01)
	ctx, parentSID := parentContext(true /*sampled*/, false /*remote*/, 0xff)

	_, span := tracing.StartNamedSpan(ctx, "child")

	require.True(t, span.IsRecording(), "child of a sampled parent should record")
	require.NotEqual(t, parentSID, span.SpanContext().SpanID(),
		"a real child span should have its own span ID")
}

// A REMOTE not-sampled parent must be re-sampled (WithRemoteParentNotSampled),
// so the fast path must NOT short-circuit it. With a trace ID that samples,
// the resulting span records.
func TestStartNamedSpan_RemoteUnsampledParent_Resampled(t *testing.T) {
	setupTracing(t, 0.01)
	sampleID, _ := parentContext(false /*sampled*/, true /*remote*/, 0x00)
	_, span := tracing.StartNamedSpan(sampleID, "child")
	require.True(t, span.IsRecording(), "remote unsampled parent should be re-sampled, not short-circuited")

	dropID, _ := parentContext(false /*sampled*/, true /*remote*/, 0xff)
	_, span = tracing.StartNamedSpan(dropID, "child")
	require.False(t, span.IsRecording(), "remote unsampled parent that fails resampling is dropped")
}

// The force-trace header overrides sampling where the sampler actually runs
// (root / remote not-sampled parent), so the fast path must not short-circuit
// such spans. (A local not-sampled parent is AlwaysOff and never consults the
// sampler, so force does not apply there -- see TestSamplerAssumption below.)
func TestStartNamedSpan_ForcedHeader_OverridesRemoteUnsampledParent(t *testing.T) {
	setupTracing(t, 0.01)
	base, _ := parentContext(false /*sampled*/, true /*remote*/, 0xff)

	// Without the force header, the (0xff) trace ID fails resampling -> dropped.
	_, span := tracing.StartNamedSpan(base, "child")
	require.False(t, span.IsRecording())

	// With the force header it records.
	forced := metadata.NewIncomingContext(base, metadata.Pairs("x-buildbuddy-trace", "force"))
	_, span = tracing.StartNamedSpan(forced, "child")
	require.True(t, span.IsRecording(), "forced trace should record even under an unsampled remote parent")
}

// Guards the shortcut's invariant on the real sampler (shortcut disabled): a
// child of an unsampled local parent is always dropped, fraction and force
// ignored. Fails if a sampler-config change (e.g. WithLocalParentNotSampled)
// makes it sample-able -- the signal to revisit shortcutTracer.Start.
func TestSamplerAssumption_LocalUnsampledParentAlwaysDropped(t *testing.T) {
	flags.Set(t, "app.trace_sampling_shortcut", false) // exercise the real sampler
	setupTracing(t, 0.01)

	base, _ := parentContext(false /*sampled*/, false /*remote*/, 0x00 /*would sample*/)

	_, span := tracing.StartNamedSpan(base, "child")
	require.False(t, span.IsRecording(), "local unsampled parent must drop its child (fraction ignored)")

	forced := metadata.NewIncomingContext(base, metadata.Pairs("x-buildbuddy-trace", "force"))
	_, span = tracing.StartNamedSpan(forced, "child")
	require.False(t, span.IsRecording(), "force does not apply to a local unsampled parent (AlwaysOff)")
}

// The kill-switch flag disables the shortcut: a child of an unsampled local
// parent then goes through the real tracer (a new, non-recording span).
func TestStartNamedSpan_ShortcutDisabled(t *testing.T) {
	flags.Set(t, "app.trace_sampling_shortcut", false)
	setupTracing(t, 0.01)
	ctx, parentSID := parentContext(false /*sampled*/, false /*remote*/, 0xff)

	_, span := tracing.StartNamedSpan(ctx, "child")

	require.False(t, span.IsRecording())
	require.NotEqual(t, parentSID, span.SpanContext().SpanID(),
		"with the shortcut disabled a real (new) child span is created")
}

// Same workload -- a child span of an unsampled local parent -- with the
// shortcut on vs off, to isolate the shortcut's effect.
func benchmarkUnsampledLocalParent(b *testing.B, shortcut bool) {
	flags.Set(b, "app.trace_fraction", 0.01)
	flags.Set(b, "app.trace_sampling_shortcut", shortcut)
	require.NoError(b, tracing.ConfigureWithNoopExporter(testenv.GetTestEnv(b)))
	ctx, _ := parentContext(false /*sampled*/, false /*remote*/, 0xff)
	for b.Loop() {
		tracing.StartNamedSpan(ctx, "bench")
	}
}

func BenchmarkUnsampledLocalParent_ShortcutOn(b *testing.B) {
	benchmarkUnsampledLocalParent(b, true)
}

func BenchmarkUnsampledLocalParent_ShortcutOff(b *testing.B) {
	benchmarkUnsampledLocalParent(b, false)
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
