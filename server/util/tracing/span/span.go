// Package span provides lightweight wrappers around the OpenTelemetry tracing
// API for starting spans and propagating trace metadata.
//
// This package intentionally has a minimal dependency footprint: it imports
// only the OpenTelemetry API surface (no SDK, no exporters, no resource
// detectors). Code that lives on the critical path of binaries with strict
// size budgets (e.g. goinit, which ships inside firecracker microVMs) should
// import this package instead of //server/util/tracing, because importing the
// latter pulls in heavy transitive dependencies such as the AWS/GCP resource
// detectors, OTLP/jaeger exporters, k8s.io/client-go and gnostic-models, even
// when the configuration entry points are dead-code-eliminated.
//
// All functions here are no-ops when no tracer provider has been configured
// (i.e. when //server/util/tracing.Configure has not been called), which
// matches the behavior of OpenTelemetry's default noop provider.
package span

import (
	"context"
	"path/filepath"
	"runtime"

	tpb "github.com/buildbuddy-io/buildbuddy/proto/trace"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// InstrumentationName is the OpenTelemetry instrumentation name used for
// spans started by buildbuddy code. Kept exported so the heavyweight
// //server/util/tracing package can refer to the same constant.
const InstrumentationName = "buildbuddy.io"

// tracer returns the current global tracer for buildbuddy instrumentation.
// It looks up the tracer fresh on each call so that spans started before
// //server/util/tracing.Configure runs use the noop provider, and spans
// started afterwards use the configured SDK provider, without any explicit
// hand-off between the two packages.
func tracer() trace.Tracer {
	return otel.Tracer(InstrumentationName)
}

// StartSpan starts a new span named after the calling function.
//
// Note:
//   - StartSpan doesn't support --app.trace_fraction_overrides; if you need
//     to override the trace fraction with a specified function, use
//     StartNamedSpan instead.
//   - If you want to record a trace inside a loop or in a performance-critical
//     path, use StartNamedSpan instead.
func StartSpan(ctx context.Context, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx, sp := tracer().Start(ctx, "unknown_go_function", opts...)
	if !sp.IsRecording() {
		return ctx, sp
	}

	rpc := make([]uintptr, 1)
	n := runtime.Callers(2, rpc[:])
	if n > 0 {
		frame, _ := runtime.CallersFrames(rpc).Next()
		sp.SetName(filepath.Base(frame.Function))
	}
	return ctx, sp
}

// StartNamedSpan is like StartSpan, except the caller specifies the name
// instead of using the call stack.
func StartNamedSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer().Start(ctx, name, opts...)
}

// AddStringAttributeToCurrentSpan attaches a string attribute to the span in
// the given context, if there is one and it is recording.
func AddStringAttributeToCurrentSpan(ctx context.Context, key, value string) {
	sp := trace.SpanFromContext(ctx)
	if !sp.IsRecording() {
		return
	}
	sp.SetAttributes(attribute.String(key, value))
}

// RecordErrorToSpan records a non-nil error on the given span and marks the
// span as failed. It is a no-op if err is nil or the span is not recording.
func RecordErrorToSpan(sp trace.Span, err error) {
	if err == nil {
		return
	}
	if !sp.IsRecording() {
		return
	}
	sp.RecordError(err)
	sp.SetStatus(codes.Error, err.Error())
}

// SetMetadata is invoked by InjectProtoTraceMetadata when the carrier is
// populated for the first time, so that callers can attach the resulting
// proto metadata to the message they're injecting into.
type SetMetadata func(m *tpb.Metadata)

type protoCarrier struct {
	metadata    map[string]string
	setMetadata SetMetadata
}

func newProtoCarrier(metadata *tpb.Metadata, setMetadata SetMetadata) *protoCarrier {
	return &protoCarrier{
		metadata:    metadata.GetEntries(),
		setMetadata: setMetadata,
	}
}

func (c *protoCarrier) Get(key string) string {
	return c.metadata[key]
}

func (c *protoCarrier) Set(key string, value string) {
	if c.metadata == nil {
		c.metadata = make(map[string]string)
		if c.setMetadata == nil {
			// Should never happen since this is only called via Inject which
			// always sets setMetadata function.
			log.Errorf("Can't set metadata w/o setMetadata function")
			return
		}
		c.setMetadata(&tpb.Metadata{Entries: c.metadata})
	}
	c.metadata[key] = value
}

func (c *protoCarrier) Keys() []string {
	keys := make([]string, 0, len(c.metadata))
	for k := range c.metadata {
		keys = append(keys, k)
	}
	return keys
}

// InjectProtoTraceMetadata serializes the trace context from ctx into the
// given proto Metadata using the globally-configured text-map propagator.
// If no metadata entries exist yet, setMetadata is invoked with the freshly
// allocated metadata so the caller can attach it to its outbound message.
func InjectProtoTraceMetadata(ctx context.Context, metadata *tpb.Metadata, setMetadata SetMetadata) {
	p := otel.GetTextMapPropagator()
	p.Inject(ctx, newProtoCarrier(metadata, setMetadata))
}

// ExtractProtoTraceMetadata returns a copy of ctx with the trace context
// extracted from the given proto Metadata using the globally-configured
// text-map propagator.
func ExtractProtoTraceMetadata(ctx context.Context, metadata *tpb.Metadata) context.Context {
	p := otel.GetTextMapPropagator()
	return p.Extract(ctx, newProtoCarrier(metadata, nil))
}
