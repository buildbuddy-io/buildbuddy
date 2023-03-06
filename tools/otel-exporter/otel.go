package main

import (
	"context"
	"log"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.11.0"
	"go.opentelemetry.io/otel/trace"
)

func newExporter(ctx context.Context) (*otlptrace.Exporter, error) {
	return otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(*honeycombServer),
		otlptracegrpc.WithHeaders(map[string]string{
			"x-honeycomb-team": *honeycombAPIKey,
		}),
	))
}

// newTraceProvider create a trace provider
func newTraceProvider(exp *otlptrace.Exporter) *sdktrace.TracerProvider {
	// The service.name attribute is required.
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(serviceName),
		semconv.ServiceVersionKey.String(serviceVer),
	)

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
}

// initOtel returns a tracer object and a function that help handler graceful shutdown
func initOtel(ctx context.Context, serviceName string) (trace.Tracer, func()) {
	// Init otel
	exporter, err := newExporter(ctx)
	if err != nil {
		log.Fatalf("failed to initialize exporter: %v\n", err)
	}

	tp := newTraceProvider(exporter)

	return tp.Tracer(serviceName), func() { _ = tp.Shutdown(ctx) }
}
