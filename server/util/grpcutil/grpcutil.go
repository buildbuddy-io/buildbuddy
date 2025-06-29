package grpcutil

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Provides an OpenTelemetry MeterProvider that exports metrics to Prometheus.
// Wrapped in a sync.Once to avoid registering Prometheus metrics multiple
// times in case there are multiple gRPC clients or servers.
var MeterProvider = sync.OnceValue(func() metric.MeterProvider {
	exporter, err := prometheus.New()
	if err != nil {
		alert.UnexpectedEvent("Error creating prometheus metrics exporter")
		return noop.NewMeterProvider()
	}
	return sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
})
