package testmetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	dto "github.com/prometheus/client_model/go"
)

// GaugeValue returns the current value of a gauge metric.
func GaugeValue(t testing.TB, metric prometheus.Gauge) float64 {
	m := &dto.Metric{}
	err := metric.Write(m)
	require.NoError(t, err)
	return m.Gauge.GetValue()
}

// CounterValue returns the current value of a counter metric.
//
// Note: counter values can persist across tests, so be sure to only make
// assertions on *changes* to counter values.
func CounterValue(t testing.TB, metric prometheus.Counter) float64 {
	m := &dto.Metric{}
	err := metric.Write(m)
	require.NoError(t, err)
	return m.Counter.GetValue()
}
