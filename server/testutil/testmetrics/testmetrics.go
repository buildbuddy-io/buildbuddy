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

// GaugeValueForLabels returns the current value of the series in a GaugeVec
// matching the given labels.
func GaugeValueForLabels(t testing.TB, metric *prometheus.GaugeVec, labels prometheus.Labels) float64 {
	m := &dto.Metric{}
	err := metric.With(labels).Write(m)
	require.NoError(t, err)
	return m.Gauge.GetValue()
}

// CounterValue returns the current value of a counter metric.
//
// Note: counter values can persist across tests, so be sure to call Reset()
// on the metric you're asserting on at the start of the test.
func CounterValue(t testing.TB, metric prometheus.Counter) float64 {
	m := &dto.Metric{}
	err := metric.Write(m)
	require.NoError(t, err)
	return m.Counter.GetValue()
}
