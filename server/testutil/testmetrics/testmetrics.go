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
// Note: counter values can persist across tests, so be sure to call Reset() on
// the metrics you care about at the start of the test.
func CounterValue(t testing.TB, metric prometheus.Counter) float64 {
	m := &dto.Metric{}
	err := metric.Write(m)
	require.NoError(t, err)
	return m.Counter.GetValue()
}

// Histogram returns the histogram values for the given labels.
//
// Note: metric values can persist across tests, so be sure to call Reset() on
// the metrics you care about at the start of the test.
func Histogram(t testing.TB, h *prometheus.HistogramVec, labels prometheus.Labels) *dto.Histogram {
	metric, err := h.MetricVec.GetMetricWith(labels)
	require.NoError(t, err)
	m := &dto.Metric{}
	err = metric.Write(m)
	require.NoError(t, err)
	return m.Histogram
}

// Collect serializes all of the data observed for the given metric.
//
//	for _, m := range testmetrics.Collect(t, metrics.MyCounterVec) {
//		log.Infof("Series labels: %s", testmetrics.Labels(m))
//		log.Infof("Counter value: %s", m.Counter.GetValue())
//	}
func Collect(t testing.TB, c prometheus.Collector) []*dto.Metric {
	ch := make(chan prometheus.Metric)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	var out []*dto.Metric
	for m := range ch {
		metric := &dto.Metric{}
		err := m.Write(metric)
		require.NoError(t, err)
		out = append(out, metric)
	}
	return out
}

// Labels converts the serialized metric labels to a map.
func Labels(proto *dto.Metric) map[string]string {
	m := make(map[string]string, len(proto.Label))
	for _, label := range proto.Label {
		m[label.GetName()] = label.GetValue()
	}
	return m
}
