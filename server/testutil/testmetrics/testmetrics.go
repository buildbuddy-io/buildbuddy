package testmetrics

import (
	"bytes"
	"encoding/json"
	"slices"
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

type HistogramValues struct {
	// Metric label values.
	Labels map[string]string

	// Maps bucket upperbound (<=) to the incremental count from previous
	// bucket. Note, bucket counts are normally cumulative, but incremental
	// counts are more convenient for test assertions.
	//
	// Example: given the buckets [0, 1, 10, 100] and the observations [10, 50],
	// this would contain {10: 1, 100: 1}
	Buckets map[float64]uint64
}

// HistogramVecValues returns all of the histogram bucket series.
// Series with different label values are returned as separate series.
func HistogramVecValues(t testing.TB, metric *prometheus.HistogramVec) []HistogramValues {
	var out []HistogramValues
	metrics := collectAll(t, metric)
	for _, m := range metrics {
		out = append(out, HistogramValues{
			Labels:  labelMap(m.GetLabel()),
			Buckets: incrementalBucketMap(m.GetHistogram().GetBucket()),
		})
	}
	return out
}

func labelMap(pairs []*dto.LabelPair) map[string]string {
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		out[p.GetName()] = p.GetValue()
	}
	return out
}

func incrementalBucketMap(buckets []*dto.Bucket) map[float64]uint64 {
	out := make(map[float64]uint64, 0)
	var total uint64
	// Note: this logic assumes buckets are already sorted in order of
	// upperbound
	for _, b := range buckets {
		increment := b.GetCumulativeCount() - total
		if increment > 0 {
			out[b.GetUpperBound()] = increment
		}
		total = b.GetCumulativeCount()
	}
	return out
}

// collectAll collects all metrics from the provided collector and returns them
// as a slice. The slice is sorted lexicographically by label values.
func collectAll(t testing.TB, collector prometheus.Collector) []*dto.Metric {
	metricChan := make(chan prometheus.Metric)
	go func() {
		defer close(metricChan)
		collector.Collect(metricChan)
	}()
	var metrics []*dto.Metric
	for metric := range metricChan {
		m := &dto.Metric{}
		err := metric.Write(m)
		require.NoError(t, err)
		metrics = append(metrics, m)
	}
	slices.SortFunc(metrics, func(a, b *dto.Metric) int {
		// Use JSON representation to sort maps, since it conveniently sorts by
		// key for us.
		jsonA, err := json.Marshal(labelMap(a.GetLabel()))
		require.NoError(t, err)
		jsonB, err := json.Marshal(labelMap(b.GetLabel()))
		require.NoError(t, err)
		return bytes.Compare(jsonA, jsonB)
	})
	return metrics
}
