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
