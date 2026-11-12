package prom

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	dto "github.com/prometheus/client_model/go"
)

const invocationDuration = "buildbuddy_invocation_duration_usec_exported"

var invocationLabelNames = []string{metrics.InvocationStatusLabel, podNameLabel}

func sample(value float64, labels ...string) *model.Sample {
	m := model.Metric{}
	for i := 0; i < len(labels); i += 2 {
		m[model.LabelName(labels[i])] = model.LabelValue(labels[i+1])
	}
	return &model.Sample{Metric: m, Value: model.SampleValue(value)}
}

func invocationLabels(pod string) []string {
	return []string{metrics.InvocationStatusLabel, "success", podNameLabel, pod}
}

func withLE(labels []string, le string) []string {
	return append(append([]string{}, labels...), leLabel, le)
}

// histogramVectors returns count, sum, and bucket vectors with one complete
// series per pod. Each pod has one sum sample and two bucket samples.
func histogramVectors(pods ...string) (count, sum, bucket model.Vector) {
	for _, pod := range pods {
		labels := invocationLabels(pod)
		count = append(count, sample(3, labels...))
		sum = append(sum, sample(600, labels...))
		bucket = append(bucket,
			sample(3, withLE(labels, "+Inf")...),
			sample(1, withLE(labels, "100")...),
		)
	}
	return count, sum, bucket
}

func podNames(ms []*dto.Metric) []string {
	var pods []string
	for _, m := range ms {
		for _, l := range m.GetLabel() {
			if l.GetName() == podNameLabel {
				pods = append(pods, l.GetValue())
			}
		}
	}
	return pods
}

func TestHistogramVecToMetrics_Complete(t *testing.T) {
	count, sum, bucket := histogramVectors("app-1")

	ms, skipped, err := histogramVecToMetrics(count, sum, bucket, invocationLabelNames)
	require.NoError(t, err)
	require.Empty(t, skipped)
	require.Len(t, ms, 1)
	h := ms[0].GetHistogram()
	require.Equal(t, uint64(3), h.GetSampleCount())
	require.Equal(t, 600.0, h.GetSampleSum())
	require.Len(t, h.GetBucket(), 2)
	require.Equal(t, 100.0, h.GetBucket()[0].GetUpperBound())
	require.Equal(t, uint64(1), h.GetBucket()[0].GetCumulativeCount())
	require.Equal(t, uint64(3), h.GetBucket()[1].GetCumulativeCount())
}

func TestHistogramVecToMetrics_SkipsIncompleteSeries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		// drop removes the first pod's series from one of the vectors.
		drop func(count, sum, bucket model.Vector) (model.Vector, model.Vector, model.Vector)
	}{
		{
			name:   "missing count",
			reason: metrics.ExportedMetricMissingCountReason,
			drop: func(count, sum, bucket model.Vector) (model.Vector, model.Vector, model.Vector) {
				return count[1:], sum, bucket
			},
		},
		{
			name:   "missing sum",
			reason: metrics.ExportedMetricMissingSumReason,
			drop: func(count, sum, bucket model.Vector) (model.Vector, model.Vector, model.Vector) {
				return count, sum[1:], bucket
			},
		},
		{
			name:   "missing buckets",
			reason: metrics.ExportedMetricMissingBucketsReason,
			drop: func(count, sum, bucket model.Vector) (model.Vector, model.Vector, model.Vector) {
				return count, sum, bucket[2:]
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count, sum, bucket := tc.drop(histogramVectors("app-old", "app-new"))

			ms, skipped, err := histogramVecToMetrics(count, sum, bucket, invocationLabelNames)
			require.NoError(t, err)
			require.Equal(t, []string{"app-new"}, podNames(ms))
			require.Equal(t, map[string]int{tc.reason: 1}, skipped)
		})
	}
}

func TestHistogramVecToMetrics_MissingLabelFails(t *testing.T) {
	count, sum, bucket := histogramVectors("app-1")
	delete(count[0].Metric, podNameLabel)

	_, _, err := histogramVecToMetrics(count, sum, bucket, invocationLabelNames)
	require.Error(t, err)
}

func TestQueryResultsToMetrics_IncompleteHistogramKeepsOtherFamilies(t *testing.T) {
	// One complete series for every config.
	vectors := map[string]model.Vector{}
	for _, config := range MetricConfigs {
		labels := []string{}
		for _, ln := range config.LabelNames {
			labels = append(labels, ln, "v")
		}
		name := config.sourceMetricName
		if config.ExportedFamily.GetType() == dto.MetricType_HISTOGRAM {
			vectors[name+countSuffix] = model.Vector{sample(1, labels...)}
			vectors[name+sumSuffix] = model.Vector{sample(1, labels...)}
			vectors[name+bucketSuffix] = model.Vector{sample(1, withLE(labels, "+Inf")...)}
			continue
		}
		vectors[name] = model.Vector{sample(1, labels...)}
	}
	// The old pod's count is still visible but its buckets are not.
	count, sum, bucket := histogramVectors("app-old", "app-new")
	vectors[invocationDuration+countSuffix] = count
	vectors[invocationDuration+sumSuffix] = sum
	vectors[invocationDuration+bucketSuffix] = bucket[2:]

	res, skipped, err := queryResultsToMetrics(vectors)
	require.NoError(t, err)
	require.Equal(t, map[skippedSeriesKey]int{
		{invocationDuration, metrics.ExportedMetricMissingBucketsReason}: 1,
	}, skipped)
	families := map[string]*dto.MetricFamily{}
	for _, family := range res.GetMetricFamilies() {
		families[family.GetName()] = family
	}
	require.Len(t, families, len(MetricConfigs))
	for _, config := range MetricConfigs {
		family := families[config.ExportedFamily.GetName()]
		if config.sourceMetricName == invocationDuration {
			require.Equal(t, []string{"app-new"}, podNames(family.GetMetric()))
			continue
		}
		require.Len(t, family.GetMetric(), 1, "family %s", family.GetName())
	}
}

func TestRecordSkippedSeries(t *testing.T) {
	counter := metrics.ExportedMetricsSkippedHistogramSeriesCount.WithLabelValues(
		invocationDuration, metrics.ExportedMetricMissingSumReason)
	before := testutil.ToFloat64(counter)

	recordSkippedSeries(context.Background(), "GR123", map[skippedSeriesKey]int{
		{invocationDuration, metrics.ExportedMetricMissingSumReason}: 2,
	})
	require.Equal(t, before+2, testutil.ToFloat64(counter))
}
