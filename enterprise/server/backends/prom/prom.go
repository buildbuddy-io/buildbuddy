package prom

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/errgroup"

	"google.golang.org/protobuf/proto"

	mpb "github.com/buildbuddy-io/buildbuddy/proto/metrics"
	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
)

var (
	address = flag.String("prometheus.address", "", "the address of the promethus HTTP API")

	gaugeTypesToFetch     = []mpb.GaugeType{mpb.GaugeType_REMOTE_EXECUTION_QUEUE_LENGTH}
	histogramTypesToFetch = []mpb.HistogramType{mpb.HistogramType_INVOCATION_DURATION}
)

const (
	redisMetricsKeyPrefix = "exportedMetrics"
	version               = "v1"
	metricsExpiration     = 30*time.Second - 50*time.Millisecond
	leLabel               = "le"
)

type promQuerier struct {
	api promapi.API
	rdb redis.UniversalClient
}

type bbMetricsCollector struct {
	env     environment.Env
	groupID string

	// Metrics
	gaugeDescByType     map[mpb.GaugeType]*prometheus.Desc
	histogramDescByType map[mpb.HistogramType]*prometheus.Desc
}

type promQueryParams struct {
	metricName  string
	sumByFields []string
}

func Register(env environment.Env) error {
	if len(*address) == 0 {
		return nil
	}
	c, err := api.NewClient(api.Config{
		Address: *address,
	})
	if err != nil {
		return status.InternalErrorf("failed to configure prom querier: %s", err)
	}
	q := &promQuerier{
		api: promapi.NewAPI(c),
		rdb: env.GetDefaultRedisClient(),
	}
	env.SetPromQuerier(q)
	return nil
}

func NewRegistry(env environment.Env, groupID string) (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	err := reg.Register(newCollector(env, groupID))
	if err != nil {
		log.Errorf("unable to register prometheus registry for group ID %q:%s", groupID, err)
		return nil, err
	}
	return reg, nil
}

func getHistogramMetricPrefix(histogramType mpb.HistogramType) (string, error) {
	switch histogramType {
	case mpb.HistogramType_INVOCATION_DURATION:
		return "buildbuddy_invocation_duration_usec_exported", nil
	default:
		return "", status.InvalidArgumentErrorf("unknown histogram type %s", histogramType)
	}
}

func getHistogramLabelNames(histogramType mpb.HistogramType) ([]string, error) {
	switch histogramType {
	case mpb.HistogramType_INVOCATION_DURATION:
		return []string{metrics.InvocationStatusLabel}, nil
	default:
		return nil, status.InvalidArgumentErrorf("unknown histogram type:%s", histogramType)
	}
}

func getGaugeLabelNames(gaugeType mpb.GaugeType) ([]string, error) {
	switch gaugeType {
	case mpb.GaugeType_REMOTE_EXECUTION_QUEUE_LENGTH:
		return []string{}, nil
	default:
		return nil, status.InvalidArgumentErrorf("unknown gauge type: %s", gaugeType)
	}
}

func getHistogramCountMetricName(prefix string) string {
	return prefix + "_count"
}

func getHistogramSumMetricName(prefix string) string {
	return prefix + "_sum"
}

func getHistogramBucketsMetricName(prefix string) string {
	return prefix + "_bucket"
}

func getHistogramQueryParams(histogramType mpb.HistogramType) ([]*promQueryParams, error) {
	prefix, err := getHistogramMetricPrefix(histogramType)
	if err != nil {
		return nil, err
	}
	labels, err := getHistogramLabelNames(histogramType)
	if err != nil {
		return nil, err
	}
	return []*promQueryParams{
		{metricName: getHistogramCountMetricName(prefix), sumByFields: labels},
		{metricName: getHistogramSumMetricName(prefix), sumByFields: labels},
		{metricName: getHistogramBucketsMetricName(prefix), sumByFields: append(labels, leLabel)},
	}, nil
}

func getGaugeMetricName(gaugeType mpb.GaugeType) (string, error) {
	switch gaugeType {
	case mpb.GaugeType_REMOTE_EXECUTION_QUEUE_LENGTH:
		return "buildbuddy_remote_exectuion_queue_length", nil
	default:
		return "", status.InvalidArgumentErrorf("unexpected gauge type: %s", gaugeType)
	}
}

func getGaugeQueryParams(gaugeType mpb.GaugeType) (*promQueryParams, error) {
	name, err := getGaugeMetricName(gaugeType)
	if err != nil {
		return nil, err
	}
	labels, err := getGaugeLabelNames(gaugeType)
	if err != nil {
		return nil, err
	}
	switch gaugeType {
	case mpb.GaugeType_REMOTE_EXECUTION_QUEUE_LENGTH:
		return &promQueryParams{
			metricName:  name,
			sumByFields: labels,
		}, nil
	default:
		return nil, status.InvalidArgumentErrorf("unexpected gauge type: %s", gaugeType)
	}
}

func newCollector(env environment.Env, groupID string) *bbMetricsCollector {
	return &bbMetricsCollector{
		env:     env,
		groupID: groupID,

		gaugeDescByType: map[mpb.GaugeType]*prometheus.Desc{
			mpb.GaugeType_REMOTE_EXECUTION_QUEUE_LENGTH: prometheus.NewDesc(
				"exported_builbduddy_remote_execution_queue_length",
				"Number of actions currently waiting in the executor queue.",
				[]string{},
				nil),
		},
		histogramDescByType: map[mpb.HistogramType]*prometheus.Desc{
			mpb.HistogramType_INVOCATION_DURATION: prometheus.NewDesc(
				"exported_builbuddy_invocation_duration_usec",
				"The total duration of each invocation, in **microseconds**.",
				[]string{
					metrics.InvocationStatusLabel,
				},
				nil,
			),
		},
	}
}

// Describe implements the prometheus.Collector interface
func (c *bbMetricsCollector) Describe(out chan<- *prometheus.Desc) {
	for _, desc := range c.gaugeDescByType {
		out <- desc
	}

	for _, desc := range c.histogramDescByType {
		out <- desc
	}
}

// Collect implements the prometheus.Collector interface
func (c *bbMetricsCollector) Collect(out chan<- prometheus.Metric) {
	promQuerier := c.env.GetPromQuerier()
	if promQuerier == nil {
		log.Error("prom querier not set up")
		return
	}

	metricsProto, err := promQuerier.FetchMetrics(c.env.GetServerContext(), c.groupID)
	if err != nil {
		log.Warningf("error fetch metrics: %v", err)
		return
	}

	c.processMetrics(out, metricsProto)
}

func (c *bbMetricsCollector) processMetrics(out chan<- prometheus.Metric, metricsProto *mpb.Metrics) {
	for _, gauge := range metricsProto.GetGauges() {
		desc, ok := c.gaugeDescByType[gauge.GetType()]
		if !ok {
			log.Warningf("cannot find prometheus desc for gauge type %s", gauge.GetType())
		}
		labelNames, err := getGaugeLabelNames(gauge.GetType())
		if err != nil {
			log.Warningf("cannot find label names for gauge type %s", gauge.GetType())
		}
		labelValues := make([]string, 0, len(labelNames))
		for _, ln := range labelNames {
			labelValues = append(labelValues, gauge.GetLabelSet()[ln])
		}
		out <- prometheus.MustNewConstMetric(
			desc,
			prometheus.GaugeValue,
			gauge.GetValue(),
			labelValues...,
		)
	}

	for _, histogram := range metricsProto.GetHistograms() {
		desc, ok := c.histogramDescByType[histogram.GetType()]
		if !ok {
			log.Warningf("cannot find prometheus desc for histogram type %s", histogram.GetType())
		}
		labelNames, err := getHistogramLabelNames(histogram.GetType())
		if err != nil {
			log.Warningf("cannot find label names for histogram type %s", histogram.GetType())
		}
		labelValues := make([]string, 0, len(labelNames))
		for _, ln := range labelNames {
			labelValues = append(labelValues, histogram.GetLabelSet()[ln])
		}

		buckets := make(map[float64]uint64)
		for _, b := range histogram.GetBuckets() {
			buckets[b.GetLe()] = b.GetValue()
		}
		out <- prometheus.MustNewConstHistogram(
			desc,
			histogram.GetCount(),
			histogram.GetSum(),
			buckets,
			labelValues...)
	}
}

func (q *promQuerier) FetchMetrics(ctx context.Context, groupID string) (*mpb.Metrics, error) {
	metrics, err := q.getCachedMetrics(ctx, groupID)
	if err != nil {
		log.Warningf("failed to get cached metrics: %s", err)
		// Failed to get metrics from Redis. Let's try query prometheus.
	}
	if metrics != nil {
		return metrics, nil
	}

	vectorMap, err := q.fetchMetrics(ctx, groupID)
	if err != nil {
		return nil, status.InternalErrorf("failed to fetch metrics from prometheus: %s", err)
	}
	metrics, err = vectorsToProto(vectorMap)

	err = q.setMetrics(ctx, groupID, metrics)
	if err != nil {
		log.Warningf("failed to set metrics to redis: %s", err)
	}
	return metrics, nil
}

func (q *promQuerier) fetchMetrics(ctx context.Context, groupID string) (map[string]model.Vector, error) {
	now := time.Now()
	res := make(map[string]model.Vector)
	mu := &sync.Mutex{}
	queryParams := make([]*promQueryParams, 0, len(gaugeTypesToFetch)+3*len(histogramTypesToFetch))

	for _, histogramType := range histogramTypesToFetch {
		qp, err := getHistogramQueryParams(histogramType)
		if err != nil {
			return nil, err
		}
		queryParams = append(queryParams, qp...)
	}
	for _, gaugeType := range gaugeTypesToFetch {
		qp, err := getGaugeQueryParams(gaugeType)
		if err != nil {
			return nil, err
		}
		queryParams = append(queryParams, qp)
	}

	eg, egCtx := errgroup.WithContext(ctx)

	for _, p := range queryParams {
		p := p
		eg.Go(func() error {
			vec, err := q.query(egCtx, p.metricName, p.sumByFields, groupID, now)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()

			res[p.metricName] = vec
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return res, nil
}

func (q *promQuerier) query(ctx context.Context, metricName string, sumByFields []string, groupID string, now time.Time) (model.Vector, error) {
	query := ""
	if len(sumByFields) > 0 {
		query = fmt.Sprintf("sum by (%s)(%s{group_id='%s'})", strings.Join(sumByFields, ","), metricName, groupID)
	} else {
		query = fmt.Sprintf("sum(%s{group_id='%s'})", metricName, groupID)
	}
	result, _, err := q.api.Query(ctx, query, now)
	if err != nil {
		return nil, err
	}
	resultVector, ok := result.(model.Vector)
	if !ok {
		return nil, status.InternalErrorf("failed to query Prometheus: unexpected type %T", result)
	}
	return resultVector, nil
}

func vectorsToProto(vectors map[string]model.Vector) (*mpb.Metrics, error) {
	res := &mpb.Metrics{}

	for _, gaugeType := range gaugeTypesToFetch {
		gauges, err := gaugeVecToProto(vectors, gaugeType)
		if err != nil {
			return nil, err
		}
		res.Gauges = append(res.Gauges, gauges...)
	}

	for _, histogramType := range histogramTypesToFetch {
		histograms, err := histogramVecToProto(vectors, histogramType)
		if err != nil {
			return nil, err
		}
		res.Histograms = append(res.Histograms, histograms...)
	}

	return res, nil
}

func gaugeVecToProto(vectors map[string]model.Vector, gaugeType mpb.GaugeType) ([]*mpb.Gauge, error) {
	name, err := getGaugeMetricName(gaugeType)
	if err != nil {
		return nil, err
	}
	vec, ok := vectors[name]
	if !ok {
		return nil, status.InternalErrorf("miss metric %q", name)
	}
	res := make([]*mpb.Gauge, 0, len(vec))
	for _, promSample := range vec {
		labelSet := make(map[string]string)
		for labelName, labelValue := range promSample.Metric {
			labelSet[string(labelName)] = string(labelValue)
		}
		sample := &mpb.Gauge{
			Type:     gaugeType,
			LabelSet: labelSet,
			Value:    float64(promSample.Value),
		}
		res = append(res, sample)
	}
	return res, nil
}

func histogramVecToProto(vectors map[string]model.Vector, histogramType mpb.HistogramType) ([]*mpb.Histogram, error) {
	prefix, err := getHistogramMetricPrefix(histogramType)
	if err != nil {
		return nil, err
	}
	countName := getHistogramCountMetricName(prefix)
	countVec, countExists := vectors[countName]
	sumName := getHistogramSumMetricName(prefix)
	sumVec, sumExists := vectors[sumName]
	bucketsName := getHistogramBucketsMetricName(prefix)
	bucketsVec, bucketsExists := vectors[bucketsName]
	if !countExists || !sumExists || !bucketsExists {
		log.Warningf("miss metric for histogram %s", histogramType)
		return nil, nil
	}

	sumByFingerprint := make(map[model.Fingerprint]float64)
	for _, sample := range sumVec {
		footprint := sample.Metric.Fingerprint()
		sumByFingerprint[footprint] = float64(sample.Value)
	}

	bucketsByFingerprint := make(map[model.Fingerprint][]*mpb.Histogram_Bucket)
	for _, sample := range bucketsVec {
		leStr, ok := sample.Metric[leLabel]
		if !ok {
			log.Warningf("miss 'le' value for metric %s, label set: %s", bucketsName, sample.Metric)
			continue
		}
		le, err := strconv.ParseFloat(string(leStr), 64)
		if err != nil {
			log.Warningf("failed to parse %q to float64", leStr)
		}
		bucket := &mpb.Histogram_Bucket{
			Le:    le,
			Value: uint64(sample.Value),
		}
		// remove leLabel

		delete(model.LabelSet(sample.Metric), model.LabelName(leLabel))
		fingerprint := sample.Metric.Fingerprint()
		bucketsByFingerprint[fingerprint] = append(bucketsByFingerprint[fingerprint], bucket)
	}

	res := make([]*mpb.Histogram, 0, len(countVec))

	for _, sample := range countVec {
		fingerprint := sample.Metric.Fingerprint()
		sum, ok := sumByFingerprint[fingerprint]
		if !ok {
			log.Warningf("miss sum value for metric %s, label set: %s", sumName, sample.Metric)
			continue
		}
		labelSet := make(map[string]string)
		for labelName, labelValue := range sample.Metric {
			labelSet[string(labelName)] = string(labelValue)
		}
		buckets, ok := bucketsByFingerprint[fingerprint]
		if !ok {
			log.Warningf("miss bucket value for metric %s, label set: %s", bucketsName, sample.Metric)
			continue
		}

		res = append(res, &mpb.Histogram{
			Type:     histogramType,
			LabelSet: labelSet,
			Count:    uint64(sample.Value),
			Sum:      sum,
			Buckets:  buckets,
		})
	}
	return res, nil
}

func getExportedMetricsKey(groupID string) string {
	return strings.Join([]string{redisMetricsKeyPrefix, version, groupID}, "/")
}

func (q *promQuerier) setMetrics(ctx context.Context, groupID string, metricsProto *mpb.Metrics) error {
	if q.rdb == nil {
		return nil
	}
	key := getExportedMetricsKey(groupID)
	b, err := proto.Marshal(metricsProto)
	if err != nil {
		return status.InternalErrorf("failed to marshal json: %s", err)
	}
	return q.rdb.Set(ctx, key, string(b), metricsExpiration).Err()

}
func (q *promQuerier) getCachedMetrics(ctx context.Context, groupID string) (*mpb.Metrics, error) {
	if q.rdb == nil {
		return nil, nil
	}
	key := getExportedMetricsKey(groupID)
	blob, err := q.rdb.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	res := &mpb.Metrics{}
	err = proto.Unmarshal([]byte(blob), res)
	if err != nil {
		return nil, status.InternalErrorf("failed to unmarshal json: %s", err)
	}
	return res, nil
}
