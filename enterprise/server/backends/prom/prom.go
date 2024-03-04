package prom

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/errgroup"

	mpb "github.com/buildbuddy-io/buildbuddy/proto/metrics"
	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
	dto "github.com/prometheus/client_model/go"
)

const (
	podNameLabel = "pod_name"
)

var (
	address = flag.String("prometheus.address", "", "the address of the promethus HTTP API")

	// MetricConfigs define a list of metrics we will fetch from prometheus and
	// export to customers.
	MetricConfigs = []*MetricConfig{
		{
			sourceMetricName: "buildbuddy_remote_execution_queue_length",
			LabelNames:       []string{podNameLabel},
			ExportedFamily: &dto.MetricFamily{
				Name: proto.String("exported_buildbuddy_remote_execution_queue_length"),
				Help: proto.String("Number of actions currently waiting in the executor queue."),
				Type: dto.MetricType_GAUGE.Enum(),
			},
			Examples: "sum by(pod_name) (exported_buildbuddy_remote_execution_queue_length)",
		},
		{
			sourceMetricName: "buildbuddy_invocation_duration_usec_exported",
			LabelNames:       []string{metrics.InvocationStatusLabel, podNameLabel},
			ExportedFamily: &dto.MetricFamily{
				Name: proto.String("exported_buildbuddy_invocation_duration_usec"),
				Help: proto.String("The total duration of each invocation, in **microseconds**."),
				Type: dto.MetricType_HISTOGRAM.Enum(),
			},
			Examples: `# Median invocation duration in the past 5 minutes
histogram_quantile(
0.5,
sum(rate(exported_buildbuddy_invocation_duration_usec_bucket[5m])) by (le)
)

# Number of invocations per Second
sum by (invocation_status) (rate(exported_buildbuddy_invocation_duration_usec_count[5m]))
`,
		},
		{
			sourceMetricName: "buildbuddy_remote_cache_num_hits_exported",
			LabelNames:       []string{metrics.CacheTypeLabel, podNameLabel},
			ExportedFamily: &dto.MetricFamily{
				Name: proto.String("exported_buildbuddy_remote_cache_num_hits"),
				Help: proto.String("Number of cache hits."),
				Type: dto.MetricType_COUNTER.Enum(),
			},
			Examples: `# Number of Hits as measured over the last week
sum by (cache_type) (increase(exported_buildbuddy_remote_cache_num_hits[1w]))`,
		},
		{
			sourceMetricName: "buildbuddy_remote_cache_download_size_bytes_exported",
			LabelNames:       []string{podNameLabel},
			ExportedFamily: &dto.MetricFamily{
				Name: proto.String("exported_buildbuddy_remote_cache_download_size_bytes"),
				Help: proto.String("Number of bytes downloaded from the remote cache."),
				Type: dto.MetricType_COUNTER.Enum(),
			},
			Examples: `# Number of bytes downloaded as measured over the last week
sum(increase(exported_buildbuddy_remote_cache_download_size_bytes[1w]))`,
		},
		{
			sourceMetricName: "buildbuddy_remote_cache_upload_size_bytes_exported",
			LabelNames:       []string{podNameLabel},
			ExportedFamily: &dto.MetricFamily{
				Name: proto.String("exported_buildbuddy_remote_cache_upload_size_bytes"),
				Help: proto.String("Number of bytes uploaded to the remote cache."),
				Type: dto.MetricType_COUNTER.Enum(),
			},
			Examples: `# Number of bytes uploaded as measured over the last week
sum(increase(exported_buildbuddy_remote_cache_upload_size_bytes[1w]))`,
		},
		{
			sourceMetricName: "buildbuddy_remote_execution_duration_usec_exported",
			LabelNames:       []string{metrics.OS},
			ExportedFamily: &dto.MetricFamily{
				Name: proto.String("exported_buildbuddy_remote_execution_duration_usec"),
				Help: proto.String("The total duration of remote execution, in **microseconds**."),
				Type: dto.MetricType_HISTOGRAM.Enum(),
			},
			Examples: `# The total duration of remote execution as measured over the last week
sum by (os) (rate(exported_buildbuddy_remote_execution_duration_usec_sum[1w]))`,
		},
	}
)

const (
	redisMetricsKeyPrefix = "exportedMetrics"
	// The version in redis cache, as part of the redis key.
	version = "v3"
	// The time for the metrics to expire in redis.
	metricsExpiration = 30*time.Second - 50*time.Millisecond

	// The label name that indicates the upper bound of a bucket in a histogram
	leLabel = "le"

	// A prometheus histogram generates three metrics: sum, count, and bucket.
	sumSuffix    = "_sum"
	countSuffix  = "_count"
	bucketSuffix = "_bucket"
)

type promQuerier struct {
	api promapi.API
	rdb redis.UniversalClient
}

type MetricConfig struct {
	sourceMetricName string
	LabelNames       []string
	ExportedFamily   *dto.MetricFamily
	// The examples should be in promql, and is going to be shown in the docs.
	Examples string
}

// bbMetric implements prometheus.Metric Interface.
type bbMetric struct {
	family *dto.MetricFamily
	metric *dto.Metric
}

func (m *bbMetric) Desc() *prometheus.Desc {
	labelNames := make([]string, 0, len(m.metric.GetLabel()))

	for _, label := range m.metric.Label {
		labelNames = append(labelNames, *label.Name)
	}
	return prometheus.NewDesc(m.family.GetName(), m.family.GetHelp(), labelNames, nil)
}

func (m *bbMetric) Write(out *dto.Metric) error {
	proto.Merge(out, m.metric)
	return nil
}

type bbMetricsCollector struct {
	env     environment.Env
	groupID string
}

type promQueryParams struct {
	metricName  string
	sumByFields []string
}

func Register(env *real_environment.RealEnv) error {
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

func newCollector(env environment.Env, groupID string) *bbMetricsCollector {
	return &bbMetricsCollector{
		env:     env,
		groupID: groupID,
	}
}

// Describe implements the prometheus.Collector interface
func (c *bbMetricsCollector) Describe(out chan<- *prometheus.Desc) {
	for _, c := range MetricConfigs {
		f := c.ExportedFamily
		out <- prometheus.NewDesc(f.GetName(), f.GetHelp(), c.LabelNames, nil)
	}
}

// Collect implements the prometheus.Collector interface
func (c *bbMetricsCollector) Collect(out chan<- prometheus.Metric) {
	promQuerier := c.env.GetPromQuerier()
	if promQuerier == nil {
		log.Error("prom querier not set up")
		return
	}

	metricFamilies, err := promQuerier.FetchMetrics(c.env.GetServerContext(), c.groupID)
	if err != nil {
		log.Warningf("error fetch metrics: %v", err)
		return
	}

	for _, family := range metricFamilies {
		for _, metric := range family.Metric {
			out <- &bbMetric{family: family, metric: metric}
		}
	}
}

func (q *promQuerier) FetchMetrics(ctx context.Context, groupID string) ([]*dto.MetricFamily, error) {
	cachedMetrics, err := q.getCachedMetrics(ctx, groupID)
	if err != nil {
		log.Warningf("failed to get cached metrics: %s", err)
		// Failed to get metrics from Redis. Let's try query prometheus.
	}
	if cachedMetrics != nil {
		return cachedMetrics.GetMetricFamilies(), nil
	}

	vectorMap, err := q.fetchMetrics(ctx, groupID)
	if err != nil {
		return nil, status.InternalErrorf("failed to fetch metrics from prometheus: %s", err)
	}
	metricFamilies, err := queryResultsToMetrics(vectorMap)
	if err != nil {
		return nil, status.InternalErrorf("failed to prase metrics fetched from prometheus: %s", err)
	}

	err = q.setMetrics(ctx, groupID, metricFamilies)
	if err != nil {
		log.Warningf("failed to set metrics to redis: %s", err)
	}
	return metricFamilies.GetMetricFamilies(), nil
}

func (q *promQuerier) fetchMetrics(ctx context.Context, groupID string) (map[string]model.Vector, error) {
	now := time.Now()
	res := make(map[string]model.Vector)
	mu := &sync.Mutex{}

	queryParams := make([]*promQueryParams, 0, 3*len(MetricConfigs))
	for _, config := range MetricConfigs {
		switch config.ExportedFamily.GetType() {
		case dto.MetricType_GAUGE:
			queryParams = append(queryParams, &promQueryParams{
				metricName: config.sourceMetricName, sumByFields: config.LabelNames,
			})
		case dto.MetricType_COUNTER:
			queryParams = append(queryParams, &promQueryParams{
				metricName: config.sourceMetricName, sumByFields: config.LabelNames,
			})
		case dto.MetricType_HISTOGRAM:
			queryParams = append(queryParams,
				&promQueryParams{
					metricName:  config.sourceMetricName + countSuffix,
					sumByFields: config.LabelNames,
				},
				&promQueryParams{
					metricName:  config.sourceMetricName + sumSuffix,
					sumByFields: config.LabelNames,
				},
				&promQueryParams{
					metricName:  config.sourceMetricName + bucketSuffix,
					sumByFields: append(config.LabelNames, leLabel),
				},
			)
		default:
			return nil, status.InvalidArgumentErrorf("unsupported type :%s", config.ExportedFamily.GetType())
		}
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

func queryResultsToMetrics(vectors map[string]model.Vector) (*mpb.Metrics, error) {
	m := make(map[string]*dto.MetricFamily)

	for _, config := range MetricConfigs {
		family, ok := m[config.ExportedFamily.GetName()]
		if !ok {
			family = proto.Clone(config.ExportedFamily).(*dto.MetricFamily)
		}

		if config.ExportedFamily.GetType() == dto.MetricType_COUNTER {
			vector, ok := vectors[config.sourceMetricName]
			if !ok {
				return nil, status.InternalErrorf("miss metric %q", config.sourceMetricName)
			}
			metric, err := counterVecToMetrics(vector, config.LabelNames)
			if err != nil {
				return nil, status.InternalErrorf("failed to parse metric %q: %s", config.sourceMetricName, err)
			}
			family.Metric = append(family.GetMetric(), metric...)
		} else if config.ExportedFamily.GetType() == dto.MetricType_GAUGE {
			vector, ok := vectors[config.sourceMetricName]
			if !ok {
				return nil, status.InternalErrorf("miss metric %q", config.sourceMetricName)
			}
			metric, err := gaugeVecToMetrics(vector, config.LabelNames)
			if err != nil {
				return nil, status.InternalErrorf("failed to parse metric %q: %s", config.sourceMetricName, err)
			}
			family.Metric = append(family.GetMetric(), metric...)
		} else if config.ExportedFamily.GetType() == dto.MetricType_HISTOGRAM {
			sumVector, sumExists := vectors[config.sourceMetricName+sumSuffix]
			countVector, countExists := vectors[config.sourceMetricName+countSuffix]
			bucketVector, bucketExists := vectors[config.sourceMetricName+bucketSuffix]
			if !countExists || !sumExists || !bucketExists {
				return nil, status.InternalErrorf("missing metric %q for histogram", config.sourceMetricName)
			}
			metric, err := histogramVecToMetrics(countVector, sumVector, bucketVector, config.LabelNames)
			if err != nil {
				return nil, status.InternalErrorf("failed to parse metric %q: %s", config.sourceMetricName, err)
			}
			family.Metric = append(family.GetMetric(), metric...)
		}

		m[config.ExportedFamily.GetName()] = family
	}

	res := &mpb.Metrics{}
	for _, mf := range m {
		res.MetricFamilies = append(res.GetMetricFamilies(), mf)
	}
	return res, nil
}

func makeLabelPairs(labelNames []string, sample *model.Sample) ([]*dto.LabelPair, error) {
	labelPairs := make([]*dto.LabelPair, 0, len(labelNames))
	for _, ln := range labelNames {
		lv, ok := sample.Metric[model.LabelName(ln)]
		if !ok {
			return nil, status.InternalErrorf("miss label name %q", ln)
		}
		labelPairs = append(labelPairs, &dto.LabelPair{
			Name:  proto.String(ln),
			Value: proto.String(string(lv)),
		})
	}
	return labelPairs, nil
}

func counterVecToMetrics(vector model.Vector, labelNames []string) ([]*dto.Metric, error) {
	res := make([]*dto.Metric, 0, len(vector))
	for _, promSample := range vector {
		labelPairs, err := makeLabelPairs(labelNames, promSample)
		if err != nil {
			return nil, err
		}
		sample := &dto.Metric{
			Label: labelPairs,
			Counter: &dto.Counter{
				Value: proto.Float64(float64(promSample.Value)),
			},
		}
		res = append(res, sample)
	}
	return res, nil
}

func gaugeVecToMetrics(vector model.Vector, labelNames []string) ([]*dto.Metric, error) {
	res := make([]*dto.Metric, 0, len(vector))
	for _, promSample := range vector {
		labelPairs, err := makeLabelPairs(labelNames, promSample)
		if err != nil {
			return nil, err
		}
		sample := &dto.Metric{
			Label: labelPairs,
			Gauge: &dto.Gauge{
				Value: proto.Float64(float64(promSample.Value)),
			},
		}
		res = append(res, sample)
	}
	return res, nil
}

func histogramVecToMetrics(countVec, sumVec, bucketVec model.Vector, labelNames []string) ([]*dto.Metric, error) {
	sumByFingerprint := make(map[model.Fingerprint]float64)
	for _, sample := range sumVec {
		footprint := sample.Metric.Fingerprint()
		sumByFingerprint[footprint] = float64(sample.Value)
	}

	bucketsByFingerprint := make(map[model.Fingerprint][]*dto.Bucket)
	for _, sample := range bucketVec {
		leValue, ok := sample.Metric[leLabel]
		if !ok {
			return nil, status.InternalErrorf("miss 'le' value for bucket vector, label set: %s", sample.Metric)
		}
		upperBound, err := strconv.ParseFloat(string(leValue), 64)
		if err != nil {
			return nil, status.InternalErrorf("failed to parse %q to float64", leValue)
		}
		bucket := &dto.Bucket{
			UpperBound:      proto.Float64(upperBound),
			CumulativeCount: proto.Uint64(uint64(sample.Value)),
		}

		// remove leLabel
		delete(model.LabelSet(sample.Metric), model.LabelName(leLabel))
		fingerprint := sample.Metric.Fingerprint()
		bucketsByFingerprint[fingerprint] = append(bucketsByFingerprint[fingerprint], bucket)
	}

	res := make([]*dto.Metric, 0, len(countVec))

	for _, sample := range countVec {
		fingerprint := sample.Metric.Fingerprint()
		sum, ok := sumByFingerprint[fingerprint]
		if !ok {
			return nil, status.InternalErrorf("miss sum value for metric, label set %s", sample.Metric)
		}
		labelPairs, err := makeLabelPairs(labelNames, sample)
		if err != nil {
			return nil, err
		}
		buckets, ok := bucketsByFingerprint[fingerprint]
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].GetUpperBound() < buckets[j].GetUpperBound()
		})
		if !ok {
			return nil, status.InternalErrorf("miss bucket value for metric, label set %s", sample.Metric)
		}

		res = append(res, &dto.Metric{
			Label: labelPairs,
			Histogram: &dto.Histogram{
				SampleCount: proto.Uint64(uint64(sample.Value)),
				SampleSum:   proto.Float64(sum),
				Bucket:      buckets,
			},
		})
	}
	return res, nil
}

func getExportedMetricsKey(groupID string) string {
	return strings.Join([]string{redisMetricsKeyPrefix, version, groupID}, "/")
}

func (q *promQuerier) setMetrics(ctx context.Context, groupID string, metricFamilies *mpb.Metrics) error {
	if q.rdb == nil {
		return nil
	}
	key := getExportedMetricsKey(groupID)
	b, err := proto.Marshal(metricFamilies)
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
