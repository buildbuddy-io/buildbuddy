package filter

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func MetricToDbField(m stat_filter.MetricType, paramPrefix string) (string, error) {
	switch m {
	case stat_filter.MetricType_DURATION_USEC_METRIC:
		return paramPrefix + "duration_usec", nil
	case stat_filter.MetricType_UPDATED_AT_USEC_METRIC:
		return paramPrefix + "updated_at_usec", nil
	case stat_filter.MetricType_CAS_CACHE_MISSES_METRIC:
		return paramPrefix + "cas_cache_misses", nil
	case stat_filter.MetricType_EXECUTION_QUEUE_TIME_USEC_METRIC:
		return fmt.Sprintf("(%sworker_start_timestamp_usec - %squeued_timestamp_usec)", paramPrefix, paramPrefix), nil
	default:
		return "", status.InvalidArgumentError(fmt.Sprintf("Invalid filter: %s", m.String()))
	}
}

func GenerateFilterStringAndArgs(f *stat_filter.StatFilter, paramPrefix string) (string, []interface{}, error) {
	metric, err := MetricToDbField(f.GetMetric(), paramPrefix)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("(%s BETWEEN ? AND ?)", metric), []interface{}{f.GetMin(), f.GetMax() - 1}, nil
}
