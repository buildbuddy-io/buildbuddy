package filter

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func executionMetricToDbField(m stat_filter.ExecutionMetricType, paramPrefix string) (string, error) {
	switch m {
	case stat_filter.ExecutionMetricType_UPDATED_AT_USEC_EXECUTION_METRIC:
		return paramPrefix + "updated_at_usec", nil
	case stat_filter.ExecutionMetricType_QUEUE_TIME_USEC_EXECUTION_METRIC:
		return fmt.Sprintf("(%sworker_start_timestamp_usec - %squeued_timestamp_usec)", paramPrefix, paramPrefix), nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid filter: %s", m.String())
	}
}

func invocationMetricToDbField(m stat_filter.InvocationMetricType, paramPrefix string) (string, error) {
	switch m {
	case stat_filter.InvocationMetricType_DURATION_USEC_INVOCATION_METRIC:
		return paramPrefix + "duration_usec", nil
	case stat_filter.InvocationMetricType_UPDATED_AT_USEC_INVOCATION_METRIC:
		return paramPrefix + "updated_at_usec", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_MISSES_INVOCATION_METRIC:
		return paramPrefix + "cas_cache_misses", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid filter: %s", m.String())
	}
}

func MetricToDbField(m *stat_filter.Metric, paramPrefix string) (string, error) {
	if m.Invocation != nil {
		return invocationMetricToDbField(m.GetInvocation(), paramPrefix)
	} else if m.Execution != nil {
		return executionMetricToDbField(m.GetExecution(), paramPrefix)
	}
	return "", status.InvalidArgumentErrorf("Invalid filter: %v", m)
}

func GenerateFilterStringAndArgs(f *stat_filter.StatFilter, paramPrefix string) (string, []interface{}, error) {
	metric, err := MetricToDbField(f.GetMetric(), paramPrefix)
	if err != nil {
		return "", nil, err
	}
	if f.Max == nil && f.Min == nil {
		return "", nil, status.InvalidArgumentErrorf("No filter bounds specified: %v", f)
	}
	if f.Max != nil && f.Min != nil {
		return fmt.Sprintf("(%s BETWEEN ? AND ?)", metric), []interface{}{f.GetMin(), f.GetMax()}, nil
	}
	if f.Max != nil {
		return fmt.Sprintf("(%s <= ?)", metric), []interface{}{f.GetMax()}, nil
	}
	return fmt.Sprintf("(%s >= ?)", metric), []interface{}{f.GetMin()}, nil
}
