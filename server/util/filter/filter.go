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
		return fmt.Sprintf("IF(%sworker_start_timestamp_usec < %squeued_timestamp_usec, 0, (%sworker_start_timestamp_usec - %squeued_timestamp_usec))", paramPrefix, paramPrefix, paramPrefix, paramPrefix), nil
	case stat_filter.ExecutionMetricType_INPUT_DOWNLOAD_TIME_EXECUTION_METRIC:
		return fmt.Sprintf("(%sinput_fetch_completed_timestamp_usec - %sinput_fetch_start_timestamp_usec)", paramPrefix, paramPrefix), nil
	case stat_filter.ExecutionMetricType_REAL_EXECUTION_TIME_EXECUTION_METRIC:
		return fmt.Sprintf("(%sexecution_completed_timestamp_usec - %sexecution_start_timestamp_usec)", paramPrefix, paramPrefix), nil
	case stat_filter.ExecutionMetricType_OUTPUT_UPLOAD_TIME_EXECUTION_METRIC:
		return fmt.Sprintf("(%soutput_upload_completed_timestamp_usec - %soutput_upload_start_timestamp_usec)", paramPrefix, paramPrefix), nil
	case stat_filter.ExecutionMetricType_PEAK_MEMORY_EXECUTION_METRIC:
		return paramPrefix + "peak_memory_bytes", nil
	case stat_filter.ExecutionMetricType_INPUT_DOWNLOAD_SIZE_EXECUTION_METRIC:
		return paramPrefix + "file_download_size_bytes", nil
	case stat_filter.ExecutionMetricType_OUTPUT_UPLOAD_SIZE_EXECUTION_METRIC:
		return paramPrefix + "file_upload_size_bytes", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
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
	case stat_filter.InvocationMetricType_CAS_CACHE_DOWNLOAD_SIZE_INVOCATION_METRIC:
		return paramPrefix + "total_download_size_bytes", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_UPLOAD_SIZE_INVOCATION_METRIC:
		return paramPrefix + "total_upload_size_bytes", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_DOWNLOAD_SPEED_INVOCATION_METRIC:
		return paramPrefix + "download_throughput_bytes_per_second", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_UPLOAD_SPEED_INVOCATION_METRIC:
		return paramPrefix + "upload_throughput_bytes_per_second", nil
	case stat_filter.InvocationMetricType_ACTION_CACHE_MISSES_INVOCATION_METRIC:
		return paramPrefix + "action_cache_misses", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func MetricToDbField(m *stat_filter.Metric, paramPrefix string) (string, error) {
	if m == nil {
		return "", status.InvalidArgumentErrorf("Filter metric must not be nil")
	}
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
