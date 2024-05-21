package filter

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func executionMetricToDbField(m stat_filter.ExecutionMetricType, tablePrefix string) (string, error) {
	switch m {
	case stat_filter.ExecutionMetricType_UPDATED_AT_USEC_EXECUTION_METRIC:
		return tablePrefix + "updated_at_usec", nil
	case stat_filter.ExecutionMetricType_QUEUE_TIME_USEC_EXECUTION_METRIC:
		return fmt.Sprintf("IF(%sworker_start_timestamp_usec < %squeued_timestamp_usec, 0, (%sworker_start_timestamp_usec - %squeued_timestamp_usec))", tablePrefix, tablePrefix, tablePrefix, tablePrefix), nil
	case stat_filter.ExecutionMetricType_INPUT_DOWNLOAD_TIME_EXECUTION_METRIC:
		return fmt.Sprintf("(%sinput_fetch_completed_timestamp_usec - %sinput_fetch_start_timestamp_usec)", tablePrefix, tablePrefix), nil
	case stat_filter.ExecutionMetricType_REAL_EXECUTION_TIME_EXECUTION_METRIC:
		return fmt.Sprintf("(%sexecution_completed_timestamp_usec - %sexecution_start_timestamp_usec)", tablePrefix, tablePrefix), nil
	case stat_filter.ExecutionMetricType_OUTPUT_UPLOAD_TIME_EXECUTION_METRIC:
		return fmt.Sprintf("(%soutput_upload_completed_timestamp_usec - %soutput_upload_start_timestamp_usec)", tablePrefix, tablePrefix), nil
	case stat_filter.ExecutionMetricType_PEAK_MEMORY_EXECUTION_METRIC:
		return tablePrefix + "peak_memory_bytes", nil
	case stat_filter.ExecutionMetricType_INPUT_DOWNLOAD_SIZE_EXECUTION_METRIC:
		return tablePrefix + "file_download_size_bytes", nil
	case stat_filter.ExecutionMetricType_OUTPUT_UPLOAD_SIZE_EXECUTION_METRIC:
		return tablePrefix + "file_upload_size_bytes", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func invocationMetricToDbField(m stat_filter.InvocationMetricType, tablePrefix string) (string, error) {
	switch m {
	case stat_filter.InvocationMetricType_DURATION_USEC_INVOCATION_METRIC:
		return tablePrefix + "duration_usec", nil
	case stat_filter.InvocationMetricType_UPDATED_AT_USEC_INVOCATION_METRIC:
		return tablePrefix + "updated_at_usec", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_MISSES_INVOCATION_METRIC:
		return tablePrefix + "cas_cache_misses", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_DOWNLOAD_SIZE_INVOCATION_METRIC:
		return tablePrefix + "total_download_size_bytes", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_UPLOAD_SIZE_INVOCATION_METRIC:
		return tablePrefix + "total_upload_size_bytes", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_DOWNLOAD_SPEED_INVOCATION_METRIC:
		return tablePrefix + "download_throughput_bytes_per_second", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_UPLOAD_SPEED_INVOCATION_METRIC:
		return tablePrefix + "upload_throughput_bytes_per_second", nil
	case stat_filter.InvocationMetricType_ACTION_CACHE_MISSES_INVOCATION_METRIC:
		return tablePrefix + "action_cache_misses", nil
	case stat_filter.InvocationMetricType_TIME_SAVED_USEC_INVOCATION_METRIC:
		return tablePrefix + "total_cached_action_exec_usec", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func executionDimensionToDbField(m stat_filter.ExecutionDimensionType, tablePrefix string) (string, error) {
	switch m {
	case stat_filter.ExecutionDimensionType_WORKER_EXECUTION_DIMENSION:
		return tablePrefix + "worker", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func invocationDimensionToDbField(m stat_filter.InvocationDimensionType, tablePrefix string) (string, error) {
	switch m {
	case stat_filter.InvocationDimensionType_BRANCH_INVOCATION_DIMENSION:
		return tablePrefix + "branch", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func MetricToDbField(m *stat_filter.Metric, tablePrefix string) (string, error) {
	if m == nil {
		return "", status.InvalidArgumentErrorf("Filter metric must not be nil")
	}
	if m.Invocation != nil {
		return invocationMetricToDbField(m.GetInvocation(), tablePrefix)
	} else if m.Execution != nil {
		return executionMetricToDbField(m.GetExecution(), tablePrefix)
	}
	return "", status.InvalidArgumentErrorf("Invalid filter: %v", m)
}

func GenerateFilterStringAndArgs(f *stat_filter.StatFilter, tablePrefix string) (string, []interface{}, error) {
	metric, err := MetricToDbField(f.GetMetric(), tablePrefix)
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

func DimensionToDbField(m *stat_filter.Dimension, tablePrefix string) (string, error) {
	if m == nil {
		return "", status.InvalidArgumentErrorf("Filter dimension must not be nil")
	}
	if m.Invocation != nil {
		return invocationDimensionToDbField(m.GetInvocation(), tablePrefix)
	} else if m.Execution != nil {
		return executionDimensionToDbField(m.GetExecution(), tablePrefix)
	}
	return "", status.InvalidArgumentErrorf("Invalid filter: %v", m)
}

func GenerateDimensionFilterStringAndArgs(f *stat_filter.DimensionFilter, tablePrefix string) (string, []interface{}, error) {
	metric, err := DimensionToDbField(f.GetDimension(), tablePrefix)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("(%s = ?)", metric), []interface{}{f.GetValue()}, nil
}
