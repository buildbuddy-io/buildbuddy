package filter

import (
	"fmt"
	"log"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func GetStringAndArgs(ft stat_filter.FilterType, fto *stat_filter.FilterTypeOptions, foo *stat_filter.FilterOperandOptions, v *stat_filter.FilterValue, tablePrefix string, negate bool) (string, []interface{}, error) {
	var args []interface{}
	if fto.GetCategory() == stat_filter.FilterCategory_INT_FILTER_CATEGORY {
		if foo.GetArgumentCount() == stat_filter.FilterArgumentCount_ONE_FILTER_ARGUMENT_COUNT && len(v.GetIntValue()) == 1 {
			args = []interface{}{v.GetIntValue()[0]}
		} else if foo.GetArgumentCount() == stat_filter.FilterArgumentCount_MANY_FILTER_ARGUMENT_COUNT && len(v.GetIntValue()) > 0 {
			args = []interface{}{v.GetIntValue()}
		} else {
			return "", nil, status.InvalidArgumentError("invalid value.")
		}
	} else if fto.GetCategory() == stat_filter.FilterCategory_STRING_FILTER_CATEGORY {
		if foo.GetArgumentCount() == stat_filter.FilterArgumentCount_ONE_FILTER_ARGUMENT_COUNT && len(v.GetStringValue()) == 1 {
			args = []interface{}{v.GetStringValue()[0]}
		} else if foo.GetArgumentCount() == stat_filter.FilterArgumentCount_MANY_FILTER_ARGUMENT_COUNT && len(v.GetStringValue()) > 0 {
			args = []interface{}{v.GetStringValue()}
		} else {
			return "", nil, status.InvalidArgumentError("invalid value.")
		}
	} else {
		return "", nil, status.InvalidArgumentError("unknown filter category")
	}

	fieldName := tablePrefix + fto.GetDatabaseColumnName()
	var str string
	if ft != stat_filter.FilterType_TEXT_MATCH_FILTER_TYPE {
		str = strings.ReplaceAll(foo.GetDatabaseQueryString(), "?field", fieldName)
	} else {
		str = foo.GetDatabaseQueryString()
		args = []interface{}{args[0], args[0], args[0]}
	}
	str = strings.ReplaceAll(str, "?value", "?")
	if negate {
		str = fmt.Sprintf("NOT(%s)", str)
	}

	log.Printf("%+v, %+v", str, args)

	return str, args, nil
}

func ValidateAndGenerateGenericFilterQueryStringAndArgs(f *stat_filter.GenericFilter, tablePrefix string) (string, []interface{}, error) {
	if (f == nil) {
		return "", nil, status.InvalidArgumentError("invalid nil entry in filter list")
	}
	operandDescriptorOptions := protoreflect.EnumValueDescriptor(f.GetOperand().Descriptor().Values().ByNumber(f.GetOperand().Number())).Options()
	if (operandDescriptorOptions == nil) {
		return "", nil, status.InvalidArgumentError("unknown enum value????")
	}
	operandOptions := proto.GetExtension(operandDescriptorOptions, stat_filter.E_FilterOperandOptions).(*stat_filter.FilterOperandOptions)
	if (operandOptions == nil) {
		return "", nil, status.InvalidArgumentError("man, i dunno")
	}

	typeDescriptorOptions := protoreflect.EnumValueDescriptor(f.GetType().Descriptor().Values().ByNumber(f.GetType().Number())).Options()
	if (typeDescriptorOptions == nil) {
		return "", nil, status.InvalidArgumentError("unknown enum value????")
	}
	typeOptions := proto.GetExtension(typeDescriptorOptions, stat_filter.E_FilterTypeOptions).(*stat_filter.FilterTypeOptions)
	if (typeOptions == nil) {
		return "", nil, status.InvalidArgumentError("man, i dunno")
	}

	// XXX: Validate category against supported categories.
	return GetStringAndArgs(f.GetType(), typeOptions, operandOptions, f.GetValue(), tablePrefix, f.GetNegate())
}