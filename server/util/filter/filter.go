package filter

import (
	"fmt"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	ispb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

func executionMetricToDbField(m stat_filter.ExecutionMetricType) (string, error) {
	switch m {
	case stat_filter.ExecutionMetricType_UPDATED_AT_USEC_EXECUTION_METRIC:
		return "updated_at_usec", nil
	case stat_filter.ExecutionMetricType_QUEUE_TIME_USEC_EXECUTION_METRIC:
		return "IF(worker_start_timestamp_usec < queued_timestamp_usec, 0, (worker_start_timestamp_usec - queued_timestamp_usec))", nil
	case stat_filter.ExecutionMetricType_INPUT_DOWNLOAD_TIME_EXECUTION_METRIC:
		return "(input_fetch_completed_timestamp_usec - input_fetch_start_timestamp_usec)", nil
	case stat_filter.ExecutionMetricType_REAL_EXECUTION_TIME_EXECUTION_METRIC:
		return "(execution_completed_timestamp_usec - execution_start_timestamp_usec)", nil
	case stat_filter.ExecutionMetricType_OUTPUT_UPLOAD_TIME_EXECUTION_METRIC:
		return "(output_upload_completed_timestamp_usec - output_upload_start_timestamp_usec)", nil
	case stat_filter.ExecutionMetricType_PEAK_MEMORY_EXECUTION_METRIC:
		return "peak_memory_bytes", nil
	case stat_filter.ExecutionMetricType_INPUT_DOWNLOAD_SIZE_EXECUTION_METRIC:
		return "file_download_size_bytes", nil
	case stat_filter.ExecutionMetricType_OUTPUT_UPLOAD_SIZE_EXECUTION_METRIC:
		return "file_upload_size_bytes", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func invocationMetricToDbField(m stat_filter.InvocationMetricType) (string, error) {
	switch m {
	case stat_filter.InvocationMetricType_DURATION_USEC_INVOCATION_METRIC:
		return "duration_usec", nil
	case stat_filter.InvocationMetricType_UPDATED_AT_USEC_INVOCATION_METRIC:
		return "updated_at_usec", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_MISSES_INVOCATION_METRIC:
		return "cas_cache_misses", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_DOWNLOAD_SIZE_INVOCATION_METRIC:
		return "total_download_size_bytes", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_UPLOAD_SIZE_INVOCATION_METRIC:
		return "total_upload_size_bytes", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_DOWNLOAD_SPEED_INVOCATION_METRIC:
		return "download_throughput_bytes_per_second", nil
	case stat_filter.InvocationMetricType_CAS_CACHE_UPLOAD_SPEED_INVOCATION_METRIC:
		return "upload_throughput_bytes_per_second", nil
	case stat_filter.InvocationMetricType_ACTION_CACHE_MISSES_INVOCATION_METRIC:
		return "action_cache_misses", nil
	case stat_filter.InvocationMetricType_TIME_SAVED_USEC_INVOCATION_METRIC:
		return "total_cached_action_exec_usec", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func executionDimensionToDbField(m stat_filter.ExecutionDimensionType) (string, error) {
	switch m {
	case stat_filter.ExecutionDimensionType_WORKER_EXECUTION_DIMENSION:
		return "worker", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func invocationDimensionToDbField(m stat_filter.InvocationDimensionType) (string, error) {
	switch m {
	case stat_filter.InvocationDimensionType_BRANCH_INVOCATION_DIMENSION:
		return "branch", nil
	default:
		return "", status.InvalidArgumentErrorf("Invalid field: %s", m.String())
	}
}

func MetricToDbField(m *stat_filter.Metric) (string, error) {
	if m == nil {
		return "", status.InvalidArgumentErrorf("Filter metric must not be nil")
	}
	if m.Invocation != nil {
		return invocationMetricToDbField(m.GetInvocation())
	} else if m.Execution != nil {
		return executionMetricToDbField(m.GetExecution())
	}
	return "", status.InvalidArgumentErrorf("Invalid filter: %v", m)
}

func GenerateFilterStringAndArgs(f *stat_filter.StatFilter) (string, []interface{}, error) {
	metric, err := MetricToDbField(f.GetMetric())
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

func DimensionToDbField(m *stat_filter.Dimension) (string, error) {
	if m == nil {
		return "", status.InvalidArgumentErrorf("Filter dimension must not be nil")
	}
	if m.Invocation != nil {
		return invocationDimensionToDbField(m.GetInvocation())
	} else if m.Execution != nil {
		return executionDimensionToDbField(m.GetExecution())
	}
	return "", status.InvalidArgumentErrorf("Invalid filter: %v", m)
}

func GenerateDimensionFilterStringAndArgs(f *stat_filter.DimensionFilter) (string, []interface{}, error) {
	metric, err := DimensionToDbField(f.GetDimension())
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("(%s = ?)", metric), []interface{}{f.GetValue()}, nil
}

func getStringAndArgs(databaseQueryTemplate string, v interface{}, columnName string, negate bool) (string, []interface{}) {
	str := strings.ReplaceAll(databaseQueryTemplate, "?field", columnName)
	var args []interface{}
	for strings.Contains(str, "?value") {
		str = strings.Replace(str, "?value", "?", 1)
		args = append(args, v)
	}
	if negate {
		str = fmt.Sprintf("NOT(%s)", str)
	}
	return str, args
}

func generateStatusFilterQueryStringAndArgs(f *stat_filter.GenericFilter) (string, []interface{}, error) {
	// Currently, we only support IN queries for status.
	if f.GetOperand() != stat_filter.FilterOperand_IN_OPERAND {
		return "", nil, status.InvalidArgumentErrorf("Status filters only support the IN operand.")
	}
	if len(f.GetValue().GetStatusValue()) < 1 {
		return "", nil, status.InvalidArgumentErrorf("No values specified for status filter.")
	}

	statusClauses := query_builder.OrClauses{}
	for _, value := range f.GetValue().GetStatusValue() {
		switch value {
		case ispb.OverallStatus_SUCCESS:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS), 1)
		case ispb.OverallStatus_FAILURE:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS), 0)
		case ispb.OverallStatus_IN_PROGRESS:
			statusClauses.AddOr(`invocation_status = ?`, int(ispb.InvocationStatus_PARTIAL_INVOCATION_STATUS))
		case ispb.OverallStatus_DISCONNECTED:
			statusClauses.AddOr(`invocation_status = ?`, int(ispb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS))
		case ispb.OverallStatus_UNKNOWN_OVERALL_STATUS:
			return "", nil, status.InvalidArgumentError("Unknown invocation status is not supported.")
		default:
			return "", nil, status.InvalidArgumentErrorf("Unsupported status value: %s.", value)
		}
	}
	out, outArgs := statusClauses.Build()
	if f.GetNegate() {
		out = fmt.Sprintf("NOT(%s)", out)
	}
	return out, outArgs, nil
}

func ValidateAndGenerateGenericFilterQueryStringAndArgs(f *stat_filter.GenericFilter, qType stat_filter.ObjectTypes) (string, []interface{}, error) {
	if f == nil {
		return "", nil, status.InvalidArgumentError("invalid nil entry in filter list")
	}

	// Enum value descriptors (i.e., for "GREATER_THAN") are nested inside of
	// enum type descriptors ("Operand"), so we need to dig around for them.
	operandDescriptorOptions := protoreflect.EnumValueDescriptor(f.GetOperand().Descriptor().Values().ByNumber(f.GetOperand().Number())).Options()
	if operandDescriptorOptions == nil {
		return "", nil, status.InvalidArgumentErrorf("Unknown operand value: %s", f.GetOperand())
	}
	operandOptions := proto.GetExtension(operandDescriptorOptions, stat_filter.E_FilterOperandOptions).(*stat_filter.FilterOperandOptions)
	if operandOptions == nil {
		return "", nil, status.InternalErrorf("Operand has no options specified: %s", f.GetOperand())
	}

	typeDescriptorOptions := protoreflect.EnumValueDescriptor(f.GetType().Descriptor().Values().ByNumber(f.GetType().Number())).Options()
	if typeDescriptorOptions == nil {
		return "", nil, status.InvalidArgumentErrorf("Unknown filter type: %s", f.GetType())
	}
	typeOptions := proto.GetExtension(typeDescriptorOptions, stat_filter.E_FilterTypeOptions).(*stat_filter.FilterTypeOptions)
	if typeOptions == nil {
		return "", nil, status.InvalidArgumentErrorf("Filter type has no options specified:: %s", f.GetType())
	}

	v := f.GetValue()
	var arg interface{}
	// We have information about the field we're filtering, the type of filter
	// we are using, and the values passed to the filter.  Now we mush this all
	// together to validate the request.
	if !slices.Contains(operandOptions.GetSupportedCategories(), typeOptions.GetCategory()) {
		return "", nil, status.InvalidArgumentErrorf("Filter %s does not support operand %s", f.GetType(), f.GetOperand().String())
	}
	if !slices.Contains(typeOptions.GetSupportedObjects(), qType) {
		return "", nil, status.InvalidArgumentErrorf("Filtering by %s not supported for %s", qType, f.GetType())
	}

	// Special case: status filters are weird and occur over two db fields.
	if typeOptions.GetCategory() == stat_filter.FilterCategory_STATUS_FILTER_CATEGORY {
		return generateStatusFilterQueryStringAndArgs(f)
	}

	// Normal cases (ints, strings).
	if typeOptions.GetCategory() == stat_filter.FilterCategory_INT_FILTER_CATEGORY {
		if operandOptions.GetArgumentCount() == stat_filter.FilterArgumentCount_ONE_FILTER_ARGUMENT_COUNT && len(v.GetIntValue()) == 1 {
			arg = v.GetIntValue()[0]
		} else if operandOptions.GetArgumentCount() == stat_filter.FilterArgumentCount_MANY_FILTER_ARGUMENT_COUNT && len(v.GetIntValue()) > 0 {
			arg = v.GetIntValue()
		} else {
			return "", nil, status.InvalidArgumentErrorf("Invalid value for integer filter: %s %s Value: %+v", f.GetType(), f.GetOperand(), v)
		}
	} else if typeOptions.GetCategory() == stat_filter.FilterCategory_STRING_FILTER_CATEGORY {
		if operandOptions.GetArgumentCount() == stat_filter.FilterArgumentCount_ONE_FILTER_ARGUMENT_COUNT && len(v.GetStringValue()) == 1 {
			arg = v.GetStringValue()[0]
		} else if operandOptions.GetArgumentCount() == stat_filter.FilterArgumentCount_MANY_FILTER_ARGUMENT_COUNT && len(v.GetStringValue()) > 0 {
			arg = v.GetStringValue()
		} else {
			return "", nil, status.InvalidArgumentErrorf("Invalid value for string filter: %s %s Value: %+v", f.GetType(), f.GetOperand(), v)
		}
	} else {
		return "", nil, status.InternalErrorf("Unknown filter category: %s", typeOptions.GetCategory())
	}

	qStr, qArgs := getStringAndArgs(operandOptions.GetDatabaseQueryString(), arg, typeOptions.GetDatabaseColumnName(), f.GetNegate())
	return qStr, qArgs, nil
}
