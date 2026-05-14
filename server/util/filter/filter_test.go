package filter_test

import (
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	"github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestValidGenericFilters(t *testing.T) {
	cases := []struct {
		filter        *stat_filter.GenericFilter
		filterType    stat_filter.ObjectTypes
		expectedQStr  string
		expectedQArgs []interface{}
	}{
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_DURATION_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{10000},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "duration_usec > ?",
			expectedQArgs: []interface{}{int64(10000)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_REPO_URL_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_IN_OPERAND,
				Value: &stat_filter.FilterValue{
					StringValue: []string{"http://github.com/buildbuddy-io/buildbuddy"},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "repo_url IN ?",
			expectedQArgs: []interface{}{[]string{"http://github.com/buildbuddy-io/buildbuddy"}},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_USER_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_IN_OPERAND,
				Value: &stat_filter.FilterValue{
					StringValue: []string{"siggisim", "tylerw"},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "\"user\" IN ?",
			expectedQArgs: []interface{}{[]string{"siggisim", "tylerw"}},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_USER_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_STRING_CONTAINS_OPERAND,
				Value: &stat_filter.FilterValue{
					StringValue: []string{"sigg"},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "INSTR(\"user\", ?) > 0",
			expectedQArgs: []interface{}{"sigg"},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_EXECUTION_CREATED_AT_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_LESS_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{10001},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "created_at_usec < ?",
			expectedQArgs: []interface{}{int64(10001)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_STATUS_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_IN_OPERAND,
				Value: &stat_filter.FilterValue{
					StatusValue: []invocation_status.OverallStatus{invocation_status.OverallStatus_SUCCESS, invocation_status.OverallStatus_FAILURE},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  " (invocation_status = ? AND success = ?) OR (invocation_status = ? AND success = ?) ",
			expectedQArgs: []interface{}{1, 1, 1, 0},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_STATUS_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_IN_OPERAND,
				Value: &stat_filter.FilterValue{
					StatusValue: []invocation_status.OverallStatus{invocation_status.OverallStatus_IN_PROGRESS, invocation_status.OverallStatus_DISCONNECTED},
				},
				Negate: true,
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "NOT( invocation_status = ? OR invocation_status = ? )",
			expectedQArgs: []interface{}{2, 3},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_CAS_CACHE_MISSES_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{0},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "cas_cache_misses > ?",
			expectedQArgs: []interface{}{int64(0)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_ACTION_CACHE_MISSES_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{0},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "action_cache_misses > ?",
			expectedQArgs: []interface{}{int64(0)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_CAS_CACHE_DOWNLOAD_BYTES_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_LESS_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{2001},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "total_download_size_bytes < ?",
			expectedQArgs: []interface{}{int64(2001)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_CAS_CACHE_DOWNLOAD_BPS_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{10_000},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "download_throughput_bytes_per_second > ?",
			expectedQArgs: []interface{}{int64(10_000)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_CAS_CACHE_UPLOAD_BYTES_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_LESS_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{2001},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "total_upload_size_bytes < ?",
			expectedQArgs: []interface{}{int64(2001)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_CAS_CACHE_UPLOAD_BPS_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{10_000},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "upload_throughput_bytes_per_second > ?",
			expectedQArgs: []interface{}{int64(10_000)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_TIME_SAVED_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{456},
				},
			},
			filterType:    stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			expectedQStr:  "total_cached_action_exec_usec > ?",
			expectedQArgs: []interface{}{int64(456)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_EXECUTION_QUEUE_TIME_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_LESS_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{500},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "IF(worker_start_timestamp_usec < queued_timestamp_usec, 0, (worker_start_timestamp_usec - queued_timestamp_usec)) < ?",
			expectedQArgs: []interface{}{int64(500)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_EXECUTION_INPUT_DOWNLOAD_TIME_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_LESS_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{1000},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "(input_fetch_completed_timestamp_usec - input_fetch_start_timestamp_usec) < ?",
			expectedQArgs: []interface{}{int64(1000)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_EXECUTION_REAL_TIME_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_LESS_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{9090},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "(execution_completed_timestamp_usec - execution_start_timestamp_usec) < ?",
			expectedQArgs: []interface{}{int64(9090)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_OUTPUT_UPLOAD_TIME_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{100},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "(output_upload_completed_timestamp_usec - output_upload_start_timestamp_usec) > ?",
			expectedQArgs: []interface{}{int64(100)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_PEAK_MEMORY_BYTES_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{250},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "peak_memory_bytes > ?",
			expectedQArgs: []interface{}{int64(250)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INPUT_DOWNLOAD_SIZE_BYTES_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{400},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "file_download_size_bytes > ?",
			expectedQArgs: []interface{}{int64(400)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_OUTPUT_UPLOAD_SIZE_BYTES_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{500},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "file_upload_size_bytes > ?",
			expectedQArgs: []interface{}{int64(500)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_EXECUTION_WALL_TIME_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{7500},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "IF(worker_completed_timestamp_usec < queued_timestamp_usec, 0, (worker_completed_timestamp_usec - queued_timestamp_usec)) > ?",
			expectedQArgs: []interface{}{int64(7500)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_EXECUTION_CPU_NANOS_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{10_000},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "cpu_nanos > ?",
			expectedQArgs: []interface{}{int64(10_000)},
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_EXECUTION_AVERAGE_MILLICORES_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{4000},
				},
			},
			filterType:    stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			expectedQStr:  "IF(cpu_nanos <= 0 OR (execution_completed_timestamp_usec - execution_start_timestamp_usec) <= 0, 0, intDivOrZero(cpu_nanos*1000, (execution_completed_timestamp_usec - execution_start_timestamp_usec) * 1000)) > ?",
			expectedQArgs: []interface{}{int64(4000)},
		},
	}
	for _, tc := range cases {
		qStr, qArgs, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(tc.filter, tc.filterType, "clickhouse")
		assert.Nil(t, err)
		assert.Equal(t, tc.expectedQStr, qStr)
		assert.ElementsMatch(t, tc.expectedQArgs, qArgs)
		qStr, qArgs, err = filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(tc.filter, tc.filterType, "mysql")
		assert.Nil(t, err)
		assert.Equal(t, tc.expectedQStr, qStr)
		assert.ElementsMatch(t, tc.expectedQArgs, qArgs)
	}
}

// Tags are annoying to template because they behave different for different
// dialects, so they get their own test case.
func TestTagGenericFilters(t *testing.T) {
	f := &stat_filter.GenericFilter{
		Type:    stat_filter.FilterType_TAG_FILTER_TYPE,
		Operand: stat_filter.FilterOperand_ARRAY_CONTAINS_OPERAND,
		Value: &stat_filter.FilterValue{
			StringValue: []string{"tag_one", "tag_two"},
		}}
	qStr, qArgs, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(f, stat_filter.ObjectTypes_INVOCATION_OBJECTS, "clickhouse")
	assert.Nil(t, err)
	assert.Equal(t, "hasAny(tags, array(?))", qStr)
	assert.ElementsMatch(t, []interface{}{[]string{"tag_one", "tag_two"}}, qArgs)
	qStr, qArgs, err = filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(f, stat_filter.ObjectTypes_INVOCATION_OBJECTS, "mysql")
	assert.Nil(t, err)
	assert.Equal(t, " INSTR(tags, ?) OR INSTR(tags, ?) ", qStr)
	assert.ElementsMatch(t, []interface{}{"tag_one", "tag_two"}, qArgs)
}

func TestInvalidGenericFilters(t *testing.T) {
	cases := []struct {
		filter           *stat_filter.GenericFilter
		filterType       stat_filter.ObjectTypes
		errorTypeFn      func(error) bool
		errorExplanation string
	}{
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_DURATION_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					StringValue: []string{"duration_usec shouldn't accept a string"},
				},
			},
			filterType:       stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			errorTypeFn:      status.IsInvalidArgumentError,
			errorExplanation: "duration_usec shouldn't accept a string",
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_EXECUTION_CREATED_AT_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_LESS_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{10001},
				},
			},
			filterType:       stat_filter.ObjectTypes_INVOCATION_OBJECTS,
			errorTypeFn:      status.IsInvalidArgumentError,
			errorExplanation: "Shouldn't be able to filter execution creation time on invocations.",
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_STATUS_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_IN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{0, 1},
				},
			},
			filterType:       stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			errorTypeFn:      status.IsInvalidArgumentError,
			errorExplanation: "Shouldn't be able to filter status with a number.",
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_STATUS_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					StatusValue: []invocation_status.OverallStatus{invocation_status.OverallStatus_SUCCESS},
				},
			},
			filterType:       stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			errorTypeFn:      status.IsInvalidArgumentError,
			errorExplanation: "Shouldn't be able to filter status with something other than IN.",
		},
		{
			filter: &stat_filter.GenericFilter{
				Type:    stat_filter.FilterType_INVOCATION_STATUS_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_IN_OPERAND,
				Value: &stat_filter.FilterValue{
					StringValue: []string{"success", "failure"},
				},
			},
			filterType:       stat_filter.ObjectTypes_EXECUTION_OBJECTS,
			errorTypeFn:      status.IsInvalidArgumentError,
			errorExplanation: "Shouldn't be able to filter status with a string.",
		},
	}
	for _, tc := range cases {
		_, _, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(tc.filter, tc.filterType, "clickhouse")
		assert.True(t, tc.errorTypeFn(err), tc.errorExplanation)
		_, _, err = filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(tc.filter, tc.filterType, "mysql")
		assert.True(t, tc.errorTypeFn(err), tc.errorExplanation)
	}
}

var customColumnFilterTypes = []stat_filter.FilterType{
	stat_filter.FilterType_UNKNOWN_FILTER_TYPE,
	stat_filter.FilterType_TEXT_MATCH_FILTER_TYPE,
	stat_filter.FilterType_INVOCATION_STATUS_FILTER_TYPE,
}

func TestAllFilterTypesHaveRequiredOptions(t *testing.T) {
	// API is a little weird here--need to specify a single filter type to get
	// the enum descriptor, which has an iterator over all values.  Whatever.
	descriptors := stat_filter.FilterType.Descriptor(stat_filter.FilterType_PATTERN_FILTER_TYPE).Values()
	for i := range descriptors.Len() {
		fto := proto.GetExtension(descriptors.Get(i).Options(), stat_filter.E_FilterTypeOptions).(*stat_filter.FilterTypeOptions)

		if len(fto.GetSupportedObjects()) == 0 {
			// Don't need to worry about filter types that don't support any objects.
			continue
		}

		if !slices.Contains(customColumnFilterTypes, stat_filter.FilterType(descriptors.Get(i).Number())) {
			assert.NotEmpty(t, fto.GetDatabaseColumnName())
		}
		assert.False(t, slices.Contains(fto.GetSupportedObjects(), stat_filter.ObjectTypes_UNKNOWN_OBJECTS))
		assert.True(t, fto.GetCategory().Enum().Number() > 0)
	}
}

func TestAllFilterOperandsHaveRequiredOptions(t *testing.T) {
	// API is a little weird here--need to specify a single operand to get
	// the enum descriptor, which has an iterator over all values.  Whatever.
	descriptors := stat_filter.FilterOperand.Descriptor(stat_filter.FilterOperand_GREATER_THAN_OPERAND).Values()
	for i := range descriptors.Len() {
		foo := proto.GetExtension(descriptors.Get(i).Options(), stat_filter.E_FilterOperandOptions).(*stat_filter.FilterOperandOptions)
		if len(foo.GetSupportedCategories()) == 0 {
			// Won't be applied to anything anyway, skip.
			continue
		}

		assert.NotEmpty(t, foo.GetDatabaseQueryString())
		assert.True(t, foo.GetArgumentCount().Enum().Number() > 0)
		for _, c := range foo.GetSupportedCategories() {
			assert.True(t, c.Enum().Number() > 0)
		}
	}
}
