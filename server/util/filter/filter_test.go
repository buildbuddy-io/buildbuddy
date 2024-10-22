package filter_test

import (
	"slices"
	"testing"

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
			expectedQStr:  "user IN ?",
			expectedQArgs: []interface{}{[]string{"siggisim", "tylerw"}},
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
	}
	for _, tc := range cases {
		qStr, qArgs, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(tc.filter, tc.filterType)
		assert.Nil(t, err)
		assert.Equal(t, tc.expectedQStr, qStr)
		assert.ElementsMatch(t, tc.expectedQArgs, qArgs)
	}
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
	}
	for _, tc := range cases {
		_, _, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(tc.filter, tc.filterType)
		assert.True(t, tc.errorTypeFn(err), tc.errorExplanation)
	}
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

		if descriptors.Get(i).Number() != stat_filter.FilterType_UNKNOWN_FILTER_TYPE.Number() &&
			descriptors.Get(i).Number() != stat_filter.FilterType_TEXT_MATCH_FILTER_TYPE.Number() {
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
