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
		prefix        string
		filterType    stat_filter.SupportedObjects       
		expectedQStr  string
		expectedQArgs []interface{}
	}{
		{
			filter:       &stat_filter.GenericFilter{
				Type: stat_filter.FilterType_INVOCATION_DURATION_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					IntValue: []int64{10000},
				},
			},
			prefix: "i.",
			filterType: stat_filter.SupportedObjects_INVOCATIONS_SUPPORTED,
			expectedQStr: "i.duration_usec > ?",
			expectedQArgs: []interface{}{int64(10000)},
		},
	}
	for _, tc := range cases {
		qStr, qArgs, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(tc.filter, tc.prefix, tc.filterType)
		assert.Nil(t, err)
		assert.Equal(t, tc.expectedQStr, qStr)
		assert.ElementsMatch(t, tc.expectedQArgs, qArgs)
	}
}

func TestInvalidGenericFilters(t *testing.T) {
	cases := []struct {
		filter        *stat_filter.GenericFilter
		prefix        string
		filterType    stat_filter.SupportedObjects
		errorTypeFn   func (error) bool
	}{
		{
			filter:       &stat_filter.GenericFilter{
				Type: stat_filter.FilterType_INVOCATION_DURATION_USEC_FILTER_TYPE,
				Operand: stat_filter.FilterOperand_GREATER_THAN_OPERAND,
				Value: &stat_filter.FilterValue{
					StringValue: []string{"duration_usec shouldn't accept a string"},
				},
			},
			prefix: "",
			filterType: stat_filter.SupportedObjects_INVOCATIONS_SUPPORTED,
			errorTypeFn: status.IsInvalidArgumentError,
		},
	}
	for _, tc := range cases {
		_, _, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(tc.filter, tc.prefix, tc.filterType)
		assert.True(t, tc.errorTypeFn(err))
	}
}

func TestAllFilterTypesHaveRequiredOptions(t *testing.T) {
	// API is a little weird here--need to specify a single filter type to get
	// the enum descriptor, which has an iterator over all values.  Whatever.
	descriptors := stat_filter.FilterType.Descriptor(stat_filter.FilterType_PATTERN_FILTER_TYPE).Values()
	for i := range descriptors.Len() {
		fto := proto.GetExtension(descriptors.Get(i).Options(), stat_filter.E_FilterTypeOptions).(*stat_filter.FilterTypeOptions)

		// Fully de-supported / unknown options: all that matters is that we'll always throw an error.
		if slices.Contains(fto.GetSupportedObjects(), stat_filter.SupportedObjects_NO_SUPPORT) {
			assert.Equal(t, 1, len(fto.GetSupportedObjects()))
			continue;
		}

		if descriptors.Get(i).Number() != stat_filter.FilterType_UNKNOWN_FILTER_TYPE.Number() &&
		   descriptors.Get(i).Number() != stat_filter.FilterType_TEXT_MATCH_FILTER_TYPE.Number() {
			assert.NotEmpty(t, fto.GetDatabaseColumnName())
		}
		assert.NotEmpty(t, fto.GetSupportedObjects())
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
		for _, c := range(foo.GetSupportedCategories()) {
			assert.True(t, c.Enum().Number() > 0)
		}
	}
}
