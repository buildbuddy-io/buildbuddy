package common_test

import (
	"flag"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flagtypes "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types"
)

type unsupportedFlagValue struct{}

func (f *unsupportedFlagValue) Set(string) error { return nil }
func (f *unsupportedFlagValue) String() string   { return "" }

type testStruct struct {
	Field  int    `json:"field"`
	Meadow string `json:"meadow"`
}

func replaceFlagsForTesting(t *testing.T) *flag.FlagSet {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	common.DefaultFlagSet = flags

	t.Cleanup(func() {
		common.DefaultFlagSet = flag.CommandLine
	})

	return flags
}

func TestSetValueForFlagName(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	flagBool := flags.Bool("bool", false, "")
	err := common.SetValueForFlagName("bool", true, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, true, *flagBool)

	flags = replaceFlagsForTesting(t)
	flagBool = flags.Bool("bool", false, "")
	err = common.SetValueForFlagName("bool", true, map[string]struct{}{"bool": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, false, *flagBool)

	flags = replaceFlagsForTesting(t)
	err = common.SetValueForFlagName("bool", true, map[string]struct{}{}, true, false, false)
	require.NoError(t, err)

	flags = replaceFlagsForTesting(t)
	flagInt := flags.Int("int", 2, "")
	err = common.SetValueForFlagName("int", 1, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, 1, *flagInt)

	flags = replaceFlagsForTesting(t)
	flagInt = flags.Int("int", 2, "")
	err = common.SetValueForFlagName("int", 1, map[string]struct{}{"int": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, 2, *flagInt)

	flags = replaceFlagsForTesting(t)
	flagInt64 := flags.Int64("int64", 2, "")
	err = common.SetValueForFlagName("int64", 1, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, int64(1), *flagInt64)

	flags = replaceFlagsForTesting(t)
	flagInt64 = flags.Int64("int64", 2, "")
	err = common.SetValueForFlagName("int64", 1, map[string]struct{}{"int64": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, int64(2), *flagInt64)

	flags = replaceFlagsForTesting(t)
	flagUint := flags.Uint("uint", 2, "")
	err = common.SetValueForFlagName("uint", 1, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, uint(1), *flagUint)

	flags = replaceFlagsForTesting(t)
	flagUint = flags.Uint("uint", 2, "")
	err = common.SetValueForFlagName("uint", 1, map[string]struct{}{"uint": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, uint(2), *flagUint)

	flags = replaceFlagsForTesting(t)
	flagUint64 := flags.Uint64("uint64", 2, "")
	err = common.SetValueForFlagName("uint64", 1, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), *flagUint64)

	flags = replaceFlagsForTesting(t)
	flagUint64 = flags.Uint64("uint64", 2, "")
	err = common.SetValueForFlagName("uint64", 1, map[string]struct{}{"uint64": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), *flagUint64)

	flags = replaceFlagsForTesting(t)
	flagFloat64 := flags.Float64("float64", 2, "")
	err = common.SetValueForFlagName("float64", 1, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, float64(1), *flagFloat64)

	flags = replaceFlagsForTesting(t)
	flagFloat64 = flags.Float64("float64", 2, "")
	err = common.SetValueForFlagName("float64", 1, map[string]struct{}{"float64": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, float64(2), *flagFloat64)

	flags = replaceFlagsForTesting(t)
	flagString := flags.String("string", "2", "")
	err = common.SetValueForFlagName("string", "1", map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, "1", *flagString)

	flags = replaceFlagsForTesting(t)
	flagString = flags.String("string", "2", "")
	err = common.SetValueForFlagName("string", "1", map[string]struct{}{"string": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, "2", *flagString)

	flags = replaceFlagsForTesting(t)
	flagURL := flagtypes.URLFromString("url", "https://www.example.com", "")
	u, err := url.Parse("https://www.example.com:8080")
	require.NoError(t, err)
	err = common.SetValueForFlagName("url", *u, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, url.URL{Scheme: "https", Host: "www.example.com:8080"}, *flagURL)

	flags = replaceFlagsForTesting(t)
	flagURL = flagtypes.URLFromString("url", "https://www.example.com", "")
	u, err = url.Parse("https://www.example.com:8080")
	require.NoError(t, err)
	err = common.SetValueForFlagName("url", *u, map[string]struct{}{"url": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, url.URL{Scheme: "https", Host: "www.example.com"}, *flagURL)

	flags = replaceFlagsForTesting(t)
	string_slice := make([]string, 2)
	string_slice[0] = "1"
	string_slice[1] = "2"
	flagtypes.SliceVar(&string_slice, "string_slice", "")
	err = common.SetValueForFlagName("string_slice", []string{"3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, string_slice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice := flagtypes.Slice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{"string_slice": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = flagtypes.Slice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{}, false, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = flagtypes.Slice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{"string_slice": {}}, false, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice := []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.SliceVar(&flagStructSlice, "struct_slice", "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}, {Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.SliceVar(&flagStructSlice, "struct_slice", "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{"struct_slice": {}}, true, false, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}, {Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.SliceVar(&flagStructSlice, "struct_slice", "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{}, false, false, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.SliceVar(&flagStructSlice, "struct_slice", "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{"struct_slice": {}}, false, false, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, flagStructSlice)
}

func TestBadSetValueForFlagName(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	_ = flags.Bool("bool", false, "")
	err := common.SetValueForFlagName("bool", 0, map[string]struct{}{}, true, false, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	err = common.SetValueForFlagName("bool", false, map[string]struct{}{}, true, false, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	_ = flagtypes.Slice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", "3", map[string]struct{}{}, true, false, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	_ = flagtypes.Slice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", "3", map[string]struct{}{}, false, false, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	flagStructSlice := []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.SliceVar(&flagStructSlice, "struct_slice", "")
	err = common.SetValueForFlagName("struct_slice", testStruct{Field: 3}, map[string]struct{}{}, true, false, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.SliceVar(&flagStructSlice, "struct_slice", "")
	err = common.SetValueForFlagName("struct_slice", testStruct{Field: 3}, map[string]struct{}{}, false, false, true)
	require.Error(t, err)
}

func TestDereferencedValueFromFlagName(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	_ = flags.Bool("bool", false, "")
	v, err := common.GetDereferencedValue[bool]("bool")
	require.NoError(t, err)
	assert.Equal(t, false, v)

	flags = replaceFlagsForTesting(t)
	_ = flags.Bool("bool", true, "")
	v, err = common.GetDereferencedValue[bool]("bool")
	require.NoError(t, err)
	assert.Equal(t, true, v)

	flags = replaceFlagsForTesting(t)
	_ = flagtypes.Slice("string_slice", []string{"1", "2"}, "")
	stringSlice, err := common.GetDereferencedValue[[]string]("string_slice")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, stringSlice)

	flags = replaceFlagsForTesting(t)
	flagtypes.SliceVar(&[]testStruct{{Field: 1}, {Field: 2}}, "struct_slice", "")
	structSlice, err := common.GetDereferencedValue[[]testStruct]("struct_slice")
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, structSlice)
}

func TestBadDereferencedValueFromFlagName(t *testing.T) {
	_ = replaceFlagsForTesting(t)
	_, err := common.GetDereferencedValue[any]("unknown_flag")
	require.Error(t, err)
}
