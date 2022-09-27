package common_test

import (
	"encoding/json"
	"flag"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags"
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
	err := common.SetValueForFlagName("bool", true, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, true, *flagBool)

	flags = replaceFlagsForTesting(t)
	flagBool = flags.Bool("bool", false, "")
	err = common.SetValueForFlagName("bool", true, map[string]struct{}{"bool": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, false, *flagBool)

	flags = replaceFlagsForTesting(t)
	flagInt := flags.Int("int", 2, "")
	err = common.SetValueForFlagName("int", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, 1, *flagInt)

	flags = replaceFlagsForTesting(t)
	flagInt = flags.Int("int", 2, "")
	err = common.SetValueForFlagName("int", 1, map[string]struct{}{"int": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, 2, *flagInt)

	flags = replaceFlagsForTesting(t)
	flagInt64 := flags.Int64("int64", 2, "")
	err = common.SetValueForFlagName("int64", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, int64(1), *flagInt64)

	flags = replaceFlagsForTesting(t)
	flagInt64 = flags.Int64("int64", 2, "")
	err = common.SetValueForFlagName("int64", 1, map[string]struct{}{"int64": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, int64(2), *flagInt64)

	flags = replaceFlagsForTesting(t)
	flagUint := flags.Uint("uint", 2, "")
	err = common.SetValueForFlagName("uint", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, uint(1), *flagUint)

	flags = replaceFlagsForTesting(t)
	flagUint = flags.Uint("uint", 2, "")
	err = common.SetValueForFlagName("uint", 1, map[string]struct{}{"uint": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, uint(2), *flagUint)

	flags = replaceFlagsForTesting(t)
	flagUint64 := flags.Uint64("uint64", 2, "")
	err = common.SetValueForFlagName("uint64", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), *flagUint64)

	flags = replaceFlagsForTesting(t)
	flagUint64 = flags.Uint64("uint64", 2, "")
	err = common.SetValueForFlagName("uint64", 1, map[string]struct{}{"uint64": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), *flagUint64)

	flags = replaceFlagsForTesting(t)
	flagFloat64 := flags.Float64("float64", 2, "")
	err = common.SetValueForFlagName("float64", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, float64(1), *flagFloat64)

	flags = replaceFlagsForTesting(t)
	flagFloat64 = flags.Float64("float64", 2, "")
	err = common.SetValueForFlagName("float64", 1, map[string]struct{}{"float64": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, float64(2), *flagFloat64)

	flags = replaceFlagsForTesting(t)
	flagString := flags.String("string", "2", "")
	err = common.SetValueForFlagName("string", "1", map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, "1", *flagString)

	flags = replaceFlagsForTesting(t)
	flagString = flags.String("string", "2", "")
	err = common.SetValueForFlagName("string", "1", map[string]struct{}{"string": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, "2", *flagString)

	flags = replaceFlagsForTesting(t)
	flagURL := flagtypes.URLFromString("url", "https://www.example.com", "")
	u, err := url.Parse("https://www.example.com:8080")
	require.NoError(t, err)
	err = common.SetValueForFlagName("url", *u, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, url.URL{Scheme: "https", Host: "www.example.com:8080"}, *flagURL)

	flags = replaceFlagsForTesting(t)
	flagURL = flagtypes.URLFromString("url", "https://www.example.com", "")
	u, err = url.Parse("https://www.example.com:8080")
	require.NoError(t, err)
	err = common.SetValueForFlagName("url", *u, map[string]struct{}{"url": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, url.URL{Scheme: "https", Host: "www.example.com"}, *flagURL)

	flags = replaceFlagsForTesting(t)
	stringSlice := make([]string, 2)
	stringSlice[0] = "1"
	stringSlice[1] = "2"
	flagtypes.StringSliceVar(&stringSlice, "string_slice", stringSlice, "")
	err = common.SetValueForFlagName("string_slice", []string{"3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, stringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice := flagtypes.StringSlice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{"string_slice": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, *(*[]string)(flags.Lookup("string_slice").Value.(*flagtypes.StringSliceFlag)))
	assert.Equal(t, []string{"1", "2", "3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = flagtypes.StringSlice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = flagtypes.StringSlice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{"string_slice": {}}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice := []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.JSONSliceVar(&flagStructSlice, "struct_slice", flagStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}, {Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.JSONSliceVar(&flagStructSlice, "struct_slice", flagStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{"struct_slice": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}, {Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.JSONSliceVar(&flagStructSlice, "struct_slice", flagStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{}, false)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.JSONSliceVar(&flagStructSlice, "struct_slice", flagStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{"struct_slice": {}}, false)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, flagStructSlice)
}

func TestBadSetValueForFlagName(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	_ = flags.Bool("bool", false, "")
	err := common.SetValueForFlagName("bool", 0, map[string]struct{}{}, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	err = common.SetValueForFlagName("bool", false, map[string]struct{}{}, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	_ = flagtypes.StringSlice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", "3", map[string]struct{}{}, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	_ = flagtypes.StringSlice("string_slice", []string{"1", "2"}, "")
	err = common.SetValueForFlagName("string_slice", "3", map[string]struct{}{}, false)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	flagStructSlice := []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.JSONSliceVar(&flagStructSlice, "struct_slice", flagStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", testStruct{Field: 3}, map[string]struct{}{}, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	flagtypes.JSONSliceVar(&flagStructSlice, "struct_slice", flagStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", testStruct{Field: 3}, map[string]struct{}{}, false)
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
	_ = flagtypes.StringSlice("string_slice", []string{"1", "2"}, "")
	stringSlice, err := common.GetDereferencedValue[[]string]("string_slice")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, stringSlice)

	flags = replaceFlagsForTesting(t)
	structSlice := flagtypes.JSONSlice("struct_slice", []testStruct{{Field: 1}, {Field: 2}}, "")
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, *structSlice)
}

func TestBadDereferencedValueFromFlagName(t *testing.T) {
	_ = replaceFlagsForTesting(t)
	_, err := common.GetDereferencedValue[any]("unknown_flag")
	require.Error(t, err)
}

func SetWithOverride(t *testing.T) {
	_ = replaceFlagsForTesting(t)

	defaultBool := false
	defaultInt := 37
	defaultEmptyStringSlice := []string{}
	defaultStringSlice := []string{"ert+", "y76p", "olu8"}
	defaultEmptyStructSlice := []testStruct{}
	defaultStructSlice := []testStruct{{Field: 0, Meadow: "ert+"}, {Field: 1, Meadow: "y76p"}, {Field: 2, Meadow: "olu8"}}

	boolFromFlag := autoflags.New("bool_flag", defaultBool, "")
	intFromFlag := autoflags.New("int_flag", defaultInt, "")
	emptyStringSliceFromFlag := autoflags.New("empty_string_slice_flag", defaultEmptyStringSlice, "")
	stringSliceFromFlag := autoflags.New("string_slice_flag", defaultStringSlice, "")
	emptyStructSliceFromFlag := autoflags.New("empty_struct_slice_flag", defaultEmptyStructSlice, "")
	structSliceFromFlag := autoflags.New("struct_slice_flag", defaultStructSlice, "")

	common.SetWithOverride("bool_flag", "true")
	assert.Equal(t, true, *boolFromFlag)

	common.SetWithOverride("int_flag", "20")
	assert.Equal(t, 20, *intFromFlag)

	common.SetWithOverride("empty_string_slice_flag", "yo,what up,hey")
	assert.Equal(t, []string{"yo", "what up", "hey"}, *emptyStringSliceFromFlag)

	common.SetWithOverride("string_slice_flag", "flour,cornmeal,baking powder,sugar")
	assert.Equal(t, []string{"flour", "corn meal", "baking powder", "sugar"}, *stringSliceFromFlag)

	newEmptyStructSliceFlag := []testStruct{{Field: 5, Meadow: "halibut"}, {Field: 9, Meadow: "sheep's cheese"}, {Field: 11, Meadow: "tamahtoes"}}
	jsonForEmptyStructSliceFlag, err := json.Marshal(newEmptyStructSliceFlag)
	require.NoError(t, err)
	common.SetWithOverride("empty_struct_slice_flag", string(jsonForEmptyStructSliceFlag))
	assert.Equal(t, newEmptyStructSliceFlag, *emptyStructSliceFromFlag)

	newStructSliceFlag := []testStruct{{Field: 207, Meadow: "PURPLE PEOPLE"}}
	jsonForStructSliceFlag, err := json.Marshal(newStructSliceFlag)
	require.NoError(t, err)
	common.SetWithOverride("struct_slice_flag", string(jsonForStructSliceFlag))
	assert.Equal(t, newStructSliceFlag, *structSliceFromFlag)
}

func TestResetFlags(t *testing.T) {
	_ = replaceFlagsForTesting(t)

	defaultBool := false
	defaultInt := 37
	defaultEmptyStringSlice := []string{}
	defaultStringSlice := []string{"ert+", "y76p", "olu8"}
	defaultEmptyStructSlice := []testStruct{}
	defaultStructSlice := []testStruct{{Field: 0, Meadow: "ert+"}, {Field: 1, Meadow: "y76p"}, {Field: 2, Meadow: "olu8"}}

	boolFromFlag := autoflags.New("bool_flag", defaultBool, "")
	intFromFlag := autoflags.New("int_flag", defaultInt, "")
	emptyStringSliceFromFlag := autoflags.New("empty_string_slice_flag", defaultEmptyStringSlice, "")
	stringSliceFromFlag := autoflags.New("string_slice_flag", defaultStringSlice, "")
	emptyStructSliceFromFlag := autoflags.New("empty_struct_slice_flag", defaultEmptyStructSlice, "")
	structSliceFromFlag := autoflags.New("struct_slice_flag", defaultStructSlice, "")

	*boolFromFlag = true
	*intFromFlag = 20
	*emptyStringSliceFromFlag = append(*emptyStringSliceFromFlag, "hi")
	*stringSliceFromFlag = append(*stringSliceFromFlag, "hi")
	*emptyStructSliceFromFlag = append(*emptyStructSliceFromFlag, testStruct{Field: 3, Meadow: "hi"})
	*structSliceFromFlag = append(*structSliceFromFlag, testStruct{Field: 3, Meadow: "hi"})

	err := common.ResetFlags()
	require.NoError(t, err)

	assert.Equal(t, defaultBool, *boolFromFlag)
	assert.Equal(t, defaultInt, *intFromFlag)
	assert.Equal(t, 0, len(*emptyStringSliceFromFlag))
	assert.Equal(t, defaultStringSlice, *stringSliceFromFlag)
	assert.Equal(t, 0, len(*emptyStructSliceFromFlag))
	assert.Equal(t, defaultStructSlice, *structSliceFromFlag)
}
