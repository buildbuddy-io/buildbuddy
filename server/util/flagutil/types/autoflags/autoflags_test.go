package autoflags

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

func TestNew(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	flagBool := New("bool", false, "")
	err := common.SetValueForFlagName("bool", true, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, true, *flagBool)

	flags = replaceFlagsForTesting(t)
	flagBool = New("bool", false, "")
	err = common.SetValueForFlagName("bool", true, map[string]struct{}{"bool": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, false, *flagBool)

	flags = replaceFlagsForTesting(t)
	flagInt := New("int", 2, "")
	err = common.SetValueForFlagName("int", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, 1, *flagInt)

	flags = replaceFlagsForTesting(t)
	flagInt = New("int", 2, "")
	err = common.SetValueForFlagName("int", 1, map[string]struct{}{"int": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, 2, *flagInt)

	flags = replaceFlagsForTesting(t)
	flagInt64 := New("int64", int64(2), "")
	err = common.SetValueForFlagName("int64", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, int64(1), *flagInt64)

	flags = replaceFlagsForTesting(t)
	flagInt64 = New("int64", int64(2), "")
	err = common.SetValueForFlagName("int64", 1, map[string]struct{}{"int64": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, int64(2), *flagInt64)

	flags = replaceFlagsForTesting(t)
	flagUint := New("uint", uint(2), "")
	err = common.SetValueForFlagName("uint", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, uint(1), *flagUint)

	flags = replaceFlagsForTesting(t)
	flagUint = New("uint", uint(2), "")
	err = common.SetValueForFlagName("uint", 1, map[string]struct{}{"uint": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, uint(2), *flagUint)

	flags = replaceFlagsForTesting(t)
	flagUint64 := New("uint64", uint64(2), "")
	err = common.SetValueForFlagName("uint64", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), *flagUint64)

	flags = replaceFlagsForTesting(t)
	flagUint64 = New("uint64", uint64(2), "")
	err = common.SetValueForFlagName("uint64", 1, map[string]struct{}{"uint64": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), *flagUint64)

	flags = replaceFlagsForTesting(t)
	flagFloat64 := New("float64", float64(2), "")
	err = common.SetValueForFlagName("float64", 1, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, float64(1), *flagFloat64)

	flags = replaceFlagsForTesting(t)
	flagFloat64 = New("float64", float64(2), "")
	err = common.SetValueForFlagName("float64", 1, map[string]struct{}{"float64": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, float64(2), *flagFloat64)

	flags = replaceFlagsForTesting(t)
	flagString := New("string", "2", "")
	err = common.SetValueForFlagName("string", "1", map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, "1", *flagString)

	flags = replaceFlagsForTesting(t)
	flagString = New("string", "2", "")
	err = common.SetValueForFlagName("string", "1", map[string]struct{}{"string": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, "2", *flagString)

	flags = replaceFlagsForTesting(t)
	defaultURL := url.URL{Scheme: "https", Host: "www.example.com"}
	flagURL := New("url", defaultURL, "")
	u, err := url.Parse("https://www.example.com:8080")
	require.NoError(t, err)
	err = common.SetValueForFlagName("url", *u, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, url.URL{Scheme: "https", Host: "www.example.com"}, defaultURL)
	assert.Equal(t, url.URL{Scheme: "https", Host: "www.example.com:8080"}, *flagURL)

	flags = replaceFlagsForTesting(t)
	flagURL = New("url", url.URL{Scheme: "https", Host: "www.example.com"}, "")
	u, err = url.Parse("https://www.example.com:8080")
	require.NoError(t, err)
	err = common.SetValueForFlagName("url", *u, map[string]struct{}{"url": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, url.URL{Scheme: "https", Host: "www.example.com"}, *flagURL)

	flags = replaceFlagsForTesting(t)
	defaultStringSlice := []string{"1", "2"}
	stringSlice := New("string_slice", defaultStringSlice, "")
	err = common.SetValueForFlagName("string_slice", []string{"3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, defaultStringSlice)
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, *stringSlice)

	flags = replaceFlagsForTesting(t)
	stringSlice = New("string_slice", defaultStringSlice, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{"string_slice": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, defaultStringSlice)
	assert.Equal(t, []string{"1", "2", "3"}, *(*[]string)(flags.Lookup("string_slice").Value.(*flagtypes.StringSliceFlag)))
	assert.Equal(t, []string{"1", "2", "3"}, *stringSlice)

	flags = replaceFlagsForTesting(t)
	stringSlice = New("string_slice", defaultStringSlice, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, defaultStringSlice)
	assert.Equal(t, []string{"3"}, *stringSlice)

	flags = replaceFlagsForTesting(t)
	stringSlice = New("string_slice", defaultStringSlice, "")
	err = common.SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{"string_slice": {}}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, defaultStringSlice)
	assert.Equal(t, []string{"1", "2"}, *stringSlice)

	flags = replaceFlagsForTesting(t)
	defaultStructSlice := []testStruct{{Field: 1}, {Field: 2}}
	structSlice := New("struct_slice", defaultStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}, {Field: 3}}, *structSlice)

	flags = replaceFlagsForTesting(t)
	structSlice = New("struct_slice", defaultStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{"struct_slice": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, defaultStructSlice)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}, {Field: 3}}, *structSlice)

	flags = replaceFlagsForTesting(t)
	structSlice = New("struct_slice", defaultStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{}, false)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3}}, *structSlice)

	flags = replaceFlagsForTesting(t)
	structSlice = New("struct_slice", defaultStructSlice, "")
	err = common.SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{"struct_slice": {}}, false)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, *structSlice)
}
