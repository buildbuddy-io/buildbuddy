package flagutil

import (
	"flag"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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
	defaultFlagSet = flags

	t.Cleanup(func() {
		defaultFlagSet = flag.CommandLine
	})

	return flags
}

func TestStringSliceFlag(t *testing.T) {
	var err error

	flags := replaceFlagsForTesting(t)

	foo := StringSlice("foo", []string{}, "A list of foos")
	assert.Equal(t, []string{}, *foo)
	assert.Equal(t, []string{}, flags.Lookup("foo").Value.(SliceFlag).UnderlyingSlice().([]string))
	err = flags.Set("foo", "foo0,foo1")
	assert.NoError(t, err)
	err = flags.Set("foo", "foo2")
	assert.NoError(t, err)
	err = flags.Set("foo", "foo3,foo4,foo5")
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo0", "foo1", "foo2", "foo3", "foo4", "foo5"}, *foo)
	assert.Equal(t, []string{"foo0", "foo1", "foo2", "foo3", "foo4", "foo5"}, flags.Lookup("foo").Value.(SliceFlag).UnderlyingSlice().([]string))

	bar := StringSlice("bar", []string{"bar0", "bar1"}, "A list of bars")
	assert.Equal(t, []string{"bar0", "bar1"}, *bar)
	assert.Equal(t, []string{"bar0", "bar1"}, flags.Lookup("bar").Value.(SliceFlag).UnderlyingSlice().([]string))
	err = flags.Set("bar", "bar2")
	assert.NoError(t, err)
	err = flags.Set("bar", "bar3,bar4,bar5")
	assert.NoError(t, err)
	assert.Equal(t, []string{"bar0", "bar1", "bar2", "bar3", "bar4", "bar5"}, *bar)
	assert.Equal(t, []string{"bar0", "bar1", "bar2", "bar3", "bar4", "bar5"}, flags.Lookup("bar").Value.(SliceFlag).UnderlyingSlice().([]string))

	baz := StringSlice("baz", []string{}, "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, *bar, *baz)

	testSlice := []string{"yes", "si", "hai"}
	testFlag, err := NewSliceFlag(&testSlice)
	require.NoError(t, err)
	testFlag.SetTo(testFlag.AppendSlice(testFlag.UnderlyingSlice()))
	assert.Equal(t, []string{"yes", "si", "hai", "yes", "si", "hai"}, testSlice)
	newSlice := testFlag.AppendSlice([]string{"no", "nyet", "iie"})
	assert.Equal(t, []string{"yes", "si", "hai", "yes", "si", "hai"}, testSlice)
	testFlag.SetTo(newSlice)
	assert.Equal(t, []string{"yes", "si", "hai", "yes", "si", "hai", "no", "nyet", "iie"}, testSlice)
	assert.Equal(t, 9, testFlag.Len())
}

func TestStructSliceFlag(t *testing.T) {
	var err error

	flags := replaceFlagsForTesting(t)

	fooFlag := []testStruct{}
	StructSliceVar(&fooFlag, "foo", "A list of foos")
	assert.Equal(t, []testStruct{}, fooFlag)
	assert.Equal(t, []testStruct{}, flags.Lookup("foo").Value.(SliceFlag).UnderlyingSlice().([]testStruct))
	err = flags.Set("foo", `[{"field":3,"meadow":"watership down"}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}}, fooFlag)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}}, flags.Lookup("foo").Value.(SliceFlag).UnderlyingSlice().([]testStruct))
	err = flags.Set("foo", `{"field":5,"meadow":"runnymede"}`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}, {Field: 5, Meadow: "runnymede"}}, fooFlag)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}, {Field: 5, Meadow: "runnymede"}}, flags.Lookup("foo").Value.(SliceFlag).UnderlyingSlice().([]testStruct))
	flags.Lookup("foo").Value.(SliceFlag).SetTo([]testStruct{{Field: 7, Meadow: "rose end"}, {}, {Field: 9}})
	assert.Equal(t, []testStruct{{Field: 7, Meadow: "rose end"}, {}, {Field: 9}}, fooFlag)
	assert.Equal(t, []testStruct{{Field: 7, Meadow: "rose end"}, {}, {Field: 9}}, flags.Lookup("foo").Value.(SliceFlag).UnderlyingSlice().([]testStruct))

	barFlag := []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}
	StructSliceVar(&barFlag, "bar", "A list of bars")
	assert.Equal(t, []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}, barFlag)
	assert.Equal(t, []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}, flags.Lookup("bar").Value.(SliceFlag).UnderlyingSlice().([]testStruct))
	flags.Lookup("bar").Value.(SliceFlag).SetTo([]testStruct{})
	assert.Equal(t, []testStruct{}, barFlag)
	assert.Equal(t, []testStruct{}, flags.Lookup("bar").Value.(SliceFlag).UnderlyingSlice().([]testStruct))
	err = flags.Set("bar", `[{"field":13,"meadow":"cors y llyn"},{},{"field":15}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}}, barFlag)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}}, flags.Lookup("bar").Value.(SliceFlag).UnderlyingSlice().([]testStruct))
	err = flags.Set("bar", `[{"field":17,"meadow":"red hill"},{},{"field":19}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}, {Field: 17, Meadow: "red hill"}, {}, {Field: 19}}, barFlag)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}, {Field: 17, Meadow: "red hill"}, {}, {Field: 19}}, flags.Lookup("bar").Value.(SliceFlag).UnderlyingSlice().([]testStruct))

	bazFlag := []testStruct{}
	StructSliceVar(&bazFlag, "baz", "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, barFlag, bazFlag)

	testSlice := []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}}
	testFlag, err := NewSliceFlag(&testSlice)
	assert.NoError(t, err)
	testFlag.SetTo(testFlag.AppendSlice(testFlag.UnderlyingSlice()))
	assert.Equal(t, []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}, {}, {Field: 1}, {Meadow: "Paradise"}}, testSlice)
	newSlice := testFlag.AppendSlice([]testStruct{{Field: -1, Meadow: "sunflower fields"}, {Field: -3, Meadow: "keukenhof gardens"}})
	assert.Equal(t, []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}, {}, {Field: 1}, {Meadow: "Paradise"}}, testSlice)
	testFlag.SetTo(newSlice)
	assert.Equal(t, []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}, {}, {Field: 1}, {Meadow: "Paradise"}, {Field: -1, Meadow: "sunflower fields"}, {Field: -3, Meadow: "keukenhof gardens"}}, testSlice)
	assert.Equal(t, 8, testFlag.Len())
}

func TestGenerateYAMLMapFromFlags(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	flags.Bool("bool", true, "")
	flags.Int("one.two.int", 10, "")
	StringSlice("one.two.string_slice", []string{"hi", "hello"}, "")
	flags.Float64("one.two.two_and_a_half.float64", 5.2, "")
	StructSliceVar(&[]testStruct{{Field: 4, Meadow: "Great"}}, "one.two.three.struct_slice", "")
	flags.String("a.b.string", "xxx", "")
	URLString("a.b.url", "https://www.example.com", "")
	actual, err := GenerateYAMLMapFromFlags()
	require.NoError(t, err)
	expected := map[interface{}]interface{}{
		"bool": false,
		"one": map[interface{}]interface{}{
			"two": map[interface{}]interface{}{
				"int":          int(0),
				"string_slice": ([]string)(nil),
				"two_and_a_half": map[interface{}]interface{}{
					"float64": float64(0),
				},
				"three": map[interface{}]interface{}{
					"struct_slice": ([]testStruct)(nil),
				},
			},
		},
		"a": map[interface{}]interface{}{
			"b": map[interface{}]interface{}{
				"string": "",
				"url":    URLFlag(""),
			},
		},
	}
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error(diff)
	}
}

func TestBadGenerateYAMLMapFromFlags(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	flags.Int("one.two.int", 10, "")
	flags.Int("one.two", 10, "")
	_, err := GenerateYAMLMapFromFlags()
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)

	flags.Int("one.two", 10, "")
	flags.Int("one.two.int", 10, "")
	_, err = GenerateYAMLMapFromFlags()
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)

	flags.Var(&unsupportedFlagValue{}, "unsupported", "")
	_, err = GenerateYAMLMapFromFlags()
	require.Error(t, err)

}

func TestPopulateFlagsFromYAML(t *testing.T) {
	flags := replaceFlagsForTesting(t)

	flagBool := flags.Bool("bool", true, "")
	flagOneTwoInt := flags.Int("one.two.int", 10, "")
	flagOneTwoStringSlice := StringSlice("one.two.string_slice", []string{"hi", "hello"}, "")
	flagOneTwoTwoAndAHalfFloat := flags.Float64("one.two.two_and_a_half.float64", 5.2, "")
	flagOneTwoThreeStructSlice := []testStruct{{Field: 4, Meadow: "Great"}}
	StructSliceVar(&flagOneTwoThreeStructSlice, "one.two.three.struct_slice", "")
	flagABString := flags.String("a.b.string", "xxx", "")
	flagABStructSlice := []testStruct{{Field: 7, Meadow: "Chimney"}}
	StructSliceVar(&flagABStructSlice, "a.b.struct_slice", "")
	flagABURL := URLString("a.b.url", "https://www.example.com", "")
	input := map[interface{}]interface{}{
		"bool": false,
		"one": map[interface{}]interface{}{
			"two": map[interface{}]interface{}{
				"string_slice": []string{"meow", "woof"},
				"two_and_a_half": map[interface{}]interface{}{
					"float64": float64(7),
				},
				"three": map[interface{}]interface{}{
					"struct_slice": ([]testStruct)(nil),
				},
			},
		},
		"a": map[interface{}]interface{}{
			"b": map[interface{}]interface{}{
				"string":       "",
				"struct_slice": []testStruct{{Field: 9}},
				"url":          URLFlag("https://www.example.com:8080"),
			},
		},
		"undefined": struct{}{}, // keys without with no corresponding flag name should be ignored.
	}
	err := PopulateFlagsFromYAMLMap(input)
	require.NoError(t, err)

	assert.Equal(t, false, *flagBool)
	assert.Equal(t, 10, *flagOneTwoInt)
	assert.Equal(t, []string{"hi", "hello", "meow", "woof"}, *flagOneTwoStringSlice)
	assert.Equal(t, float64(7), *flagOneTwoTwoAndAHalfFloat)
	assert.Equal(t, []testStruct{{Field: 4, Meadow: "Great"}}, flagOneTwoThreeStructSlice)
	assert.Equal(t, "", *flagABString)
	assert.Equal(t, []testStruct{{Field: 7, Meadow: "Chimney"}, {Field: 9}}, flagABStructSlice)
	assert.Equal(t, "https://www.example.com:8080", *flagABURL)
}

func TestBadPopulateFlagsFromYAML(t *testing.T) {
	_ = replaceFlagsForTesting(t)

	err := PopulateFlagsFromYAMLMap(map[interface{}]interface{}{
		struct{}{}: "invalid key type",
	})
	require.Error(t, err)

	flags := replaceFlagsForTesting(t)
	flags.Var(&unsupportedFlagValue{}, "unsupported", "")
	err = PopulateFlagsFromYAMLMap(map[interface{}]interface{}{
		"unsupported": 0,
	})
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	flags.Bool("bool", false, "")
	err = PopulateFlagsFromYAMLMap(map[interface{}]interface{}{
		"bool": 0,
	})
	require.Error(t, err)
}

func TestSetValueForFlagName(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	flagBool := flags.Bool("bool", false, "")
	err := SetValueForFlagName("bool", true, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, true, *flagBool)

	flags = replaceFlagsForTesting(t)
	flagBool = flags.Bool("bool", false, "")
	err = SetValueForFlagName("bool", true, map[string]struct{}{"bool": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, false, *flagBool)

	flags = replaceFlagsForTesting(t)
	err = SetValueForFlagName("bool", true, map[string]struct{}{}, true, false)
	require.NoError(t, err)

	flags = replaceFlagsForTesting(t)
	flagInt := flags.Int("int", 2, "")
	err = SetValueForFlagName("int", 1, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, 1, *flagInt)

	flags = replaceFlagsForTesting(t)
	flagInt = flags.Int("int", 2, "")
	err = SetValueForFlagName("int", 1, map[string]struct{}{"int": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, 2, *flagInt)

	flags = replaceFlagsForTesting(t)
	flagInt64 := flags.Int64("int64", 2, "")
	err = SetValueForFlagName("int64", 1, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, int64(1), *flagInt64)

	flags = replaceFlagsForTesting(t)
	flagInt64 = flags.Int64("int64", 2, "")
	err = SetValueForFlagName("int64", 1, map[string]struct{}{"int64": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, int64(2), *flagInt64)

	flags = replaceFlagsForTesting(t)
	flagUint := flags.Uint("uint", 2, "")
	err = SetValueForFlagName("uint", 1, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, uint(1), *flagUint)

	flags = replaceFlagsForTesting(t)
	flagUint = flags.Uint("uint", 2, "")
	err = SetValueForFlagName("uint", 1, map[string]struct{}{"uint": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, uint(2), *flagUint)

	flags = replaceFlagsForTesting(t)
	flagUint64 := flags.Uint64("uint64", 2, "")
	err = SetValueForFlagName("uint64", 1, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), *flagUint64)

	flags = replaceFlagsForTesting(t)
	flagUint64 = flags.Uint64("uint64", 2, "")
	err = SetValueForFlagName("uint64", 1, map[string]struct{}{"uint64": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), *flagUint64)

	flags = replaceFlagsForTesting(t)
	flagFloat64 := flags.Float64("float64", 2, "")
	err = SetValueForFlagName("float64", 1, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, float64(1), *flagFloat64)

	flags = replaceFlagsForTesting(t)
	flagFloat64 = flags.Float64("float64", 2, "")
	err = SetValueForFlagName("float64", 1, map[string]struct{}{"float64": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, float64(2), *flagFloat64)

	flags = replaceFlagsForTesting(t)
	flagString := flags.String("string", "2", "")
	err = SetValueForFlagName("string", "1", map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, "1", *flagString)

	flags = replaceFlagsForTesting(t)
	flagString = flags.String("string", "2", "")
	err = SetValueForFlagName("string", "1", map[string]struct{}{"string": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, "2", *flagString)

	flags = replaceFlagsForTesting(t)
	flagURL := URLString("url", "https://www.example.com", "")
	err = SetValueForFlagName("url", "https://www.example.com:8080", map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, "https://www.example.com:8080", *flagURL)

	flags = replaceFlagsForTesting(t)
	flagURL = URLString("url", "https://www.example.com", "")
	err = SetValueForFlagName("url", "https://www.example.com:8080", map[string]struct{}{"url": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, "https://www.example.com", *flagURL)

	flags = replaceFlagsForTesting(t)
	flagStringSlice := StringSlice("string_slice", []string{"1", "2"}, "")
	err = SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	err = SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{"string_slice": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	err = SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{}, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	err = SetValueForFlagName("string_slice", []string{"3"}, map[string]struct{}{"string_slice": struct{}{}}, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice := []testStruct{{Field: 1}, {Field: 2}}
	StructSliceVar(&flagStructSlice, "struct_slice", "")
	err = SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}, {Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	StructSliceVar(&flagStructSlice, "struct_slice", "")
	err = SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{"struct_slice": struct{}{}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}, {Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	StructSliceVar(&flagStructSlice, "struct_slice", "")
	err = SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{}, false, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3}}, flagStructSlice)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	StructSliceVar(&flagStructSlice, "struct_slice", "")
	err = SetValueForFlagName("struct_slice", []testStruct{{Field: 3}}, map[string]struct{}{"struct_slice": struct{}{}}, false, true)
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, flagStructSlice)
}

func TestBadSetValueForFlagName(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	_ = flags.Bool("bool", false, "")
	err := SetValueForFlagName("bool", 0, map[string]struct{}{}, true, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	err = SetValueForFlagName("bool", false, map[string]struct{}{}, true, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	_ = StringSlice("string_slice", []string{"1", "2"}, "")
	err = SetValueForFlagName("string_slice", "3", map[string]struct{}{}, true, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	_ = StringSlice("string_slice", []string{"1", "2"}, "")
	err = SetValueForFlagName("string_slice", "3", map[string]struct{}{}, false, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	flagStructSlice := []testStruct{{Field: 1}, {Field: 2}}
	StructSliceVar(&flagStructSlice, "struct_slice", "")
	err = SetValueForFlagName("struct_slice", testStruct{Field: 3}, map[string]struct{}{}, true, true)
	require.Error(t, err)

	flags = replaceFlagsForTesting(t)
	flagStructSlice = []testStruct{{Field: 1}, {Field: 2}}
	StructSliceVar(&flagStructSlice, "struct_slice", "")
	err = SetValueForFlagName("struct_slice", testStruct{Field: 3}, map[string]struct{}{}, false, true)
	require.Error(t, err)
}

func TestDereferencedValueFromFlagName(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	_ = flags.Bool("bool", false, "")
	v, err := DereferencedValueFromFlagName("bool")
	require.NoError(t, err)
	assert.Equal(t, false, v)

	flags = replaceFlagsForTesting(t)
	_ = flags.Bool("bool", true, "")
	v, err = DereferencedValueFromFlagName("bool")
	require.NoError(t, err)
	assert.Equal(t, true, v)

	flags = replaceFlagsForTesting(t)
	_ = StringSlice("string_slice", []string{"1", "2"}, "")
	v, err = DereferencedValueFromFlagName("string_slice")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, v)

	flags = replaceFlagsForTesting(t)
	StructSliceVar(&[]testStruct{{Field: 1}, {Field: 2}}, "struct_slice", "")
	v, err = DereferencedValueFromFlagName("struct_slice")
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, v)
}

func TestBadDereferencedValueFromFlagName(t *testing.T) {
	_ = replaceFlagsForTesting(t)
	_, err := DereferencedValueFromFlagName("unknown_flag")
	require.Error(t, err)

	flags := replaceFlagsForTesting(t)
	flags.Var(&unsupportedFlagValue{}, "unsupported", "")
	_, err = DereferencedValueFromFlagName("unsupported")
	require.Error(t, err)
}
