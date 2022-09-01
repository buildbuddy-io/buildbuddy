package types

import (
	"flag"
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

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

func TestStringSliceFlag(t *testing.T) {
	var err error

	flags := replaceFlagsForTesting(t)

	foo := StringSlice("foo", []string{}, "A list of foos")
	assert.Equal(t, []string{}, *foo)
	assert.Equal(t, []string{}, *(*[]string)(flags.Lookup("foo").Value.(*StringSliceFlag)))
	err = flags.Set("foo", "foo0,foo1")
	assert.NoError(t, err)
	err = flags.Set("foo", "foo2")
	assert.NoError(t, err)
	err = flags.Set("foo", "foo3,foo4,foo5")
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo0", "foo1", "foo2", "foo3", "foo4", "foo5"}, *foo)
	assert.Equal(t, []string{"foo0", "foo1", "foo2", "foo3", "foo4", "foo5"}, *(*[]string)(flags.Lookup("foo").Value.(*StringSliceFlag)))

	bar := StringSlice("bar", []string{"bar0", "bar1"}, "A list of bars")
	assert.Equal(t, []string{"bar0", "bar1"}, *bar)
	assert.Equal(t, []string{"bar0", "bar1"}, *(*[]string)(flags.Lookup("bar").Value.(*StringSliceFlag)))
	err = flags.Set("bar", "bar2")
	assert.NoError(t, err)
	err = flags.Set("bar", "bar3,bar4,bar5")
	assert.NoError(t, err)
	assert.Equal(t, []string{"bar0", "bar1", "bar2", "bar3", "bar4", "bar5"}, *bar)
	assert.Equal(t, []string{"bar0", "bar1", "bar2", "bar3", "bar4", "bar5"}, *(*[]string)(flags.Lookup("bar").Value.(*StringSliceFlag)))

	baz := StringSlice("baz", []string{}, "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, *bar, *baz)

	testSlice := []string{"yes", "si", "hai"}
	testFlag := NewStringSliceFlag(&testSlice)
	testFlag.AppendSlice(([]string)(*testFlag))
	assert.Equal(t, []string{"yes", "si", "hai", "yes", "si", "hai"}, testSlice)

	// `String` should not panic on zero-constructed flag
	assert.Equal(t, "", reflect.New(reflect.TypeOf((*StringSliceFlag)(nil)).Elem()).Interface().(flag.Value).String())
}

func TestStructSliceFlag(t *testing.T) {
	var err error

	flags := replaceFlagsForTesting(t)

	fooFlag := JSONSlice("foo", []testStruct{}, "A list of foos")
	assert.Equal(t, []testStruct{}, *fooFlag)
	assert.Equal(t, []testStruct{}, ([]testStruct)(flags.Lookup("foo").Value.(*JSONSliceFlag[[]testStruct]).Slice()))
	err = flags.Set("foo", `[{"field":3,"meadow":"watership down"}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}}, *fooFlag)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}}, ([]testStruct)(flags.Lookup("foo").Value.(*JSONSliceFlag[[]testStruct]).Slice()))
	err = flags.Set("foo", `{"field":5,"meadow":"runnymede"}`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}, {Field: 5, Meadow: "runnymede"}}, *fooFlag)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}, {Field: 5, Meadow: "runnymede"}}, ([]testStruct)(flags.Lookup("foo").Value.(*JSONSliceFlag[[]testStruct]).Slice()))

	barFlag := []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}
	JSONSliceVar(&barFlag, "bar", barFlag, "A list of bars")
	assert.Equal(t, []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}, barFlag)
	assert.Equal(t, []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}, ([]testStruct)(flags.Lookup("bar").Value.(*JSONSliceFlag[[]testStruct]).Slice()))

	fooxFlag := JSONSlice("foox", []testStruct{}, "A list of fooxes")
	assert.Equal(t, []testStruct{}, *fooxFlag)
	assert.Equal(t, []testStruct{}, ([]testStruct)(flags.Lookup("foox").Value.(*JSONSliceFlag[[]testStruct]).Slice()))
	err = flags.Set("foox", `[{"field":13,"meadow":"cors y llyn"},{},{"field":15}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}}, *fooxFlag)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}}, ([]testStruct)(flags.Lookup("foox").Value.(*JSONSliceFlag[[]testStruct]).Slice()))
	err = flags.Set("foox", `[{"field":17,"meadow":"red hill"},{},{"field":19}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}, {Field: 17, Meadow: "red hill"}, {}, {Field: 19}}, *fooxFlag)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}, {Field: 17, Meadow: "red hill"}, {}, {Field: 19}}, ([]testStruct)(flags.Lookup("foox").Value.(*JSONSliceFlag[[]testStruct]).Slice()))

	bazFlag := []testStruct{}
	JSONSliceVar(&bazFlag, "baz", bazFlag, "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, barFlag, bazFlag)

	testSlice := []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}}
	testFlag := NewJSONSliceFlag(&testSlice)
	testFlag.AppendSlice(testFlag.Slice())
	assert.Equal(t, []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}, {}, {Field: 1}, {Meadow: "Paradise"}}, testSlice)

	// `String` should not panic on zero-constructed flag
	assert.Equal(t, "[]", reflect.New(reflect.TypeOf((*JSONSliceFlag[[]testStruct])(nil)).Elem()).Interface().(flag.Value).String())
}

func TestProtoSliceFlag(t *testing.T) {
	var err error

	flags := replaceFlagsForTesting(t)

	fooFlag := JSONSlice("foo", []*timestamppb.Timestamp{}, "A list of foos")
	assert.Equal(t, []*timestamppb.Timestamp{}, *fooFlag)
	assert.Equal(t, []*timestamppb.Timestamp{}, ([]*timestamppb.Timestamp)(flags.Lookup("foo").Value.(*JSONSliceFlag[[]*timestamppb.Timestamp]).Slice()))
	err = flags.Set("foo", `[{"seconds":3,"nanos":5}]`)
	assert.NoError(t, err)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 3, Nanos: 5}}, *fooFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 3, Nanos: 5}}, ([]*timestamppb.Timestamp)(flags.Lookup("foo").Value.(*JSONSliceFlag[[]*timestamppb.Timestamp]).Slice()))
	err = flags.Set("foo", `{"seconds":5,"nanos":9}`)
	assert.NoError(t, err)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 3, Nanos: 5}, {Seconds: 5, Nanos: 9}}, *fooFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 3, Nanos: 5}, {Seconds: 5, Nanos: 9}}, ([]*timestamppb.Timestamp)(flags.Lookup("foo").Value.(*JSONSliceFlag[[]*timestamppb.Timestamp]).Slice()))

	barFlag := []*timestamppb.Timestamp{{Seconds: 11, Nanos: 100}, {Seconds: 13, Nanos: 256}}
	JSONSliceVar(&barFlag, "bar", barFlag, "A list of bars")
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 11, Nanos: 100}, {Seconds: 13, Nanos: 256}}, barFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 11, Nanos: 100}, {Seconds: 13, Nanos: 256}}, ([]*timestamppb.Timestamp)(flags.Lookup("bar").Value.(*JSONSliceFlag[[]*timestamppb.Timestamp]).Slice()))

	fooxFlag := JSONSlice("foox", []*timestamppb.Timestamp{}, "A list of fooxes")
	assert.Equal(t, []*timestamppb.Timestamp{}, *fooxFlag)
	assert.Equal(t, []*timestamppb.Timestamp{}, ([]*timestamppb.Timestamp)(flags.Lookup("foox").Value.(*JSONSliceFlag[[]*timestamppb.Timestamp]).Slice()))
	err = flags.Set("foox", `[{"seconds":13,"nanos":64},{},{"seconds":15}]`)
	assert.NoError(t, err)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 13, Nanos: 64}, {}, {Seconds: 15}}, *fooxFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 13, Nanos: 64}, {}, {Seconds: 15}}, ([]*timestamppb.Timestamp)(flags.Lookup("foox").Value.(*JSONSliceFlag[[]*timestamppb.Timestamp]).Slice()))
	err = flags.Set("foox", `[{"seconds":17,"nanos":9001},{},{"seconds":19}]`)
	assert.NoError(t, err)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 13, Nanos: 64}, {}, {Seconds: 15}, {Seconds: 17, Nanos: 9001}, {}, {Seconds: 19}}, *fooxFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 13, Nanos: 64}, {}, {Seconds: 15}, {Seconds: 17, Nanos: 9001}, {}, {Seconds: 19}}, ([]*timestamppb.Timestamp)(flags.Lookup("foox").Value.(*JSONSliceFlag[[]*timestamppb.Timestamp]).Slice()))

	bazFlag := []*timestamppb.Timestamp{}
	JSONSliceVar(&bazFlag, "baz", bazFlag, "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, barFlag, bazFlag)

	testSlice := []*timestamppb.Timestamp{{}, {Seconds: 1}, {Nanos: 99}}
	testFlag := NewJSONSliceFlag(&testSlice)
	testFlag.AppendSlice(testFlag.Slice())
	assert.Equal(t, []*timestamppb.Timestamp{{}, {Seconds: 1}, {Nanos: 99}, {}, {Seconds: 1}, {Nanos: 99}}, testSlice)

	// `String` should not panic on zero-constructed flag
	assert.Equal(t, "[]", reflect.New(reflect.TypeOf((*JSONSliceFlag[[]*timestamppb.Timestamp])(nil)).Elem()).Interface().(flag.Value).String())
}

func TestJSONStructFlag(t *testing.T) {
	var err error
	flags := replaceFlagsForTesting(t)
	flagName := "foo"

	f := JSONStruct(flagName, testStruct{}, "A flag that should contain a testStruct")
	assert.Equal(t, testStruct{}, *f)
	assert.Equal(t, testStruct{}, (flags.Lookup(flagName).Value.(*JSONStructFlag[testStruct]).Struct()))

	err = flags.Set(flagName, `{"field":3,"meadow":"watership down"}`)
	assert.NoError(t, err)
	assert.Equal(t, testStruct{Field: 3, Meadow: "watership down"}, *f)
	assert.Equal(t, testStruct{Field: 3, Meadow: "watership down"}, (flags.Lookup(flagName).Value.(*JSONStructFlag[testStruct]).Struct()))

	err = flags.Set(flagName, `{"field":5,"meadow":"runnymede"}`)
	assert.NoError(t, err)
	assert.Equal(t, testStruct{Field: 5, Meadow: "runnymede"}, *f)
	assert.Equal(t, testStruct{Field: 5, Meadow: "runnymede"}, (flags.Lookup(flagName).Value.(*JSONStructFlag[testStruct]).Struct()))

	// `String` should not panic on zero-constructed flag
	assert.Equal(t, "{}", reflect.New(reflect.TypeOf((*JSONStructFlag[testStruct])(nil)).Elem()).Interface().(flag.Value).String())
}

func TestFlagAlias(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	s := flags.String("string", "test", "")
	as := Alias[string]("string", "string_alias")
	aas := Alias[string]("string_alias", "string_alias_alias")
	assert.Equal(t, *s, "test")
	assert.Equal(t, s, as)
	assert.Equal(t, as, aas)
	flags.Lookup("string").Value.Set("moo")
	assert.Equal(t, *s, "moo")
	flags.Lookup("string_alias").Value.Set("woof")
	assert.Equal(t, *s, "woof")
	flags.Lookup("string_alias_alias").Value.Set("meow")
	assert.Equal(t, *s, "meow")

	asf := flags.Lookup("string_alias").Value.(*FlagAlias[string])
	assert.Equal(t, "meow", asf.String())
	assert.Equal(t, "string", asf.AliasedName())
	asfType, err := common.GetTypeForFlagValue(asf)
	require.NoError(t, err)
	assert.Equal(t, reflect.TypeOf((*string)(nil)), asfType)
	asfYAMLType, err := flagyaml.GetYAMLTypeForFlagValue(asf)
	require.NoError(t, err)
	assert.Equal(t, reflect.TypeOf((*string)(nil)), asfYAMLType)

	aasf := flags.Lookup("string_alias").Value.(*FlagAlias[string])
	assert.Equal(t, "meow", aasf.String())
	assert.Equal(t, "string", aasf.AliasedName())
	aasfType, err := common.GetTypeForFlagValue(asf)
	require.NoError(t, err)
	assert.Equal(t, reflect.TypeOf((*string)(nil)), aasfType)
	aasfYAMLType, err := flagyaml.GetYAMLTypeForFlagValue(asf)
	require.NoError(t, err)
	assert.Equal(t, reflect.TypeOf((*string)(nil)), aasfYAMLType)

	p := Alias[string]("string_alias_alias")
	assert.Equal(t, aas, p)

	flags = replaceFlagsForTesting(t)

	flagString := flags.String("string", "test", "")
	Alias[string]("string", "string_alias")
	Alias[string]("string", "string_alias2")
	Alias[string]("string", "string_alias3")
	yamlData := `
string: "woof"
string_alias2: "moo"
string_alias3: "oink"
string_alias: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "meow", *flagString)

	flags = replaceFlagsForTesting(t)

	flagStringSlice := StringSlice("string_slice", []string{"test"}, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	Alias[[]string]("string_slice", "string_slice_alias2")
	Alias[[]string]("string_slice", "string_slice_alias3")
	flags.Set("string_slice", "squeak")
	yamlData = `
string_slice:
  - "woof"
string_slice_alias2:
  - "moo"
string_slice_alias3:
  - "oink"
  - "ribbit"
string_slice_alias:
  - "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, []string{"test", "squeak", "woof", "moo", "oink", "ribbit", "meow"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)

	flagString = flags.String("string", "test", "")
	Alias[string]("string", "string_alias")
	yamlData = `
string_alias: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "meow", *flagString)

	flags = replaceFlagsForTesting(t)

	flagString = flags.String("string", "test", "")
	Alias[string]("string", "string_alias")
	flags.Set("string", "moo")
	yamlData = `
string_alias: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "moo", *flagString)

	flags = replaceFlagsForTesting(t)

	flagString = flags.String("string", "test", "")
	Alias[string]("string", "string_alias")
	flags.Set("string_alias", "moo")
	yamlData = `
string: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "moo", *flagString)

	flags = replaceFlagsForTesting(t)

	flagString = flags.String("string", "test", "")
	Alias[string]("string", "string_alias")
	flags.Set("string_alias", "moo")
	yamlData = `
string_alias: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "moo", *flagString)

	flags = replaceFlagsForTesting(t)
	flagString = flags.String("string", "2", "")
	Alias[string]("string", "string_alias")
	err = common.SetValueForFlagName("string_alias", "1", map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, "1", *flagString)

	flags = replaceFlagsForTesting(t)
	flagString = flags.String("string", "2", "")
	Alias[string]("string", "string_alias")
	err = common.SetValueForFlagName("string_alias", "1", map[string]struct{}{"string": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, "2", *flagString)

	flags = replaceFlagsForTesting(t)
	string_slice := make([]string, 2)
	string_slice[0] = "1"
	string_slice[1] = "2"
	StringSliceVar(&string_slice, "string_slice", string_slice, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	err = common.SetValueForFlagName("string_slice_alias", []string{"3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, string_slice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	err = common.SetValueForFlagName("string_slice_alias", []string{"3"}, map[string]struct{}{"string_slice": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	err = common.SetValueForFlagName("string_slice_alias", []string{"3"}, map[string]struct{}{}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	err = common.SetValueForFlagName("string_slice_alias", []string{"3"}, map[string]struct{}{"string_slice": {}}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	string_slice = make([]string, 2)
	string_slice[0] = "1"
	string_slice[1] = "2"
	StringSliceVar(&string_slice, "string_slice", string_slice, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	Alias[[]string]("string_slice_alias", "string_slice_alias_alias")
	err = common.SetValueForFlagName("string_slice_alias_alias", []string{"3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, map[string]struct{}{}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, string_slice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	Alias[[]string]("string_slice_alias", "string_slice_alias_alias")
	err = common.SetValueForFlagName("string_slice_alias_alias", []string{"3"}, map[string]struct{}{"string_slice": {}}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	Alias[[]string]("string_slice_alias", "string_slice_alias_alias")
	err = common.SetValueForFlagName("string_slice_alias_alias", []string{"3"}, map[string]struct{}{}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = StringSlice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice", "string_slice_alias")
	Alias[[]string]("string_slice_alias", "string_slice_alias_alias")
	err = common.SetValueForFlagName("string_slice_alias_alias", []string{"3"}, map[string]struct{}{"string_slice": {}}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	_ = StringSlice("string_slice", []string{"1", "2"}, "")
	stringSlice, err := common.GetDereferencedValue[[]string]("string_slice")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, stringSlice)

	_ = Alias[[]string]("string_slice", "string_slice_alias")
	stringSliceAlias, err := common.GetDereferencedValue[[]string]("string_slice_alias")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, stringSliceAlias)

	_ = Alias[[]string]("string_slice_alias", "string_slice_alias_alias")
	stringSliceAliasAlias, err := common.GetDereferencedValue[[]string]("string_slice_alias_alias")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, stringSliceAliasAlias)

	flags = replaceFlagsForTesting(t)
	JSONSlice("struct_slice", []testStruct{{Field: 1}, {Field: 2}}, "")
	structSlice, err := common.GetDereferencedValue[[]testStruct]("struct_slice")
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, structSlice)

	// `String` should not panic on zero-constructed flag
	assert.Equal(t, "[]", reflect.New(reflect.TypeOf((*FlagAlias[[]testStruct])(nil)).Elem()).Interface().(flag.Value).String())
	assert.Equal(t, "", reflect.New(reflect.TypeOf((*FlagAlias[string])(nil)).Elem()).Interface().(flag.Value).String())
}

func TestDeprecatedVar(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	flagInt := DeprecatedVar[int](NewPrimitiveFlag(5), "deprecated_int", "", "migration plan")
	flagStringSlice := DeprecatedVar[[]string](NewStringSliceFlag(&[]string{"hi"}), "deprecated_string_slice", "", "migration plan")
	assert.Equal(t, *flagInt, 5)
	assert.Equal(t, *flagStringSlice, []string{"hi"})
	flags.Set("deprecated_int", "7")
	flags.Set("deprecated_string_slice", "hello")
	assert.Equal(t, *flagStringSlice, []string{"hi", "hello"})
	assert.Equal(t, *flagInt, 7)
	testInt, err := common.GetDereferencedValue[int]("deprecated_int")
	require.NoError(t, err)
	assert.Equal(t, testInt, 7)
	testStringSlice, err := common.GetDereferencedValue[[]string]("deprecated_string_slice")
	require.NoError(t, err)
	assert.Equal(t, testStringSlice, []string{"hi", "hello"})

	flags = replaceFlagsForTesting(t)

	flagInt = DeprecatedVar[int](NewPrimitiveFlag(5), "deprecated_int", "", "migration plan")
	flagString := DeprecatedVar[string](NewPrimitiveFlag(""), "deprecated_string", "", "migration plan")
	flagStringSlice = DeprecatedVar[[]string](NewStringSliceFlag(&[]string{"hi"}), "deprecated_string_slice", "", "migration plan")
	flags.Set("deprecated_int", "7")
	flags.Set("deprecated_string_slice", "hello")
	yamlData := `
deprecated_int: 9
deprecated_string: "moo"
deprecated_string_slice:
  - "hey"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, *flagInt, 7)
	assert.Equal(t, *flagString, "moo")
	assert.Equal(t, *flagStringSlice, []string{"hi", "hello", "hey"})
	testInt, err = common.GetDereferencedValue[int]("deprecated_int")
	require.NoError(t, err)
	assert.Equal(t, testInt, 7)
	testString, err := common.GetDereferencedValue[string]("deprecated_string")
	require.NoError(t, err)
	assert.Equal(t, testString, "moo")
	testStringSlice, err = common.GetDereferencedValue[[]string]("deprecated_string_slice")
	require.NoError(t, err)
	assert.Equal(t, testStringSlice, []string{"hi", "hello", "hey"})

	d := any(&DeprecatedFlag[struct{}]{})
	_, ok := d.(common.WrappingValue)
	assert.True(t, ok)
	_, ok = d.(common.SetValueForFlagNameHooked)
	assert.True(t, ok)
	_, ok = d.(flagyaml.YAMLSetValueHooked)
	assert.True(t, ok)

	// `String` should not panic on zero-constructed flag
	assert.Equal(t, "[]", reflect.New(reflect.TypeOf((*DeprecatedFlag[[]testStruct])(nil)).Elem()).Interface().(flag.Value).String())
	assert.Equal(t, "", reflect.New(reflect.TypeOf((*DeprecatedFlag[string])(nil)).Elem()).Interface().(flag.Value).String())
}

func TestDeprecate(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	flagInt := flags.Int("deprecated_int", 5, "some usage text")
	Deprecate[int]("deprecated_int", "migration plan")
	flagStringSlice := StringSlice("deprecated_string_slice", []string{"hi"}, "")
	Deprecate[[]string]("deprecated_string_slice", "migration plan")
	assert.Equal(t, *flagInt, 5)
	assert.Equal(t, *flagStringSlice, []string{"hi"})
	flags.Set("deprecated_int", "7")
	flags.Set("deprecated_string_slice", "hello")
	assert.Equal(t, *flagStringSlice, []string{"hi", "hello"})
	assert.Equal(t, *flagInt, 7)
	testInt, err := common.GetDereferencedValue[int]("deprecated_int")
	require.NoError(t, err)
	assert.Equal(t, testInt, 7)
	testStringSlice, err := common.GetDereferencedValue[[]string]("deprecated_string_slice")
	require.NoError(t, err)
	assert.Equal(t, testStringSlice, []string{"hi", "hello"})
	assert.Equal(t, "some usage text **DEPRECATED** migration plan", flags.Lookup("deprecated_int").Usage)

	flags = replaceFlagsForTesting(t)

	flagInt = flags.Int("deprecated_int", 5, "some usage text")
	Deprecate[int]("deprecated_int", "migration plan")
	flagString := flags.String("deprecated_string", "", "")
	Deprecate[string]("deprecated_string", "migration plan")
	flagStringSlice = DeprecatedVar[[]string](NewStringSliceFlag(&[]string{"hi"}), "deprecated_string_slice", "", "migration plan")
	flags.Set("deprecated_int", "7")
	flags.Set("deprecated_string_slice", "hello")
	yamlData := `
deprecated_int: 9
deprecated_string: "moo"
deprecated_string_slice:
  - "hey"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, *flagInt, 7)
	assert.Equal(t, *flagString, "moo")
	assert.Equal(t, *flagStringSlice, []string{"hi", "hello", "hey"})
	testInt, err = common.GetDereferencedValue[int]("deprecated_int")
	require.NoError(t, err)
	assert.Equal(t, testInt, 7)
	testString, err := common.GetDereferencedValue[string]("deprecated_string")
	require.NoError(t, err)
	assert.Equal(t, testString, "moo")
	testStringSlice, err = common.GetDereferencedValue[[]string]("deprecated_string_slice")
	require.NoError(t, err)
	assert.Equal(t, testStringSlice, []string{"hi", "hello", "hey"})
	assert.Equal(t, "some usage text **DEPRECATED** migration plan", flags.Lookup("deprecated_int").Usage)

	d := any(flags.Lookup("deprecated_int").Value)
	_, ok := d.(common.WrappingValue)
	assert.True(t, ok)
	_, ok = d.(common.SetValueForFlagNameHooked)
	assert.True(t, ok)
	_, ok = d.(flagyaml.YAMLSetValueHooked)
	assert.True(t, ok)
}

func TestURLFlag(t *testing.T) {
	// `String` should not panic on zero-constructed flag
	assert.Equal(t, "", reflect.New(reflect.TypeOf((*URLFlag)(nil)).Elem()).Interface().(flag.Value).String())
}
