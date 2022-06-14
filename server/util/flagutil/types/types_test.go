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

	foo := Slice("foo", []string{}, "A list of foos")
	assert.Equal(t, []string{}, *foo)
	assert.Equal(t, []string{}, *(*[]string)(flags.Lookup("foo").Value.(*SliceFlag[string])))
	err = flags.Set("foo", "foo0,foo1")
	assert.NoError(t, err)
	err = flags.Set("foo", "foo2")
	assert.NoError(t, err)
	err = flags.Set("foo", "foo3,foo4,foo5")
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo0", "foo1", "foo2", "foo3", "foo4", "foo5"}, *foo)
	assert.Equal(t, []string{"foo0", "foo1", "foo2", "foo3", "foo4", "foo5"}, *(*[]string)(flags.Lookup("foo").Value.(*SliceFlag[string])))

	bar := Slice("bar", []string{"bar0", "bar1"}, "A list of bars")
	assert.Equal(t, []string{"bar0", "bar1"}, *bar)
	assert.Equal(t, []string{"bar0", "bar1"}, *(*[]string)(flags.Lookup("bar").Value.(*SliceFlag[string])))
	err = flags.Set("bar", "bar2")
	assert.NoError(t, err)
	err = flags.Set("bar", "bar3,bar4,bar5")
	assert.NoError(t, err)
	assert.Equal(t, []string{"bar0", "bar1", "bar2", "bar3", "bar4", "bar5"}, *bar)
	assert.Equal(t, []string{"bar0", "bar1", "bar2", "bar3", "bar4", "bar5"}, *(*[]string)(flags.Lookup("bar").Value.(*SliceFlag[string])))

	baz := Slice("baz", []string{}, "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, *bar, *baz)

	testSlice := []string{"yes", "si", "hai"}
	testFlag := NewSliceFlag(&testSlice)
	testFlag.AppendSlice(*(*[]string)(testFlag))
	assert.Equal(t, []string{"yes", "si", "hai", "yes", "si", "hai"}, testSlice)
}

func TestStructSliceFlag(t *testing.T) {
	var err error

	flags := replaceFlagsForTesting(t)

	fooFlag := Slice("foo", []testStruct{}, "A list of foos")
	assert.Equal(t, []testStruct{}, *fooFlag)
	assert.Equal(t, []testStruct{}, *(*[]testStruct)(flags.Lookup("foo").Value.(*SliceFlag[testStruct])))
	err = flags.Set("foo", `[{"field":3,"meadow":"watership down"}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}}, *fooFlag)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}}, *(*[]testStruct)(flags.Lookup("foo").Value.(*SliceFlag[testStruct])))
	err = flags.Set("foo", `{"field":5,"meadow":"runnymede"}`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}, {Field: 5, Meadow: "runnymede"}}, *fooFlag)
	assert.Equal(t, []testStruct{{Field: 3, Meadow: "watership down"}, {Field: 5, Meadow: "runnymede"}}, *(*[]testStruct)(flags.Lookup("foo").Value.(*SliceFlag[testStruct])))

	barFlag := []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}
	SliceVar(&barFlag, "bar", "A list of bars")
	assert.Equal(t, []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}, barFlag)
	assert.Equal(t, []testStruct{{Field: 11, Meadow: "arcadia"}, {Field: 13, Meadow: "kingcombe"}}, *(*[]testStruct)(flags.Lookup("bar").Value.(*SliceFlag[testStruct])))

	fooxFlag := Slice("foox", []testStruct{}, "A list of fooxes")
	assert.Equal(t, []testStruct{}, *fooxFlag)
	assert.Equal(t, []testStruct{}, *(*[]testStruct)(flags.Lookup("foox").Value.(*SliceFlag[testStruct])))
	err = flags.Set("foox", `[{"field":13,"meadow":"cors y llyn"},{},{"field":15}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}}, *fooxFlag)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}}, *(*[]testStruct)(flags.Lookup("foox").Value.(*SliceFlag[testStruct])))
	err = flags.Set("foox", `[{"field":17,"meadow":"red hill"},{},{"field":19}]`)
	assert.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}, {Field: 17, Meadow: "red hill"}, {}, {Field: 19}}, *fooxFlag)
	assert.Equal(t, []testStruct{{Field: 13, Meadow: "cors y llyn"}, {}, {Field: 15}, {Field: 17, Meadow: "red hill"}, {}, {Field: 19}}, *(*[]testStruct)(flags.Lookup("foox").Value.(*SliceFlag[testStruct])))

	bazFlag := []testStruct{}
	SliceVar(&bazFlag, "baz", "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, barFlag, bazFlag)

	testSlice := []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}}
	testFlag := NewSliceFlag(&testSlice)
	testFlag.AppendSlice(*(*[]testStruct)(testFlag))
	assert.Equal(t, []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}, {}, {Field: 1}, {Meadow: "Paradise"}}, testSlice)
}

func TestProtoSliceFlag(t *testing.T) {
	var err error

	flags := replaceFlagsForTesting(t)

	fooFlag := Slice("foo", []*timestamppb.Timestamp{}, "A list of foos")
	assert.Equal(t, []*timestamppb.Timestamp{}, *fooFlag)
	assert.Equal(t, []*timestamppb.Timestamp{}, *(*[]*timestamppb.Timestamp)(flags.Lookup("foo").Value.(*SliceFlag[*timestamppb.Timestamp])))
	err = flags.Set("foo", `[{"seconds":3,"nanos":5}]`)
	assert.NoError(t, err)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 3, Nanos: 5}}, *fooFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 3, Nanos: 5}}, *(*[]*timestamppb.Timestamp)(flags.Lookup("foo").Value.(*SliceFlag[*timestamppb.Timestamp])))
	err = flags.Set("foo", `{"seconds":5,"nanos":9}`)
	assert.NoError(t, err)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 3, Nanos: 5}, {Seconds: 5, Nanos: 9}}, *fooFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 3, Nanos: 5}, {Seconds: 5, Nanos: 9}}, *(*[]*timestamppb.Timestamp)(flags.Lookup("foo").Value.(*SliceFlag[*timestamppb.Timestamp])))

	barFlag := []*timestamppb.Timestamp{{Seconds: 11, Nanos: 100}, {Seconds: 13, Nanos: 256}}
	SliceVar(&barFlag, "bar", "A list of bars")
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 11, Nanos: 100}, {Seconds: 13, Nanos: 256}}, barFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 11, Nanos: 100}, {Seconds: 13, Nanos: 256}}, *(*[]*timestamppb.Timestamp)(flags.Lookup("bar").Value.(*SliceFlag[*timestamppb.Timestamp])))

	fooxFlag := Slice("foox", []*timestamppb.Timestamp{}, "A list of fooxes")
	assert.Equal(t, []*timestamppb.Timestamp{}, *fooxFlag)
	assert.Equal(t, []*timestamppb.Timestamp{}, *(*[]*timestamppb.Timestamp)(flags.Lookup("foox").Value.(*SliceFlag[*timestamppb.Timestamp])))
	err = flags.Set("foox", `[{"seconds":13,"nanos":64},{},{"seconds":15}]`)
	assert.NoError(t, err)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 13, Nanos: 64}, {}, {Seconds: 15}}, *fooxFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 13, Nanos: 64}, {}, {Seconds: 15}}, *(*[]*timestamppb.Timestamp)(flags.Lookup("foox").Value.(*SliceFlag[*timestamppb.Timestamp])))
	err = flags.Set("foox", `[{"seconds":17,"nanos":9001},{},{"seconds":19}]`)
	assert.NoError(t, err)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 13, Nanos: 64}, {}, {Seconds: 15}, {Seconds: 17, Nanos: 9001}, {}, {Seconds: 19}}, *fooxFlag)
	assert.Equal(t, []*timestamppb.Timestamp{{Seconds: 13, Nanos: 64}, {}, {Seconds: 15}, {Seconds: 17, Nanos: 9001}, {}, {Seconds: 19}}, *(*[]*timestamppb.Timestamp)(flags.Lookup("foox").Value.(*SliceFlag[*timestamppb.Timestamp])))

	bazFlag := []*timestamppb.Timestamp{}
	SliceVar(&bazFlag, "baz", "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, barFlag, bazFlag)

	testSlice := []*timestamppb.Timestamp{{}, {Seconds: 1}, {Nanos: 99}}
	testFlag := NewSliceFlag(&testSlice)
	testFlag.AppendSlice(*(*[]*timestamppb.Timestamp)(testFlag))
	assert.Equal(t, []*timestamppb.Timestamp{{}, {Seconds: 1}, {Nanos: 99}, {}, {Seconds: 1}, {Nanos: 99}}, testSlice)

}

func TestFlagAlias(t *testing.T) {
	flags := replaceFlagsForTesting(t)
	s := flags.String("string", "test", "")
	as := Alias[string]("string_alias", "string")
	aas := Alias[string]("string_alias_alias", "string_alias")
	assert.Equal(t, *s, "test")
	assert.Equal(t, s, as)
	assert.Equal(t, as, aas)
	flags.Lookup("string").Value.Set("moo")
	assert.Equal(t, *s, "moo")
	flags.Lookup("string_alias").Value.Set("woof")
	assert.Equal(t, *s, "woof")
	flags.Lookup("string_alias_alias").Value.Set("meow")
	assert.Equal(t, *s, "meow")

	asf := flags.Lookup("string_alias").Value.(*FlagAlias)
	assert.Equal(t, "meow", asf.String())
	assert.Equal(t, "string", asf.AliasedName())
	assert.Equal(t, reflect.TypeOf((*string)(nil)), asf.AliasedType())
	assert.Equal(t, reflect.TypeOf((*string)(nil)), asf.YAMLTypeAlias())

	aasf := flags.Lookup("string_alias").Value.(*FlagAlias)
	assert.Equal(t, "meow", aasf.String())
	assert.Equal(t, "string", aasf.AliasedName())
	assert.Equal(t, reflect.TypeOf((*string)(nil)), aasf.AliasedType())
	assert.Equal(t, reflect.TypeOf((*string)(nil)), aasf.YAMLTypeAlias())

	flags = replaceFlagsForTesting(t)

	flagString := flags.String("string", "test", "")
	Alias[string]("string_alias", "string")
	Alias[string]("string_alias2", "string")
	Alias[string]("string_alias3", "string")
	yamlData := `
string: "woof"
string_alias2: "moo"
string_alias3: "oink"
string_alias: "meow"
`
	err := flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "meow", *flagString)

	flags = replaceFlagsForTesting(t)

	flagStringSlice := Slice("string_slice", []string{"test"}, "")
	Alias[[]string]("string_slice_alias", "string_slice")
	Alias[[]string]("string_slice_alias2", "string_slice")
	Alias[[]string]("string_slice_alias3", "string_slice")
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
	assert.Equal(t, []string{"test", "woof", "moo", "oink", "ribbit", "meow"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)

	flagString = flags.String("string", "test", "")
	Alias[string]("string_alias", "string")
	yamlData = `
string_alias: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "meow", *flagString)

	flags = replaceFlagsForTesting(t)

	flagString = flags.String("string", "test", "")
	Alias[string]("string_alias", "string")
	flags.Set("string", "moo")
	yamlData = `
string_alias: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "moo", *flagString)

	flags = replaceFlagsForTesting(t)

	flagString = flags.String("string", "test", "")
	Alias[string]("string_alias", "string")
	flags.Set("string_alias", "moo")
	yamlData = `
string: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "moo", *flagString)

	flags = replaceFlagsForTesting(t)

	flagString = flags.String("string", "test", "")
	Alias[string]("string_alias", "string")
	flags.Set("string_alias", "moo")
	yamlData = `
string_alias: "meow"
`
	err = flagyaml.PopulateFlagsFromData([]byte(yamlData))
	require.NoError(t, err)
	assert.Equal(t, "moo", *flagString)

	flags = replaceFlagsForTesting(t)
	flagString = flags.String("string", "2", "")
	Alias[string]("string_alias", "string")
	err = common.SetValueForFlagName("string_alias", "1", map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, "1", *flagString)

	flags = replaceFlagsForTesting(t)
	flagString = flags.String("string", "2", "")
	Alias[string]("string_alias", "string")
	err = common.SetValueForFlagName("string_alias", "1", map[string]struct{}{"string": {}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, "2", *flagString)

	flags = replaceFlagsForTesting(t)
	string_slice := make([]string, 2)
	string_slice[0] = "1"
	string_slice[1] = "2"
	SliceVar(&string_slice, "string_slice", "")
	Alias[[]string]("string_slice_alias", "string_slice")
	err = common.SetValueForFlagName("string_slice_alias", []string{"3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, string_slice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = Slice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice_alias", "string_slice")
	err = common.SetValueForFlagName("string_slice_alias", []string{"3"}, map[string]struct{}{"string_slice": {}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = Slice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice_alias", "string_slice")
	err = common.SetValueForFlagName("string_slice_alias", []string{"3"}, map[string]struct{}{}, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = Slice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice_alias", "string_slice")
	err = common.SetValueForFlagName("string_slice_alias", []string{"3"}, map[string]struct{}{"string_slice": {}}, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	string_slice = make([]string, 2)
	string_slice[0] = "1"
	string_slice[1] = "2"
	SliceVar(&string_slice, "string_slice", "")
	Alias[[]string]("string_slice_alias", "string_slice")
	Alias[[]string]("string_slice_alias_alias", "string_slice_alias")
	err = common.SetValueForFlagName("string_slice_alias_alias", []string{"3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, map[string]struct{}{}, true, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2"}, string_slice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = Slice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice_alias", "string_slice")
	Alias[[]string]("string_slice_alias_alias", "string_slice_alias")
	err = common.SetValueForFlagName("string_slice_alias_alias", []string{"3"}, map[string]struct{}{"string_slice": {}}, true, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = Slice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice_alias", "string_slice")
	Alias[[]string]("string_slice_alias_alias", "string_slice_alias")
	err = common.SetValueForFlagName("string_slice_alias_alias", []string{"3"}, map[string]struct{}{}, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"3"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	flagStringSlice = Slice("string_slice", []string{"1", "2"}, "")
	Alias[[]string]("string_slice_alias", "string_slice")
	Alias[[]string]("string_slice_alias_alias", "string_slice_alias")
	err = common.SetValueForFlagName("string_slice_alias_alias", []string{"3"}, map[string]struct{}{"string_slice": {}}, false, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, *flagStringSlice)

	flags = replaceFlagsForTesting(t)
	_ = Slice("string_slice", []string{"1", "2"}, "")
	stringSlice, err := common.GetDereferencedValue[[]string]("string_slice")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, stringSlice)

	_ = Alias[[]string]("string_slice_alias", "string_slice")
	stringSliceAlias, err := common.GetDereferencedValue[[]string]("string_slice_alias")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, stringSliceAlias)

	_ = Alias[[]string]("string_slice_alias_alias", "string_slice_alias")
	stringSliceAliasAlias, err := common.GetDereferencedValue[[]string]("string_slice_alias_alias")
	require.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, stringSliceAliasAlias)

	flags = replaceFlagsForTesting(t)
	SliceVar(&[]testStruct{{Field: 1}, {Field: 2}}, "struct_slice", "")
	structSlice, err := common.GetDereferencedValue[[]testStruct]("struct_slice")
	require.NoError(t, err)
	assert.Equal(t, []testStruct{{Field: 1}, {Field: 2}}, structSlice)
}
