package flagutil

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringSliceFlag(t *testing.T) {
	var err error

	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	fooFlag := []string{}
	flags.Var(NewSliceFlag(&fooFlag), "foo", "A list of foos")
	assert.Equal(t, []string{}, fooFlag)
	assert.Equal(t, []string{}, flags.Lookup("foo").Value.(SliceFlag).UnderlyingSlice().([]string))
	err = flags.Set("foo", "foo0,foo1")
	assert.NoError(t, err)
	err = flags.Set("foo", "foo2")
	assert.NoError(t, err)
	err = flags.Set("foo", "foo3,foo4,foo5")
	assert.NoError(t, err)
	assert.Equal(t, []string{"foo0", "foo1", "foo2", "foo3", "foo4", "foo5"}, fooFlag)
	assert.Equal(t, []string{"foo0", "foo1", "foo2", "foo3", "foo4", "foo5"}, flags.Lookup("foo").Value.(SliceFlag).UnderlyingSlice().([]string))

	barFlag := []string{"bar0", "bar1"}
	flags.Var(NewSliceFlag(&barFlag), "bar", "A list of bars")
	assert.Equal(t, []string{"bar0", "bar1"}, barFlag)
	assert.Equal(t, []string{"bar0", "bar1"}, flags.Lookup("bar").Value.(SliceFlag).UnderlyingSlice().([]string))
	err = flags.Set("bar", "bar2")
	assert.NoError(t, err)
	err = flags.Set("bar", "bar3,bar4,bar5")
	assert.NoError(t, err)
	assert.Equal(t, []string{"bar0", "bar1", "bar2", "bar3", "bar4", "bar5"}, barFlag)
	assert.Equal(t, []string{"bar0", "bar1", "bar2", "bar3", "bar4", "bar5"}, flags.Lookup("bar").Value.(SliceFlag).UnderlyingSlice().([]string))

	bazFlag := []string{}
	flags.Var(NewSliceFlag(&bazFlag), "baz", "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, barFlag, bazFlag)

	testSlice := []string{"yes", "si", "hai"}
	testFlag := NewSliceFlag(&testSlice)
	testFlag.SetTo(testFlag.AppendSlice(testFlag.UnderlyingSlice()))
	assert.Equal(t, []string{"yes", "si", "hai", "yes", "si", "hai"}, testSlice)
	newSlice := testFlag.AppendSlice([]string{"no", "nyet", "iie"})
	assert.Equal(t, []string{"yes", "si", "hai", "yes", "si", "hai"}, testSlice)
	testFlag.SetTo(newSlice)
	assert.Equal(t, []string{"yes", "si", "hai", "yes", "si", "hai", "no", "nyet", "iie"}, testSlice)
	assert.Equal(t, 9, testFlag.Len())
}

func TestStructSliceFlag(t *testing.T) {
	type testStruct struct {
		Field  int    `json:"field"`
		Meadow string `json:"meadow"`
	}

	var err error

	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	fooFlag := []testStruct{}
	flags.Var(NewSliceFlag(&fooFlag), "foo", "A list of foos")
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
	flags.Var(NewSliceFlag(&barFlag), "bar", "A list of bars")
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
	flags.Var(NewSliceFlag(&bazFlag), "baz", "A list of bazs")
	err = flags.Set("baz", flags.Lookup("bar").Value.String())
	assert.NoError(t, err)
	assert.Equal(t, barFlag, bazFlag)

	testSlice := []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}}
	testFlag := NewSliceFlag(&testSlice)
	testFlag.SetTo(testFlag.AppendSlice(testFlag.UnderlyingSlice()))
	assert.Equal(t, []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}, {}, {Field: 1}, {Meadow: "Paradise"}}, testSlice)
	newSlice := testFlag.AppendSlice([]testStruct{{Field: -1, Meadow: "sunflower fields"}, {Field: -3, Meadow: "keukenhof gardens"}})
	assert.Equal(t, []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}, {}, {Field: 1}, {Meadow: "Paradise"}}, testSlice)
	testFlag.SetTo(newSlice)
	assert.Equal(t, []testStruct{{}, {Field: 1}, {Meadow: "Paradise"}, {}, {Field: 1}, {Meadow: "Paradise"}, {Field: -1, Meadow: "sunflower fields"}, {Field: -3, Meadow: "keukenhof gardens"}}, testSlice)
	assert.Equal(t, 8, testFlag.Len())
}
