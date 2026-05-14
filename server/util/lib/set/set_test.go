package set_test

import (
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestFrom(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Slice    []string
		Expected set.Set[string]
	}{
		{
			Name:     "Empty",
			Slice:    []string{},
			Expected: make(set.Set[string], 0),
		},
		{
			Name:  "Distinct elements",
			Slice: []string{"foo", "bar", "foobar"},
			Expected: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			}),
		},
		{
			Name:  "Repeated elements",
			Slice: []string{"foo", "bar", "foobar", "bar", "foobar", "foobar", "barfoo"},
			Expected: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
		},
		{
			Name:  "Contains zero-value element",
			Slice: []string{"", "bar", "foobar"},
			Expected: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"foobar": {},
			}),
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			s := set.From(tc.Slice...)
			if !cmp.Equal(s, tc.Expected) {
				t.Errorf("Output did not match expectation.\nexpected: (-), actual: (+):\n%s", cmp.Diff(tc.Expected, s))
			}
		})
	}
}

func TestFromSeq(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Slice    []string
		Expected set.Set[string]
	}{
		{
			Name:     "Empty",
			Slice:    []string{},
			Expected: make(set.Set[string], 0),
		},
		{
			Name:  "Distinct elements",
			Slice: []string{"foo", "bar", "foobar"},
			Expected: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			}),
		},
		{
			Name:  "Repeated elements",
			Slice: []string{"foo", "bar", "foobar", "bar", "foobar", "foobar", "barfoo"},
			Expected: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
		},
		{
			Name:  "Contains zero-value element",
			Slice: []string{"", "bar", "foobar"},
			Expected: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"foobar": {},
			}),
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			s := set.FromSeq(slices.Values(tc.Slice))
			if !cmp.Equal(s, tc.Expected) {
				t.Errorf("Output did not match expectation.\nexpected: (-), actual: (+):\n%s", cmp.Diff(tc.Expected, s))
			}
		})
	}
}

func TestAll(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    set.Set[string]
		Expected []string
	}{
		{
			Name:     "Empty",
			Input:    make(set.Set[string], 0),
			Expected: []string{},
		},
		{
			Name: "Four elements",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Expected: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
		},
		{
			Name: "Contains zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"foobar": {},
			}),
			Expected: []string{
				"",
				"bar",
				"foobar",
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			v := slices.Collect(tc.Input.All())
			assert.ElementsMatch(t, v, tc.Expected)
		})
	}
}

func TestAdd(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    set.Set[string]
		ToAdd    string
		Expected set.Set[string]
	}{
		{
			Name:  "Add to Empty",
			Input: make(set.Set[string], 0),
			ToAdd: "foo",
			Expected: set.Set[string]{
				"foo": {},
			},
		},
		{
			Name: "Add distinct elements",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			ToAdd: "barbar",
			Expected: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
				"barbar": {},
			},
		},
		{
			Name: "Add repeated element",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			ToAdd: "foo",
			Expected: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
		},
		{
			Name: "Contains zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"foobar": {},
			}),
			ToAdd: "foo",
			Expected: set.Set[string]{
				"":       {},
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
		},
		{
			Name: "Add zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"bar":    {},
				"foobar": {},
			}),
			ToAdd: "",
			Expected: set.Set[string]{
				"":       {},
				"bar":    {},
				"foobar": {},
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			tc.Input.Add(tc.ToAdd)
			if !cmp.Equal(tc.Input, tc.Expected) {
				t.Errorf("Output did not match expectation.\nexpected: (-), actual: (+):\n%s", cmp.Diff(tc.Expected, tc.Input))
			}
		})
	}
}

func TestAddSeq(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    set.Set[string]
		ToAdd    []string
		Expected set.Set[string]
	}{
		{
			Name:  "Add to Empty",
			Input: make(set.Set[string], 0),
			ToAdd: []string{
				"foo",
			},
			Expected: set.Set[string]{
				"foo": {},
			},
		},
		{
			Name: "Add Empty",
			Input: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
			ToAdd: []string{},
			Expected: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
		},
		{
			Name:     "Add Empty to Empty",
			Input:    make(set.Set[string], 0),
			ToAdd:    []string{},
			Expected: set.Set[string]{},
		},
		{
			Name: "Add distinct elements",
			Input: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
			ToAdd: []string{
				"barbar",
				"foofoo",
			},
			Expected: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"foofoo": {},
				"barfoo": {},
				"barbar": {},
			},
		},
		{
			Name: "Add repeated element",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			ToAdd: []string{
				"foo",
				"bar",
			},
			Expected: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
		},
		{
			Name: "Contains zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"foobar": {},
			}),
			ToAdd: []string{
				"foo",
				"barfoo",
			},
			Expected: set.Set[string]{
				"":       {},
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
		},
		{
			Name: "Add zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"bar":    {},
				"foobar": {},
			}),
			ToAdd: []string{
				"",
				"foobar",
				"barfoo",
			},
			Expected: set.Set[string]{
				"":       {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			tc.Input.AddSeq(slices.Values(tc.ToAdd))
			if !cmp.Equal(tc.Input, tc.Expected) {
				t.Errorf("Output did not match expectation.\nexpected: (-), actual: (+):\n%s", cmp.Diff(tc.Expected, tc.Input))
			}
		})
	}
}

func TestRemove(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    set.Set[string]
		ToRemove string
		Expected set.Set[string]
	}{
		{
			Name:     "Remove from Empty",
			Input:    make(set.Set[string], 0),
			ToRemove: "foo",
			Expected: make(set.Set[string], 0),
		},
		{
			Name:     "Remove from nil",
			Input:    nil,
			ToRemove: "foo",
			Expected: nil,
		},
		{
			Name: "Remove extant element",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			ToRemove: "barfoo",
			Expected: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
		},
		{
			Name: "Remove missing element",
			Input: set.Set[string](map[string]struct{}{
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			ToRemove: "foo",
			Expected: set.Set[string]{
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
		},
		{
			Name: "Contains zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"bar":    {},
				"foobar": {},
			}),
			ToRemove: "foo",
			Expected: set.Set[string]{
				"":       {},
				"bar":    {},
				"foobar": {},
			},
		},
		{
			Name: "Remove zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"foobar": {},
			}),
			ToRemove: "",
			Expected: set.Set[string]{
				"bar":    {},
				"foobar": {},
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			tc.Input.Remove(tc.ToRemove)
			if !cmp.Equal(tc.Input, tc.Expected) {
				t.Errorf("Output did not match expectation.\nexpected: (-), actual: (+):\n%s", cmp.Diff(tc.Expected, tc.Input))
			}
		})
	}
}

func TestRemoveSeq(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    set.Set[string]
		ToRemove []string
		Expected set.Set[string]
	}{
		{
			Name:  "Remove from Empty",
			Input: make(set.Set[string], 0),
			ToRemove: []string{
				"foo",
			},
			Expected: make(set.Set[string], 0),
		},
		{
			Name:  "Remove from nil",
			Input: nil,
			ToRemove: []string{
				"foo",
			},
			Expected: nil,
		},
		{
			Name:     "Remove Empty from Empty",
			Input:    make(set.Set[string], 0),
			ToRemove: []string{},
			Expected: make(set.Set[string], 0),
		},
		{
			Name:     "Remove Empty from nil",
			Input:    nil,
			ToRemove: []string{},
			Expected: nil,
		},
		{
			Name: "Remove empty",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			ToRemove: []string{},
			Expected: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
		},
		{
			Name: "Remove extant elements",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			ToRemove: []string{
				"barfoo",
				"foobar",
			},
			Expected: set.Set[string]{
				"foo": {},
				"bar": {},
			},
		},
		{
			Name: "Remove missing element",
			Input: set.Set[string](map[string]struct{}{
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			ToRemove: []string{
				"foo",
				"bar",
			},
			Expected: set.Set[string]{
				"foobar": {},
				"barfoo": {},
			},
		},
		{
			Name: "Contains zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"bar":    {},
				"foobar": {},
			}),
			ToRemove: []string{
				"foo",
				"bar",
			},
			Expected: set.Set[string]{
				"":       {},
				"foobar": {},
			},
		},
		{
			Name: "Remove zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"foobar": {},
				"barbar": {},
			}),
			ToRemove: []string{
				"",
				"foobar",
			},
			Expected: set.Set[string]{
				"bar":    {},
				"barbar": {},
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			tc.Input.RemoveSeq(slices.Values(tc.ToRemove))
			if !cmp.Equal(tc.Input, tc.Expected) {
				t.Errorf("Output did not match expectation.\nexpected: (-), actual: (+):\n%s", cmp.Diff(tc.Expected, tc.Input))
			}
		})
	}
}

func TestContains(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    set.Set[string]
		Contains string
		Expected bool
	}{
		{
			Name:     "Empty Contains",
			Input:    make(set.Set[string], 0),
			Contains: "foo",
			Expected: false,
		},
		{
			Name:     "Nil Contains",
			Input:    nil,
			Contains: "foo",
			Expected: false,
		},
		{
			Name: "Contains extant element",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Contains: "barfoo",
			Expected: true,
		},
		{
			Name: "Contains missing element",
			Input: set.Set[string](map[string]struct{}{
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Contains: "foo",
			Expected: false,
		},
		{
			Name: "Contains extant zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"bar":    {},
				"foobar": {},
			}),
			Contains: "",
			Expected: true,
		},
		{
			Name: "Contains missing zero-value element",
			Input: set.Set[string](map[string]struct{}{
				"bar":    {},
				"foobar": {},
			}),
			Contains: "",
			Expected: false,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			assert.Equal(t, tc.Input.Contains(tc.Contains), tc.Expected)
		})
	}
}

func TestIntersection(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    []set.Set[string]
		Expected []string
	}{
		{
			Name:     "No sets",
			Input:    make([]set.Set[string], 0),
			Expected: []string{},
		},
		{
			Name: "One set",
			Input: []set.Set[string]{
				{
					"":       {},
					"foo":    {},
					"bar":    {},
					"foobar": {},
					"barfoo": {},
				},
			},
			Expected: []string{
				"",
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
		},
		{
			Name: "Empty intersects empty",
			Input: []set.Set[string]{
				make(set.Set[string], 0),
				make(set.Set[string], 0),
			},
			Expected: []string{},
		},
		{
			Name: "Nil intersects nil",
			Input: []set.Set[string]{nil,
				nil,
			},
			Expected: []string{},
		},
		{
			Name: "Empty intersects nil",
			Input: []set.Set[string]{
				make(set.Set[string], 0),
				nil,
			},
			Expected: []string{},
		},
		{
			Name: "Nil intersects empty",
			Input: []set.Set[string]{
				nil,
				make(set.Set[string], 0),
			},
			Expected: []string{},
		},
		{
			Name: "Empty intersects non-empty",
			Input: []set.Set[string]{
				make(set.Set[string], 0),
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
			},
			Expected: []string{},
		},
		{
			Name: "Non-empty intersects empty",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
				make(set.Set[string], 0),
			},
			Expected: []string{},
		},
		{
			Name: "Nil intersects non-empty",
			Input: []set.Set[string]{
				nil,
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
			},
			Expected: []string{},
		},
		{
			Name: "Non-empty intersects nil",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
				nil,
			},
			Expected: []string{},
		},
		{
			Name: "Intersect with overlap",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
					"barfoo": {},
				},
				{
					"bar":    {},
					"barfoo": {},
					"barbar": {},
				},
			},
			Expected: []string{
				"bar",
				"barfoo",
			},
		},
		{
			Name: "Intersect with no overlap",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"barfoo": {},
				},
				{
					"bar":    {},
					"barbar": {},
				},
			},
			Expected: []string{},
		},
		{
			Name: "Intersect with zero-value overlap",
			Input: []set.Set[string]{
				{
					"":       {},
					"foo":    {},
					"barfoo": {},
				},
				{
					"":       {},
					"bar":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"",
			},
		},
		{
			Name: "Intersect with zero-value no overlap",
			Input: []set.Set[string]{
				{
					"":       {},
					"foo":    {},
					"barfoo": {},
				},
				{
					"foo":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"foo",
			},
		},
		{
			Name: "Intersect with zero-value no overlap 2",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"barfoo": {},
				},
				{
					"":       {},
					"foo":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"foo",
			},
		},
		{
			Name: "Complex Intersect with many sets",
			Input: []set.Set[string]{
				{
					"foo":       {},
					"barfoo":    {},
					"barbar":    {},
					"barbarbar": {},
				},
				{
					"":       {},
					"foo":    {},
					"foobar": {},
					"barbar": {},
				},
				{
					"":       {},
					"foo":    {},
					"foobar": {},
					"barbar": {},
				},
				{
					"":       {},
					"foo":    {},
					"foobar": {},
					"barbar": {},
				},
				{
					"":       {},
					"foo":    {},
					"barbar": {},
					"barfoo": {},
				},
			},
			Expected: []string{
				"foo",
				"barbar",
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			s := set.Intersection(tc.Input...)
			assert.ElementsMatch(t, slices.Collect(s), tc.Expected)
		})
	}
}

func TestUnion(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    []set.Set[string]
		Expected []string
	}{
		{
			Name:     "No sets",
			Input:    make([]set.Set[string], 0),
			Expected: []string{},
		},
		{
			Name: "One set",
			Input: []set.Set[string]{
				{
					"":       {},
					"foo":    {},
					"bar":    {},
					"foobar": {},
					"barfoo": {},
				},
			},
			Expected: []string{
				"",
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
		},
		{
			Name: "Empty union empty",
			Input: []set.Set[string]{
				make(set.Set[string], 0),
				make(set.Set[string], 0),
			},
			Expected: []string{},
		},
		{
			Name: "Nil union nil",
			Input: []set.Set[string]{
				nil,
				nil,
			},
			Expected: []string{},
		},
		{
			Name: "Empty union nil",
			Input: []set.Set[string]{
				make(set.Set[string], 0),
				nil,
			},
			Expected: []string{},
		},
		{
			Name: "Nil union empty",
			Input: []set.Set[string]{
				nil,
				make(set.Set[string], 0),
			},
			Expected: []string{},
		},
		{
			Name: "Empty union non-empty",
			Input: []set.Set[string]{
				make(set.Set[string], 0),
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Non-empty union empty",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
				make(set.Set[string], 0),
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Nil union non-empty",
			Input: []set.Set[string]{
				nil,
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Non-empty union nil",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
				nil,
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Union with overlap",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
					"barfoo": {},
				},
				{
					"bar":    {},
					"barfoo": {},
					"barbar": {},
				},
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
				"barbar",
			},
		},
		{
			Name: "Union with no overlap",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"barfoo": {},
				},
				{
					"bar":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"foo",
				"bar",
				"barfoo",
				"barbar",
			},
		},
		{
			Name: "Union with zero-value overlap",
			Input: []set.Set[string]{
				{
					"":       {},
					"foo":    {},
					"barfoo": {},
				},
				{
					"":       {},
					"bar":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"",
				"foo",
				"bar",
				"barfoo",
				"barbar",
			},
		},
		{
			Name: "Union with zero-value no overlap",
			Input: []set.Set[string]{
				{
					"":       {},
					"foo":    {},
					"barfoo": {},
				},
				{
					"foo":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"",
				"foo",
				"barfoo",
				"barbar",
			},
		},
		{
			Name: "Union with zero-value no overlap 2",
			Input: []set.Set[string]{
				{
					"foo":    {},
					"barfoo": {},
				},
				{
					"":       {},
					"foo":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"",
				"foo",
				"barfoo",
				"barbar",
			},
		},
		{
			Name: "Complex union with many sets",
			Input: []set.Set[string]{
				{
					"foo":       {},
					"barbar":    {},
					"barfoo":    {},
					"barbarbar": {},
				},
				{
					"":       {},
					"foo":    {},
					"foobar": {},
					"barbar": {},
				},
				nil,
				{
					"foo":       {},
					"foobar":    {},
					"barbar":    {},
					"foofoofoo": {},
				},
				{},
				{
					"":          {},
					"foofoo":    {},
					"bar":       {},
					"barbar":    {},
					"foofoofoo": {},
				},
			},
			Expected: []string{
				"",
				"foo",
				"bar",
				"foobar",
				"barfoo",
				"foofoo",
				"barbar",
				"foofoofoo",
				"barbarbar",
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			s := set.Union(tc.Input...)
			assert.ElementsMatch(t, slices.Collect(s), tc.Expected)
		})
	}
}

func TestDifference(t *testing.T) {
	for _, tc := range []struct {
		Name        string
		Minuend     set.Set[string]
		Subtrahends []set.Set[string]
		Expected    []string
	}{
		{
			Name: "No subtrahends",
			Minuend: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Subtrahends: make([]set.Set[string], 0),
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name:    "Empty subtract empty",
			Minuend: make(set.Set[string], 0),
			Subtrahends: []set.Set[string]{
				make(set.Set[string], 0),
			},
			Expected: []string{},
		},
		{
			Name:    "Nil subtract nil",
			Minuend: nil,
			Subtrahends: []set.Set[string]{
				nil,
			},
			Expected: []string{},
		},
		{
			Name:        "Empty subtract nil",
			Minuend:     make(set.Set[string], 0),
			Subtrahends: nil,
			Expected:    []string{},
		},
		{
			Name:    "Nil subtract empty",
			Minuend: nil,
			Subtrahends: []set.Set[string]{
				make(set.Set[string], 0),
			},
			Expected: []string{},
		},
		{
			Name:    "Empty subtract non-empty",
			Minuend: make(set.Set[string], 0),
			Subtrahends: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
			},
			Expected: []string{},
		},
		{
			Name: "Non-empty subtract empty",
			Minuend: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Subtrahends: []set.Set[string]{
				make(set.Set[string], 0),
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name:    "Nil subtract non-empty",
			Minuend: nil,
			Subtrahends: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
				},
			},
			Expected: []string{},
		},
		{
			Name: "Non-empty subtract nil",
			Minuend: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Subtrahends: []set.Set[string]{
				nil,
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Subtract with overlap",
			Minuend: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
			Subtrahends: []set.Set[string]{
				{
					"bar":    {},
					"barfoo": {},
					"barbar": {},
				},
			},
			Expected: []string{
				"foo",
				"foobar",
			},
		},
		{
			Name: "Subtract with no overlap",
			Minuend: set.Set[string]{
				"foo":    {},
				"barfoo": {},
			},
			Subtrahends: []set.Set[string]{
				{
					"bar":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"foo",
				"barfoo",
			},
		},
		{
			Name: "Subtract with zero-value overlap",
			Minuend: set.Set[string]{
				"":       {},
				"foo":    {},
				"barfoo": {},
			},
			Subtrahends: []set.Set[string]{
				{
					"":       {},
					"bar":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"foo",
				"barfoo",
			},
		},
		{
			Name: "Subtract with zero-value no overlap",
			Minuend: set.Set[string]{
				"":       {},
				"foo":    {},
				"barfoo": {},
			},
			Subtrahends: []set.Set[string]{
				{
					"foo":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"",
				"barfoo",
			},
		},
		{
			Name: "Subtract with zero-value no overlap 2",
			Minuend: set.Set[string]{
				"foo":    {},
				"barfoo": {},
			},
			Subtrahends: []set.Set[string]{
				{
					"":       {},
					"foo":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"barfoo",
			},
		},
		{
			Name: "Subtract all",
			Minuend: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
			Subtrahends: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
					"barfoo": {},
				},
			},
			Expected: []string{},
		},
		{
			Name: "Subtract more than all",
			Minuend: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			},
			Subtrahends: []set.Set[string]{
				{
					"foo":    {},
					"bar":    {},
					"foobar": {},
					"barfoo": {},
					"barbar": {},
				},
			},
			Expected: []string{},
		},
		{
			Name: "Complex difference with many sets",
			Minuend: set.Set[string]{
				"":          {},
				"foo":       {},
				"bar":       {},
				"foobar":    {},
				"barfoo":    {},
				"foofoo":    {},
				"barbar":    {},
				"foofoofoo": {},
				"barbarbar": {},
			},
			Subtrahends: []set.Set[string]{
				{
					"foo":       {},
					"barbar":    {},
					"barfoo":    {},
					"barbarbar": {},
				},
				{
					"":       {},
					"foo":    {},
					"barbar": {},
				},
				nil,
				{},
				{
					"":       {},
					"foofoo": {},
					"bar":    {},
					"barbar": {},
				},
			},
			Expected: []string{
				"foobar",
				"foofoofoo",
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			s := set.Difference(tc.Minuend, tc.Subtrahends...)
			assert.ElementsMatch(t, slices.Collect(s), tc.Expected)
		})
	}
}
