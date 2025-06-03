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
		Input    set.Set[string]
		Conjunct set.Set[string]
		Expected []string
	}{
		{
			Name:     "Empty intersects empty",
			Input:    make(set.Set[string], 0),
			Conjunct: make(set.Set[string], 0),
			Expected: []string{},
		},
		{
			Name:     "Nil intersects nil",
			Input:    nil,
			Conjunct: nil,
			Expected: []string{},
		},
		{
			Name:     "Empty intersects nil",
			Input:    make(set.Set[string], 0),
			Conjunct: nil,
			Expected: []string{},
		},
		{
			Name:     "Nil intersects empty",
			Input:    nil,
			Conjunct: make(set.Set[string], 0),
			Expected: []string{},
		},
		{
			Name:  "Empty intersects non-empty",
			Input: make(set.Set[string], 0),
			Conjunct: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Expected: []string{},
		},
		{
			Name: "Non-empty intersects empty",
			Input: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Conjunct: make(set.Set[string], 0),
			Expected: []string{},
		},
		{
			Name:  "Nil intersects non-empty",
			Input: nil,
			Conjunct: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Expected: []string{},
		},
		{
			Name: "Non-empty intersects nil",
			Input: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Conjunct: nil,
			Expected: []string{},
		},
		{
			Name: "Intersect with overlap",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Conjunct: set.Set[string](map[string]struct{}{
				"bar":    {},
				"barfoo": {},
				"barbar": {},
			}),
			Expected: []string{
				"bar",
				"barfoo",
			},
		},
		{
			Name: "Intersect with no overlap",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barfoo": {},
			}),
			Conjunct: set.Set[string](map[string]struct{}{
				"bar":    {},
				"barbar": {},
			}),
			Expected: []string{},
		},
		{
			Name: "Intersect with zero-value overlap",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barfoo": {},
			}),
			Conjunct: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"barbar": {},
			}),
			Expected: []string{
				"",
			},
		},
		{
			Name: "Intersect with zero-value no overlap",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barfoo": {},
			}),
			Conjunct: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barbar": {},
			}),
			Expected: []string{
				"foo",
			},
		},
		{
			Name: "Intersect with zero-value no overlap 2",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barfoo": {},
			}),
			Conjunct: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barbar": {},
			}),
			Expected: []string{
				"foo",
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			s := tc.Input.Intersection(tc.Conjunct)
			assert.ElementsMatch(t, slices.Collect(s), tc.Expected)
		})
	}
}

func TestUnion(t *testing.T) {
	for _, tc := range []struct {
		Name     string
		Input    set.Set[string]
		Disjunct set.Set[string]
		Expected []string
	}{
		{
			Name:     "Empty union empty",
			Input:    make(set.Set[string], 0),
			Disjunct: make(set.Set[string], 0),
			Expected: []string{},
		},
		{
			Name:     "Nil union nil",
			Input:    nil,
			Disjunct: nil,
			Expected: []string{},
		},
		{
			Name:     "Empty union nil",
			Input:    make(set.Set[string], 0),
			Disjunct: nil,
			Expected: []string{},
		},
		{
			Name:     "Nil union empty",
			Input:    nil,
			Disjunct: make(set.Set[string], 0),
			Expected: []string{},
		},
		{
			Name:  "Empty union non-empty",
			Input: make(set.Set[string], 0),
			Disjunct: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Non-empty union empty",
			Input: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Disjunct: make(set.Set[string], 0),
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name:  "Nil union non-empty",
			Input: nil,
			Disjunct: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Non-empty union nil",
			Input: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Disjunct: nil,
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Union with overlap",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Disjunct: set.Set[string](map[string]struct{}{
				"bar":    {},
				"barfoo": {},
				"barbar": {},
			}),
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
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barfoo": {},
			}),
			Disjunct: set.Set[string](map[string]struct{}{
				"bar":    {},
				"barbar": {},
			}),
			Expected: []string{
				"foo",
				"bar",
				"barfoo",
				"barbar",
			},
		},
		{
			Name: "Union with zero-value overlap",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barfoo": {},
			}),
			Disjunct: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"barbar": {},
			}),
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
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barfoo": {},
			}),
			Disjunct: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barbar": {},
			}),
			Expected: []string{
				"",
				"foo",
				"barfoo",
				"barbar",
			},
		},
		{
			Name: "Union with zero-value no overlap 2",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barfoo": {},
			}),
			Disjunct: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barbar": {},
			}),
			Expected: []string{
				"",
				"foo",
				"barfoo",
				"barbar",
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			s := tc.Input.Union(tc.Disjunct)
			assert.ElementsMatch(t, slices.Collect(s), tc.Expected)
		})
	}
}

func TestDifference(t *testing.T) {
	for _, tc := range []struct {
		Name       string
		Input      set.Set[string]
		Subtrahend set.Set[string]
		Expected   []string
	}{
		{
			Name:       "Empty subtract empty",
			Input:      make(set.Set[string], 0),
			Subtrahend: make(set.Set[string], 0),
			Expected:   []string{},
		},
		{
			Name:       "Nil subtract nil",
			Input:      nil,
			Subtrahend: nil,
			Expected:   []string{},
		},
		{
			Name:       "Empty subtract nil",
			Input:      make(set.Set[string], 0),
			Subtrahend: nil,
			Expected:   []string{},
		},
		{
			Name:       "Nil subtract empty",
			Input:      nil,
			Subtrahend: make(set.Set[string], 0),
			Expected:   []string{},
		},
		{
			Name:  "Empty subtract non-empty",
			Input: make(set.Set[string], 0),
			Subtrahend: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Expected: []string{},
		},
		{
			Name: "Non-empty subtract empty",
			Input: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Subtrahend: make(set.Set[string], 0),
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name:  "Nil subtract non-empty",
			Input: nil,
			Subtrahend: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Expected: []string{},
		},
		{
			Name: "Non-empty subtract nil",
			Input: set.Set[string]{
				"foo":    {},
				"bar":    {},
				"foobar": {},
			},
			Subtrahend: nil,
			Expected: []string{
				"foo",
				"bar",
				"foobar",
			},
		},
		{
			Name: "Subtract with overlap",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Subtrahend: set.Set[string](map[string]struct{}{
				"bar":    {},
				"barfoo": {},
				"barbar": {},
			}),
			Expected: []string{
				"foo",
				"foobar",
			},
		},
		{
			Name: "Subtract with no overlap",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barfoo": {},
			}),
			Subtrahend: set.Set[string](map[string]struct{}{
				"bar":    {},
				"barbar": {},
			}),
			Expected: []string{
				"foo",
				"barfoo",
			},
		},
		{
			Name: "Subtract with zero-value overlap",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barfoo": {},
			}),
			Subtrahend: set.Set[string](map[string]struct{}{
				"":       {},
				"bar":    {},
				"barbar": {},
			}),
			Expected: []string{
				"foo",
				"barfoo",
			},
		},
		{
			Name: "Subtract with zero-value no overlap",
			Input: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barfoo": {},
			}),
			Subtrahend: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barbar": {},
			}),
			Expected: []string{
				"",
				"barfoo",
			},
		},
		{
			Name: "Subtract with zero-value no overlap 2",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"barfoo": {},
			}),
			Subtrahend: set.Set[string](map[string]struct{}{
				"":       {},
				"foo":    {},
				"barbar": {},
			}),
			Expected: []string{
				"barfoo",
			},
		},
		{
			Name: "Subtract all",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Subtrahend: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Expected: []string{},
		},
		{
			Name: "Subtract more than all",
			Input: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
			}),
			Subtrahend: set.Set[string](map[string]struct{}{
				"foo":    {},
				"bar":    {},
				"foobar": {},
				"barfoo": {},
				"barbar": {},
			}),
			Expected: []string{},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			s := tc.Input.Difference(tc.Subtrahend)
			assert.ElementsMatch(t, slices.Collect(s), tc.Expected)
		})
	}
}
