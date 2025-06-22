package seq_test

import (
	"iter"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/lib/seq"
	"github.com/stretchr/testify/assert"
)

func TestSequence(t *testing.T) {
	input := []int{0, 3, 5, 2, 7}
	originalInput := slices.Clone(input)
	expected := slices.Clone(input)

	s := seq.Sequence[int](input)
	assert.ElementsMatch(t, expected, slices.Collect(s))
	assert.ElementsMatch(t, originalInput, input)
	// test statelessness
	assert.ElementsMatch(t, expected, slices.Collect(s))

	s = seq.Sequence[int](slices.Values(input))
	assert.ElementsMatch(t, expected, slices.Collect(s))
	// test statelessness
	assert.ElementsMatch(t, expected, slices.Collect(s))

	s = seq.Sequence[int](((iter.Seq[int])(nil)))
	assert.ElementsMatch(t, make([]int, 0), slices.Collect(s))
	// test statelessness
	assert.ElementsMatch(t, make([]int, 0), slices.Collect(s))

	s = seq.Sequence[int]((([]int)(nil)))
	assert.ElementsMatch(t, make([]int, 0), slices.Collect(s))
	// test statelessness
	assert.ElementsMatch(t, make([]int, 0), slices.Collect(s))
}

func TestChain(t *testing.T) {
	for name, tc := range map[string]struct {
		s1 []string
		s2 []string
	}{
		"chain empty to empty": {
			s1: make([]string, 0),
			s2: make([]string, 0),
		},
		"chain empty to nil": {
			s1: make([]string, 0),
			s2: nil,
		},
		"chain nil to empty": {
			s1: nil,
			s2: make([]string, 0),
		},
		"chain nil to nil": {
			s1: nil,
			s2: nil,
		},
		"chain empty to non-empty": {
			s1: make([]string, 0),
			s2: []string{"foo", "bar", "foobar"},
		},
		"chain nil to non-empty": {
			s1: nil,
			s2: []string{"foo", "bar", "foobar"},
		},
		"chain non-empty to empty": {
			s1: []string{"barfoo", "barbar", "foofoo"},
			s2: make([]string, 0),
		},
		"chain non-empty to nil": {
			s1: []string{"barfoo", "barbar", "foofoo"},
			s2: nil,
		},
		"chain non-empty to non-empty": {
			s1: []string{"barfoo", "barbar", "foofoo"},
			s2: []string{"foo", "bar", "foobar"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalS1 := slices.Clone(tc.s1)
			originalS2 := slices.Clone(tc.s2)

			chained := seq.Chain[string](tc.s1, tc.s2)
			assert.ElementsMatch(t, append(tc.s1, tc.s2...), slices.Collect(chained))
			assert.ElementsMatch(t, originalS1, tc.s1)
			assert.ElementsMatch(t, originalS2, tc.s2)
			// test statelessness
			assert.ElementsMatch(t, append(tc.s1, tc.s2...), slices.Collect(chained))

			chained = seq.Chain[string](tc.s1, slices.Values(tc.s2))
			assert.ElementsMatch(t, append(tc.s1, tc.s2...), slices.Collect(chained))
			assert.ElementsMatch(t, originalS1, tc.s1)
			assert.ElementsMatch(t, originalS2, tc.s2)
			// test statelessness
			assert.ElementsMatch(t, append(tc.s1, tc.s2...), slices.Collect(chained))

			chained = seq.Chain[string](slices.Values(tc.s1), tc.s2)
			assert.ElementsMatch(t, append(tc.s1, tc.s2...), slices.Collect(chained))
			assert.ElementsMatch(t, originalS1, tc.s1)
			assert.ElementsMatch(t, originalS2, tc.s2)
			// test statelessness
			assert.ElementsMatch(t, append(tc.s1, tc.s2...), slices.Collect(chained))

			chained = seq.Chain[string](slices.Values(tc.s1), slices.Values(tc.s2))
			assert.ElementsMatch(t, append(tc.s1, tc.s2...), slices.Collect(chained))
			assert.ElementsMatch(t, originalS1, tc.s1)
			assert.ElementsMatch(t, originalS2, tc.s2)
			// test statelessness
			assert.ElementsMatch(t, append(tc.s1, tc.s2...), slices.Collect(chained))
		})
	}
}

func TestFmap(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []string
		f        func(string) string
		expected []string
	}{
		"empty": {
			input:    make([]string, 0),
			f:        func(string) string { t.FailNow(); return "" },
			expected: []string{},
		},
		"nil": {
			input:    make([]string, 0),
			f:        func(string) string { t.FailNow(); return "" },
			expected: []string{},
		},
		"string to string": {
			input:    []string{"foo", "bar", "foofoo"},
			f:        func(s string) string { return s + "foo" },
			expected: []string{"foofoo", "barfoo", "foofoofoo"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)

			mapped := seq.Fmap(tc.input, tc.f)
			assert.ElementsMatch(t, slices.Collect(mapped), tc.expected)
			assert.ElementsMatch(t, originalInput, tc.input)
			// test statelessness
			assert.ElementsMatch(t, slices.Collect(mapped), tc.expected)
		})
	}
	t.Run("int to string", func(t *testing.T) {
		input := []int{2, 7, 1, 8, 2, 8}
		originalInput := slices.Clone(input)
		expected := []string{"2", "7", "1", "8", "2", "8"}

		mapped := seq.Fmap(input, strconv.Itoa)
		assert.ElementsMatch(t, expected, slices.Collect(mapped))
		assert.ElementsMatch(t, originalInput, input)
		// test statelessness
		assert.ElementsMatch(t, expected, slices.Collect(mapped))

		mapped = seq.Fmap(slices.Values(input), strconv.Itoa)
		assert.ElementsMatch(t, expected, slices.Collect(mapped))
		// test statelessness
		assert.ElementsMatch(t, expected, slices.Collect(mapped))
	})
}

func TestDrop(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []string
		n        int
		expected []string
	}{
		"empty with zero": {
			input:    make([]string, 0),
			n:        0,
			expected: make([]string, 0),
		},
		"nil with zero": {
			input:    nil,
			n:        0,
			expected: make([]string, 0),
		},
		"empty with non-zero": {
			input:    make([]string, 0),
			n:        10,
			expected: make([]string, 0),
		},
		"nil with non-zero": {
			input:    nil,
			n:        10,
			expected: make([]string, 0),
		},
		"non-empty with zero": {
			input: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
			n: 0,
			expected: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
		},
		"non-empty with n less than length": {
			input: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
			n: 2,
			expected: []string{
				"foobar",
				"barfoo",
			},
		},
		"non-empty with n equal to length": {
			input: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
			n:        4,
			expected: make([]string, 0),
		},
		"non-empty with n greater than length": {
			n: 100,
			input: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
			expected: make([]string, 0),
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)
			dropped := seq.Drop[string](tc.input, tc.n)
			assert.ElementsMatch(t, tc.expected, slices.Collect(dropped))
			assert.ElementsMatch(t, originalInput, tc.input)
			// test statelessness
			assert.ElementsMatch(t, tc.expected, slices.Collect(dropped))

			dropped = seq.Drop[string](slices.Values(tc.input), tc.n)
			assert.ElementsMatch(t, tc.expected, slices.Collect(dropped))
			// test statelessness
			assert.ElementsMatch(t, tc.expected, slices.Collect(dropped))
		})
	}
}

func TestTake(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []string
		n        int
		expected []string
	}{
		"empty with zero": {
			input:    make([]string, 0),
			n:        0,
			expected: make([]string, 0),
		},
		"nil with zero": {
			input:    nil,
			n:        0,
			expected: make([]string, 0),
		},
		"empty with non-zero": {
			input:    make([]string, 0),
			n:        10,
			expected: make([]string, 0),
		},
		"nil with non-zero": {
			input:    nil,
			n:        10,
			expected: make([]string, 0),
		},
		"non-empty with zero": {
			input: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
			n:        0,
			expected: make([]string, 0),
		},
		"non-empty with n less than length": {
			input: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
			n: 2,
			expected: []string{
				"foo",
				"bar",
			},
		},
		"non-empty with n equal to length": {
			input: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
			n: 4,
			expected: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
		},
		"non-empty with n greater than length": {
			n: 100,
			input: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
			expected: []string{
				"foo",
				"bar",
				"foobar",
				"barfoo",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)
			taken := seq.Take[string](tc.input, tc.n)
			assert.ElementsMatch(t, tc.expected, slices.Collect(taken))
			assert.ElementsMatch(t, originalInput, tc.input)
			// test statelessness
			assert.ElementsMatch(t, tc.expected, slices.Collect(taken))

			taken = seq.Take[string](slices.Values(tc.input), tc.n)
			assert.ElementsMatch(t, tc.expected, slices.Collect(taken))
			// test statelessness
			assert.ElementsMatch(t, tc.expected, slices.Collect(taken))
		})
	}
	t.Run("stateful truncate", func(t *testing.T) {
		input := "hello\nthere\ncool\nworld\n"
		// string.Lines is a stateful sequence.
		lines := strings.Lines(input)
		taken := seq.Take[string](lines, 2)
		assert.ElementsMatch(
			t,
			[]string{"hello\n", "there\n"},
			slices.Collect(taken),
		)

		assert.ElementsMatch(
			t,
			[]string{"cool\n", "world\n"},
			slices.Collect(lines),
		)
	})
}

func TestRepeat(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []string
		expected []string
	}{
		"one element": {
			input: []string{"foo"},
			expected: []string{
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
			},
		},
		"two elements": {
			input: []string{"foo", "bar"},
			expected: []string{
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
				"foo", "bar",
			},
		},
		"three elements": {
			input: []string{"foo", "bar", "foo"},
			expected: []string{
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo", "bar", "foo",
				"foo",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)
			repeated := seq.Repeat[string](tc.input)
			i := 0
			for e := range repeated {
				if i == len(tc.expected) {
					break
				}
				assert.Equalf(t, tc.expected[i], e, "at element %d, '%s' was expected, but '%s' was provided.", i, tc.expected[i], e)
				i++
			}
			assert.Equalf(t, len(tc.expected), i, "Sequence should repeat forever, but was instead %d elements in length", i)
			assert.ElementsMatch(t, originalInput, tc.input)
			// test statelessness
			i = 0
			for e := range repeated {
				if i == len(tc.expected) {
					break
				}
				assert.Equalf(t, tc.expected[i], e, "at element %d, '%s' was expected, but '%s' was provided.", i, tc.expected[i], e)
				i++
			}
			assert.Equalf(t, len(tc.expected), i, "Sequence should repeat forever, but was instead %d elements in length", i)

			repeated = seq.Repeat[string](slices.Values(tc.input))
			i = 0
			for e := range repeated {
				if i == len(tc.expected) {
					break
				}
				assert.Equalf(t, tc.expected[i], e, "at element %d, '%s' was expected, but '%s' was provided.", i, tc.expected[i], e)
				i++
			}
			assert.Equalf(t, len(tc.expected), i, "Sequence should repeat forever, but was instead %d elements in length", i)
			// test statelessness
			i = 0
			for e := range repeated {
				if i == len(tc.expected) {
					break
				}
				assert.Equalf(t, tc.expected[i], e, "at element %d, '%s' was expected, but '%s' was provided.", i, tc.expected[i], e)
				i++
			}
			assert.Equalf(t, len(tc.expected), i, "Sequence should repeat forever, but was instead %d elements in length", i)
		})
	}
	t.Run("zero-length", func(t *testing.T) {
		input := make([]string, 0)
		originalInput := slices.Clone(input)
		repeated := seq.Repeat[string](input)
		for e := range repeated {
			assert.FailNowf(t, "sequence should be zero-length, but instead contained element '%s'.", e)
		}
		assert.ElementsMatch(t, originalInput, input)
		// test statelessness
		for e := range repeated {
			assert.FailNowf(t, "sequence should be zero-length, but instead contained element '%s'.", e)
		}

		repeated = seq.Repeat[string](slices.Values(input))
		for e := range repeated {
			assert.FailNowf(t, "sequence should be zero-length, but instead contained element '%s'.", e)
		}
		// test statelessness
		for e := range repeated {
			assert.FailNowf(t, "sequence should be zero-length, but instead contained element '%s'.", e)
		}

		input = []string(nil)
		originalInput = slices.Clone(input)
		repeated = seq.Repeat[string](input)
		for e := range repeated {
			assert.FailNowf(t, "sequence should be zero-length, but instead contained element '%s'.", e)
		}
		assert.ElementsMatch(t, originalInput, input)
		// test statelessness
		for e := range repeated {
			assert.FailNowf(t, "sequence should be zero-length, but instead contained element '%s'.", e)
		}

		repeated = seq.Repeat[string](slices.Values(input))
		for e := range repeated {
			assert.FailNowf(t, "sequence should be zero-length, but instead contained element '%s'.", e)
		}
		// test statelessness
		for e := range repeated {
			assert.FailNowf(t, "sequence should be zero-length, but instead contained element '%s'.", e)
		}
	})
}

func TestRepeatN(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []string
		n        int
		expected []string
	}{
		"empty 0 times": {
			input:    make([]string, 0),
			n:        0,
			expected: make([]string, 0),
		},
		"empty 1 time": {
			input:    make([]string, 0),
			n:        1,
			expected: make([]string, 0),
		},
		"empty 2 times": {
			input:    make([]string, 0),
			n:        2,
			expected: make([]string, 0),
		},
		"empty 10 times": {
			input:    make([]string, 0),
			n:        10,
			expected: make([]string, 0),
		},
		"nil 0 times": {
			input:    nil,
			n:        0,
			expected: make([]string, 0),
		},
		"nil 1 time": {
			input:    nil,
			n:        1,
			expected: make([]string, 0),
		},
		"nil 2 times": {
			input:    nil,
			n:        2,
			expected: make([]string, 0),
		},
		"nil 10 times": {
			input:    nil,
			n:        10,
			expected: make([]string, 0),
		},
		"one element 0 times": {
			input:    []string{"foo"},
			n:        0,
			expected: make([]string, 0),
		},
		"one element 1 time": {
			input: []string{"foo"},
			n:     1,
			expected: []string{
				"foo",
			},
		},
		"one element 2 times": {
			input: []string{"foo"},
			n:     2,
			expected: []string{
				"foo",
				"foo",
			},
		},
		"one element 10 times": {
			input: []string{"foo"},
			n:     10,
			expected: []string{
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
				"foo",
			},
		},
		"five elements 0 times": {
			input:    []string{"foo", "bar", "foo", "barfoo", "foofoo"},
			n:        0,
			expected: make([]string, 0),
		},
		"five elements 1 time": {
			input: []string{"foo", "bar", "foo", "barfoo", "foofoo"},
			n:     1,
			expected: []string{
				"foo", "bar", "foo", "barfoo", "foofoo",
			},
		},
		"five elements 2 times": {
			input: []string{"foo", "bar", "foo", "barfoo", "foofoo"},
			n:     2,
			expected: []string{
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
			},
		},
		"five elements 10 times": {
			input: []string{"foo", "bar", "foo", "barfoo", "foofoo"},
			n:     10,
			expected: []string{
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
				"foo", "bar", "foo", "barfoo", "foofoo",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)
			repeated := seq.RepeatN[string](tc.input, tc.n)
			assert.ElementsMatch(t, tc.expected, slices.Collect(repeated))
			assert.ElementsMatch(t, originalInput, tc.input)
			// test statelessness
			assert.ElementsMatch(t, tc.expected, slices.Collect(repeated))

			repeated = seq.RepeatN[string](slices.Values(tc.input), tc.n)
			assert.ElementsMatch(t, tc.expected, slices.Collect(repeated))
			// test statelessness
			assert.ElementsMatch(t, tc.expected, slices.Collect(repeated))
		})
	}
}

func TestAccumulate(t *testing.T) {
	for name, tc := range map[string]struct {
		init     string
		input    []string
		f        func(string, string) string
		expected string
	}{
		"empty": {
			input:    make([]string, 0),
			f:        func(string, string) string { panic("Shouldn't run") },
			expected: "",
		},
		"nil": {
			input:    make([]string, 0),
			f:        func(string, string) string { panic("Shouldn't run") },
			expected: "",
		},
		"empty with initial value": {
			init:     "foo",
			input:    make([]string, 0),
			f:        func(string, string) string { panic("Shouldn't run") },
			expected: "foo",
		},
		"nil with initial value": {
			init:     "foo",
			input:    make([]string, 0),
			f:        func(string, string) string { panic("Shouldn't run") },
			expected: "foo",
		},
		"string to string": {
			input:    []string{"foo", "bar", "foofoo"},
			f:        func(s1 string, s2 string) string { return s1 + "hi" + s2 },
			expected: "hifoohibarhifoofoo",
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)

			accumulated := seq.Accumulate(tc.input, tc.init, tc.f)
			assert.Equal(t, tc.expected, accumulated)
			assert.Equal(t, tc.input, originalInput)

			accumulated = seq.Accumulate(slices.Values(tc.input), tc.init, tc.f)
			assert.Equal(t, tc.expected, accumulated)
			assert.Equal(t, tc.input, originalInput)
		})
	}
	t.Run("int to string", func(t *testing.T) {
		input := []int{2, 7, 1, 8, 2, 8}
		originalInput := slices.Clone(input)
		expected := "foo 271828"

		accumulated := seq.Accumulate(
			input,
			"foo ",
			func(s string, i int) string {
				return s + strconv.Itoa(i)
			},
		)
		assert.Equal(t, expected, accumulated)
		assert.Equal(t, input, originalInput)

		accumulated = seq.Accumulate(
			slices.Values(input),
			"foo ",
			func(s string, i int) string {
				return s + strconv.Itoa(i)
			},
		)
		assert.Equal(t, expected, accumulated)
	})
}

func TestSum(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []string
		f        func(string, string) string
		expected string
	}{
		"empty": {
			input:    make([]string, 0),
			f:        func(string, string) string { panic("Shouldn't run") },
			expected: "",
		},
		"nil": {
			input:    make([]string, 0),
			f:        func(string, string) string { panic("Shouldn't run") },
			expected: "",
		},
		"one value": {
			input:    []string{"foo"},
			f:        func(string, string) string { panic("Shouldn't run") },
			expected: "foo",
		},
		"several string values": {
			input:    []string{"foo", "bar", "foofoo"},
			f:        func(s1 string, s2 string) string { return s1 + "+" + s2 },
			expected: "foo+bar+foofoo",
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)

			sum := seq.Sum(tc.input, tc.f)
			assert.Equal(t, tc.expected, sum)
			assert.Equal(t, tc.input, originalInput)

			sum = seq.Sum(slices.Values(tc.input), tc.f)
			assert.Equal(t, tc.expected, sum)
			assert.Equal(t, tc.input, originalInput)
		})
	}
	t.Run("several int values", func(t *testing.T) {
		input := []int{2, 7, 1, 8, 2, 8}
		originalInput := slices.Clone(input)
		expected := 28

		accumulated := seq.Sum(
			input,
			func(i1, i2 int) int {
				return i1 + i2
			},
		)
		assert.Equal(t, expected, accumulated)
		assert.Equal(t, input, originalInput)

		accumulated = seq.Sum(
			slices.Values(input),
			func(i1, i2 int) int {
				return i1 + i2
			},
		)
		assert.Equal(t, expected, accumulated)
	})
}

func TestAny(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []int
		expected bool
	}{
		"empty": {
			input:    make([]int, 0),
			expected: false,
		},
		"nil": {
			input:    nil,
			expected: false,
		},
		"none": {
			input:    []int{0, 0, 0},
			expected: false,
		},
		"all": {
			input:    []int{1, 1, 1},
			expected: true,
		},
		"first": {
			input:    []int{1, 0, 0},
			expected: true,
		},
		"middle": {
			input:    []int{0, 1, 0},
			expected: true,
		},
		"last": {
			input:    []int{0, 0, 1},
			expected: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)
			result := seq.Any(tc.input, func(e int) bool { return e == 1 })
			assert.Equal(t, tc.expected, result)
			assert.Equal(t, originalInput, tc.input)

			result = seq.Any(slices.Values(tc.input), func(e int) bool { return e == 1 })
			assert.Equal(t, tc.expected, result)
		})
	}
	t.Run("infinite", func(t *testing.T) {
		input := seq.Repeat[int]([]int{0, 0, 1})
		expected := true
		result := seq.Any(input, func(e int) bool { return e == 1 })
		assert.Equal(t, expected, result)
	})
}

func TestAll(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []int
		expected bool
	}{
		"empty": {
			input:    make([]int, 0),
			expected: true,
		},
		"nil": {
			input:    nil,
			expected: true,
		},
		"none": {
			input:    []int{0, 0, 0},
			expected: false,
		},
		"all": {
			input:    []int{1, 1, 1},
			expected: true,
		},
		"first": {
			input:    []int{1, 0, 0},
			expected: false,
		},
		"middle": {
			input:    []int{0, 1, 0},
			expected: false,
		},
		"last": {
			input:    []int{0, 0, 1},
			expected: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)
			result := seq.All(tc.input, func(e int) bool { return e == 1 })
			assert.Equal(t, tc.expected, result)
			assert.Equal(t, originalInput, tc.input)

			result = seq.All(slices.Values(tc.input), func(e int) bool { return e == 1 })
			assert.Equal(t, tc.expected, result)
		})
	}
	t.Run("infinite", func(t *testing.T) {
		input := seq.Repeat[int]([]int{1, 1, 0})
		expected := false
		result := seq.All(input, func(e int) bool { return e == 1 })
		assert.Equal(t, expected, result)
	})
}

func TestNone(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []int
		expected bool
	}{
		"empty": {
			input:    make([]int, 0),
			expected: true,
		},
		"nil": {
			input:    nil,
			expected: true,
		},
		"none": {
			input:    []int{0, 0, 0},
			expected: true,
		},
		"all": {
			input:    []int{1, 1, 1},
			expected: false,
		},
		"first": {
			input:    []int{1, 0, 0},
			expected: false,
		},
		"middle": {
			input:    []int{0, 1, 0},
			expected: false,
		},
		"last": {
			input:    []int{0, 0, 1},
			expected: false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)
			result := seq.None(tc.input, func(e int) bool { return e == 1 })
			assert.Equal(t, tc.expected, result)
			assert.Equal(t, originalInput, tc.input)

			result = seq.None(slices.Values(tc.input), func(e int) bool { return e == 1 })
			assert.Equal(t, tc.expected, result)
		})
	}
	t.Run("infinite", func(t *testing.T) {
		input := seq.Repeat[int]([]int{0, 0, 1})
		expected := false
		result := seq.None(input, func(e int) bool { return e == 1 })
		assert.Equal(t, expected, result)
	})
}

func TestComposeFilters(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []int
		filters  []func(int) bool
		expected []bool
	}{
		"empty filters": {
			input:    []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters:  []func(int) bool{},
			expected: []bool{true, true, true, true, true, true, true, true, true, true},
		},
		"nil filters": {
			input:    []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters:  nil,
			expected: []bool{true, true, true, true, true, true, true, true, true, true},
		},
		"even filter": {
			input: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters: []func(int) bool{
				func(i int) bool { return i%2 == 0 },
			},
			expected: []bool{true, false, true, false, true, false, true, false, true, false},
		},
		"odd filter": {
			input: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters: []func(int) bool{
				func(i int) bool { return i%2 != 0 },
			},
			expected: []bool{false, true, false, true, false, true, false, true, false, true},
		},
		"even and odd filters": {
			input: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters: []func(int) bool{
				func(i int) bool { return i%2 == 0 },
				func(i int) bool { return i%2 != 0 },
			},
			expected: []bool{false, false, false, false, false, false, false, false, false, false},
		},
		"multiple of 3 filter": {
			input: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters: []func(int) bool{
				func(i int) bool { return i%3 == 0 },
			},
			expected: []bool{true, false, false, true, false, false, true, false, false, true},
		},
		"greater than 5 filter": {
			input: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters: []func(int) bool{
				func(i int) bool { return i > 5 },
			},
			expected: []bool{false, false, false, false, false, false, true, true, true, true},
		},
		"odd and multiple of 3 filter": {
			input: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters: []func(int) bool{
				func(i int) bool { return i%3 == 0 },
				func(i int) bool { return i%2 != 0 },
			},
			expected: []bool{false, false, false, true, false, false, false, false, false, true},
		},
		"even and multiple of 3 filter": {
			input: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters: []func(int) bool{
				func(i int) bool { return i%3 == 0 },
				func(i int) bool { return i%2 == 0 },
			},
			expected: []bool{true, false, false, false, false, false, true, false, false, false},
		},
		"even, multiple of 3, and greater than 5 filter": {
			input: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filters: []func(int) bool{
				func(i int) bool { return i%3 == 0 },
				func(i int) bool { return i%2 == 0 },
				func(i int) bool { return i > 5 },
			},
			expected: []bool{false, false, false, false, false, false, true, false, false, false},
		},
	} {
		t.Run(name, func(t *testing.T) {
			filter := seq.ComposeFilters(tc.filters...)
			for i := range len(tc.input) {
				assert.Equal(t, tc.expected[i], filter(tc.input[i]))
			}
		})
	}
}

func TestFilter(t *testing.T) {
	for name, tc := range map[string]struct {
		input    []int
		filter   func(int) bool
		expected []int
	}{
		"empty": {
			input:    make([]int, 0),
			filter:   func(int) bool { t.FailNow(); return false },
			expected: make([]int, 0),
		},
		"nil": {
			input:    nil,
			filter:   func(int) bool { t.FailNow(); return false },
			expected: make([]int, 0),
		},
		"sequential values nil filter": {
			input:    []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filter:   nil,
			expected: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		"sequential values even filter": {
			input:    []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filter:   func(e int) bool { return e%2 == 0 },
			expected: []int{0, 2, 4, 6, 8},
		},
		"sequential values odd filter": {
			input:    []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			filter:   func(e int) bool { return e%2 != 0 },
			expected: []int{1, 3, 5, 7, 9},
		},
		"arbitrary values nil filter": {
			input:    []int{5000, 75, 909, 444, 90, -5, 812734, 812734, 0, 256},
			filter:   nil,
			expected: []int{5000, 75, 909, 444, 90, -5, 812734, 812734, 0, 256},
		},
		"arbitrary values even filter": {
			input:    []int{5000, 75, 909, 444, 90, -5, 812734, 812734, 0, 256},
			filter:   func(e int) bool { return e%2 == 0 },
			expected: []int{5000, 444, 90, 812734, 812734, 0, 256},
		},
		"arbitrary values odd filter": {
			input:    []int{5000, 75, 909, 444, 90, -5, 812734, 812734, 0, 256},
			filter:   func(e int) bool { return e%2 != 0 },
			expected: []int{75, 909, -5},
		},
	} {
		t.Run(name, func(t *testing.T) {
			originalInput := slices.Clone(tc.input)
			filtered := seq.Filter(tc.input, tc.filter)
			assert.ElementsMatch(t, tc.expected, slices.Collect(filtered))
			assert.ElementsMatch(t, originalInput, tc.input)
			// test statelessness
			assert.ElementsMatch(t, tc.expected, slices.Collect(filtered))

			filtered = seq.Filter(slices.Values(tc.input), tc.filter)
			assert.ElementsMatch(t, tc.expected, slices.Collect(filtered))
			// test statelessness
			assert.ElementsMatch(t, tc.expected, slices.Collect(filtered))
		})
	}
}
