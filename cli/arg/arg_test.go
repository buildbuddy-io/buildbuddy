package arg

import (
	"fmt"

	"github.com/stretchr/testify/assert"
)

import "testing"

func TestFindLast(t *testing.T) {
	repr := func(val string, idx, length int) string {
		return fmt.Sprintf("val=%q, idx=%d, len=%d", val, idx, length)
	}

	for _, tc := range []struct {
		Args                          []string
		ExpectedValue                 string
		ExpectedIndex, ExpectedLength int
	}{
		{
			[]string{"--foo=bar"},
			"bar", 0, 1,
		},
		{
			[]string{"--foo", "bar"},
			"bar", 0, 2,
		},
		{
			[]string{"--foo=bar", "--foo=baz"},
			"baz", 1, 1,
		},
		{
			[]string{"--foo=bar", "--foo", "baz"},
			"baz", 1, 2,
		},
	} {
		val, idx, length := FindLast(tc.Args, "foo")
		assert.Equal(
			t,
			repr(tc.ExpectedValue, tc.ExpectedIndex, tc.ExpectedLength),
			repr(val, idx, length),
			"incorrect return value for %s", tc.Args,
		)
	}
}
