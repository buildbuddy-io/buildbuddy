package arg

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestGetMulti(t *testing.T) {
	args := []string{"--build_metadata=COMMIT_SHA=abc123", "--foo", "--build_metadata=ROLE=CI"}

	values := GetMulti(args, "build_metadata")

	assert.Equal(t, []string{"COMMIT_SHA=abc123", "ROLE=CI"}, values)
}

func TestGetTargets(t *testing.T) {
	args := []string{"build", "foo"}
	targets := GetTargets(args)
	assert.Equal(t, []string{"foo"}, targets)

	args = []string{"build", "foo", "bar"}
	targets = GetTargets(args)
	assert.Equal(t, []string{"foo", "bar"}, targets)

	args = []string{"build", "--opt=val", "foo", "bar", "--anotheropt=anotherval"}
	targets = GetTargets(args)
	assert.Equal(t, []string{"foo", "bar"}, targets)

	args = []string{"run", "--opt=val", "foo", "bar", "--", "baz"}
	targets = GetTargets(args)
	assert.Equal(t, []string{"foo", "bar"}, targets)

	// Support subtractive patterns https://bazel.build/run/build#specifying-build-targets
	args = []string{"build", "--opt=val", "--", "foo", "bar", "-baz"}
	targets = GetTargets(args)
	assert.Equal(t, []string{"foo", "bar"}, targets)

	args = []string{"build"}
	targets = GetTargets(args)
	assert.Equal(t, []string{}, targets)

	args = []string{}
	targets = GetTargets(args)
	assert.Equal(t, []string{}, targets)
}
