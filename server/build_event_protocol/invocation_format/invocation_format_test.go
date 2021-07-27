package invocation_format_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/stretchr/testify/assert"
)

func TestShortFormatPatterns(t *testing.T) {
	for _, testCase := range []struct {
		patterns []string
		expected string
	}{
		{[]string{}, ""},
		{[]string{"//a/very/long/pattern/that/exceeds/target/length/because/it/is/so/long", "//another/decently/long/pattern"}, "//a/very/long/pattern/that/exceeds/target/length/because/it/is/so/long and 1 more"},
		{[]string{"//some", "//short", "//patterns"}, "//some, //short, //patterns"},
		{[]string{"//some", "//short", "//patterns", "//and/a/very/long/pattern/that/will/most/definitely/exceed/target/length"}, "//some, //short, //patterns and 1 more"},
	} {
		assert.Equal(t, testCase.expected, invocation_format.ShortFormatPatterns(testCase.patterns))
	}
}
