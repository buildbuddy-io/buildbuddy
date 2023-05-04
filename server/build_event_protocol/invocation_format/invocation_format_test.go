package invocation_format_test

import (
	"testing"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
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

func makeTagSlice(args ...string) []*inpb.Invocation_Tag {
	out := make([]*inpb.Invocation_Tag, len(args))
	for i, tag := range args {
		out[i] = &inpb.Invocation_Tag{Name: tag}
	}
	return out
}

func TestSplitAndTrimTags(t *testing.T) {
	for _, testCase := range []struct {
		input    string
		expected []*inpb.Invocation_Tag
	}{
		{"", makeTagSlice()},
		{",", makeTagSlice()},
		{",  ,,,", makeTagSlice()},
		{"beef", makeTagSlice("beef")},
		{"  beef ", makeTagSlice("beef")},
		{"beef,beer", makeTagSlice("beef", "beer")},
		{" art , beef, beer , cheese..,ten dollars,,", makeTagSlice("art", "beef", "beer", "cheese..", "ten dollars")},
	} {
		assert.Equal(t, testCase.expected, invocation_format.SplitAndTrimTags(testCase.input))
	}
}

func TestJoinTags(t *testing.T) {
	for _, testCase := range []struct {
		input          []*inpb.Invocation_Tag
		expectedOutput string
		expectedError  bool
	}{
		{makeTagSlice(), "", false},
		{makeTagSlice(""), "", false},
		{makeTagSlice("  ", "  "), "", false},
		{makeTagSlice("beef", "cheese..", "ten dollars"), "beef,cheese..,ten dollars", false},
		{makeTagSlice("", "beef ", " ", " cheese..", " ten dollars "), "beef,cheese..,ten dollars", false},
		{makeTagSlice("  ", ","), "", true},
		{makeTagSlice("beef", "cheese,", "ten dollars"), "", true},
	} {
		out, err := invocation_format.JoinTags(testCase.input)
		if testCase.expectedError {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, testCase.expectedOutput, out)
		}
	}
}

func TestConvertDbTagsToOlap(t *testing.T) {
	for _, testCase := range []struct {
		input    string
		expected []string
	}{
		{"beef", []string{"beef"}},
		{"beef, cheese,, ..", []string{"beef", " cheese", "", " .."}},
		{"beef,cheese,ten dollars..,beer", []string{"beef", "cheese", "ten dollars..", "beer"}},
	} {
		assert.Equal(t, testCase.expected, invocation_format.ConvertDbTagsToOlap(testCase.input))
	}
}
