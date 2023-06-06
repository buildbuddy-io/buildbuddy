package invocation_format_test

import (
	"strings"
	"testing"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
	if len(args) == 0 {
		return nil
	}
	out := make([]*inpb.Invocation_Tag, len(args))
	for i, tag := range args {
		out[i] = &inpb.Invocation_Tag{Name: tag}
	}
	return out
}

func TestSplitAndTrimAndDedupeTags(t *testing.T) {
	longTag := "l" + strings.Repeat("o", 158) + "ng"
	lotsOfWhitespace := strings.Repeat(" ", 255)
	tooLong := status.InvalidArgumentError("Tag list is too long.")
	for _, testCase := range []struct {
		input         string
		truncate      bool
		expected      []*inpb.Invocation_Tag
		expectedError error
	}{
		{"", false, makeTagSlice(), nil},
		{",", false, makeTagSlice(), nil},
		{",  ,,,", false, makeTagSlice(), nil},
		{"beef", false, makeTagSlice("beef"), nil},
		{"beef,cheese, beef ", false, makeTagSlice("beef", "cheese"), nil},
		{"beef, beef", false, makeTagSlice("beef"), nil},
		{"  beef ", false, makeTagSlice("beef"), nil},
		{"beef,beer", false, makeTagSlice("beef", "beer"), nil},
		{" art , beef, beer , cheese..,ten dollars,,", false, makeTagSlice("art", "beef", "beer", "cheese..", "ten dollars"), nil},
		{" art , beef, beer , cheese..,ten dollars,,", true, makeTagSlice("art", "beef", "beer", "cheese..", "ten dollars"), nil},
		{longTag + ",short1", false, makeTagSlice(longTag, "short1"), nil},
		{longTag, true, nil, tooLong},
		{longTag + ",short1", true, nil, tooLong},
		{"short1,short2," + longTag + ",short3", true, nil, tooLong},
		{"lots of whitespace" + lotsOfWhitespace + "," + lotsOfWhitespace + "and,more,tags", true, makeTagSlice("lots of whitespace", "and", "more", "tags"), nil},
	} {
		tags, err := invocation_format.SplitAndTrimAndDedupeTags(testCase.input, testCase.truncate)
		assert.Equal(t, testCase.expected, tags)
		if testCase.expectedError == nil {
			assert.Nil(t, err)
		} else {
			assert.EqualError(t, err, testCase.expectedError.Error())
		}
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

func TestConvertDBTagsToOLAP(t *testing.T) {
	for _, testCase := range []struct {
		input    string
		expected []string
	}{
		{"beef", []string{"beef"}},
		{"beef, cheese,, ..", []string{"beef", " cheese", "", " .."}},
		{"beef,cheese,ten dollars..,beer", []string{"beef", "cheese", "ten dollars..", "beer"}},
	} {
		assert.Equal(t, testCase.expected, invocation_format.ConvertDBTagsToOLAP(testCase.input))
	}
}
