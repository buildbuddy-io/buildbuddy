package invocation_format

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/invocation"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	// The character limit for invocation patterns after which we will truncate
	// and show "and <N> more".
	patternCharLimit = 50

	// The separator used to join invocation patterns when displaying them to the
	// user.
	listSeparator = ", "
)

// ShortFormatPatterns formats a list of patterns for display in the UI.
// It will always include the first pattern, but subsequent patterns are only
// included if they will keep the total string length under a reasonable limit.
func ShortFormatPatterns(patterns []string) string {
	if len(patterns) == 0 {
		return ""
	}
	displayedPatterns := []string{}
	charCount := 0
	for i, pattern := range patterns {
		patternLength := len(pattern) + len(listSeparator)
		if i > 0 && charCount+patternLength > patternCharLimit {
			break
		}
		displayedPatterns = append(displayedPatterns, pattern)
		charCount += patternLength
	}
	out := strings.Join(displayedPatterns, listSeparator)
	if len(displayedPatterns) < len(patterns) {
		out += fmt.Sprintf(" and %d more", len(patterns)-len(displayedPatterns))
	}
	return out
}

// Splits the provided comma-separated tag string and trims off any trailing
// and leading whitespace from each tag.  If validate it set to true and the
// trimmed, comma-separated string containing the tags would be longer than 160
// characters, an error is returned.
func SplitAndTrimTags(tags string, validate bool) ([]*invocation.Invocation_Tag, error) {
	if len(tags) == 0 {
		return []*invocation.Invocation_Tag{}, nil
	}
	splitTags := strings.Split(tags, ",")
	totalLength := 0
	out := make([]*invocation.Invocation_Tag, 0, len(splitTags))
	for _, t := range splitTags {
		trimmed := strings.TrimSpace(t)
		if len(trimmed) > 0 {
			if validate && totalLength+len(trimmed) > 160 {
				return nil, status.InvalidArgumentError("Tag list is too long.")
			}
			totalLength += len(trimmed) + 1
			out = append(out, &invocation.Invocation_Tag{Name: trimmed})
		}
	}
	return out, nil
}

func JoinTags(tags []*invocation.Invocation_Tag) (string, error) {
	if len(tags) == 0 {
		return "", nil
	}
	outSlice := make([]string, 0, len(tags))
	for _, t := range tags {
		if t == nil {
			continue
		}
		trimmed := strings.TrimSpace(t.Name)
		if strings.Contains(trimmed, ",") {
			return "", status.InvalidArgumentError(fmt.Sprintf("Invalid tag: %s", trimmed))
		}
		if len(trimmed) > 0 {
			outSlice = append(outSlice, trimmed)
		}
	}
	return strings.Join(outSlice, ","), nil
}

// This *does not* trim whitespace and therefore must not be used for
// general-purpose conversion--it's taking a shortcut because we trust that the
// DB already has properly-trimmed tags.
func ConvertDBTagsToOLAP(tags string) []string {
	if len(tags) == 0 {
		return []string{}
	}
	return strings.Split(tags, ",")
}

func GetTagsAsClickhouseWhereClause(fieldName string, tags []string) (string, []interface{}) {
	outStrings := []string{}
	outArgs := []interface{}{}
	for _, tag := range tags {
		outStrings = append(outStrings, "?")
		outArgs = append(outArgs, tag)
	}
	return fmt.Sprintf("hasAll(%s, [%s])", fieldName, strings.Join(outStrings, ",")), outArgs
}
