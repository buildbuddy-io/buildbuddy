package invocation_format

import (
	"fmt"
	"strings"
)

const (
	patternCharLimit = 50
	listSeparator    = ", "
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
		if i == 0 || charCount+patternLength < patternCharLimit {
			displayedPatterns = append(displayedPatterns, pattern)
			charCount += patternLength
		}
	}
	out := strings.Join(displayedPatterns, listSeparator)
	if len(displayedPatterns) < len(patterns) {
		out += fmt.Sprintf(" and %d more", len(patterns)-len(displayedPatterns))
	}
	return out
}
