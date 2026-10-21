package review

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReviewCommentText(t *testing.T) {
	for _, tc := range []struct {
		name     string
		category string
		want     string
	}{
		{"before_merge", "before_merge", "**Address before merge:** body"},
		{"follow_up", "follow_up", "**Address in a follow-on:** body"},
		{"discussion", "discussion", "**Item for discussion:** body"},
		{"missing category", "", "body"},
		{"unknown category", "blocker", "body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := reviewComment{Category: tc.category, Body: "body"}
			assert.Equal(t, tc.want, c.text())
		})
	}
}

func TestPlaceComment(t *testing.T) {
	validLines := map[fileLine]bool{
		{"a.go", 10}: true,
		{"a.go", 20}: true,
		{"b.go", 5}:  true,
		{"c.go", 1}:  true,
	}
	for _, tc := range []struct {
		name string
		file string
		line int
		want fileLine
	}{
		{"in diff", "a.go", 20, fileLine{"a.go", 20}},
		{"nearest is an earlier line", "a.go", 12, fileLine{"a.go", 10}},
		{"nearest is a later line", "a.go", 18, fileLine{"a.go", 20}},
		{"tie picks the earlier line", "a.go", 15, fileLine{"a.go", 10}},
		{"file not in diff", "d.go", 3, fileLine{"a.go", 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loc, ok := placeComment(reviewComment{File: tc.file, Line: tc.line}, validLines)
			require.True(t, ok)
			assert.Equal(t, tc.want, loc)
		})
	}

	_, ok := placeComment(reviewComment{File: "a.go", Line: 1}, map[fileLine]bool{})
	assert.False(t, ok)
}

func TestReviewJSONCategory(t *testing.T) {
	var review reviewJSON
	err := json.Unmarshal([]byte(`{
		"summary": "s",
		"comments": [
			{"file": "a.go", "line": 1, "category": "follow_up", "body": "x"},
			{"file": "b.go", "line": 2, "body": "y"}
		]
	}`), &review)
	require.NoError(t, err)
	require.Len(t, review.Comments, 2)
	assert.Equal(t, "**Address in a follow-on:** x", review.Comments[0].text())
	assert.Equal(t, "y", review.Comments[1].text())
}
