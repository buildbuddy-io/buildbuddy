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
		{"before_merge", "before_merge", "**Before merge:** body"},
		{"follow_up", "follow_up", "**Follow-up:** body"},
		{"note", "note", "**Note:** body"},
		{"missing category", "", "body"},
		{"unknown category", "blocker", "body"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := reviewComment{Category: tc.category, Body: "body"}
			assert.Equal(t, tc.want, c.text())
		})
	}
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
	assert.Equal(t, "**Follow-up:** x", review.Comments[0].text())
	assert.Equal(t, "y", review.Comments[1].text())
}
