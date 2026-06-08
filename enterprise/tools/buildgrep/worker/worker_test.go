package main

import (
	"bytes"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogSearcher_LiteralTemplate(t *testing.T) {
	tmpl, err := template.New("match").Parse(`{{.UpdatedAt}} {{.InvocationID}} {{.LineNumber}} {{.MatchedText}} {{.Match}}`)
	require.NoError(t, err)
	matcher, err := newLineMatcher(&workerConfig{Pattern: "error"})
	require.NoError(t, err)
	var out bytes.Buffer
	searcher := newLogSearcher(batchInvocation{
		InvocationID:  "iid-1",
		UpdatedAtUsec: 1700000000123456,
	}, matcher, tmpl, &out)

	// The literal matcher ignores non matching lines and renders a template for the line that contains the exact pattern.
	require.NoError(t, searcher.write([]byte("ok\nhas error here\n")))
	require.NoError(t, searcher.flush())

	assert.Equal(t, "2023-11-14T22:13:20.123456Z iid-1 2 error has error here\n", out.String())
}

func TestLogSearcher_RegexTemplateStripsANSI(t *testing.T) {
	tmpl, err := template.New("match").Parse(`{{.InvocationID}}:{{.LineNumber}}:{{.MatchedText}}:{{.Line}}`)
	require.NoError(t, err)
	matcher, err := newLineMatcher(&workerConfig{
		Pattern: `ERR-[0-9]+`,
		Regex:   true,
	})
	require.NoError(t, err)
	var out bytes.Buffer
	searcher := newLogSearcher(batchInvocation{InvocationID: "iid-2"}, matcher, tmpl, &out)

	// The regex matcher sees the ANSI stripped line even when the matching line arrives in separate log chunks.
	require.NoError(t, searcher.write([]byte("prefix \x1b[31mERR-")))
	require.NoError(t, searcher.write([]byte("123\x1b[0m suffix\n")))
	require.NoError(t, searcher.flush())

	assert.Equal(t, "iid-2:1:ERR-123:prefix ERR-123 suffix\n", out.String())
}
