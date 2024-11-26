package shlex_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/stretchr/testify/require"
)

func TestSplitQuote(t *testing.T) {
	tokens := []string{
		"foo",
		"--path=has spaces",
		"quote's",
		"~",
	}
	quoted := shlex.Quote(tokens...)
	require.Equal(
		t,
		`foo --path='has spaces' 'quote'\''s' '~'`,
		quoted)
	split, err := shlex.Split(quoted)
	require.NoError(t, err)
	require.Equal(t, tokens, split)
}
