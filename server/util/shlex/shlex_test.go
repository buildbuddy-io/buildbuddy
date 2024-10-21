package shlex_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/stretchr/testify/require"
)

func TestQuote(t *testing.T) {
	tokens := []string{
		"foo",
		"--path=has spaces",
		"quote's",
		"~",
	}
	require.Equal(
		t,
		`foo --path='has spaces' 'quote'\''s' '~'`,
		shlex.Quote(tokens...))
}
