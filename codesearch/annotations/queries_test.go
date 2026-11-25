package annotations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllQueriesCompile(t *testing.T) {
	require.NotEmpty(t, allQueries)
	for i, q := range allQueries {
		_, err := q()
		require.NoError(t, err, "query %d", i)
	}
}
