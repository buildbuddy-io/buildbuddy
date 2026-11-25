package cacheopts_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/cacheopts"
	"github.com/stretchr/testify/require"
)

func TestMaxReadReplicas(t *testing.T) {
	ctx := context.Background()
	require.Equal(t, 0, cacheopts.MaxReadReplicas(ctx))
	require.Equal(t, 2, cacheopts.MaxReadReplicas(cacheopts.WithMaxReadReplicas(ctx, 2)))
}
