package clientip_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestDisabled(t *testing.T) {
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4")
	require.False(t, ok)
	require.Equal(t, "", clientip.Get(ctx))
}

func TestNGINXHeader(t *testing.T) {
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))
}

func TestGCPLBHeader(t *testing.T) {
	flags.Set(t, "auth.trust_xforwardedfor_header", true)

	// If there are exactly two values, we use the second IP from the right.
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), "4.3.2.1, 1.2.3.4")
	require.True(t, ok)
	require.Equal(t, "4.3.2.1", clientip.Get(ctx))

	// If there are more than two values, we still use the second IP from the right.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), "8.8.8.8, 4.3.2.1, 1.2.3.4")
	require.True(t, ok)
	require.Equal(t, "4.3.2.1", clientip.Get(ctx))
}
