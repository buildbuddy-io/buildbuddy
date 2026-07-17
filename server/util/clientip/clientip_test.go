package clientip_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestDisabled(t *testing.T) {
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4", "10.0.0.1")
	require.False(t, ok)
	require.Equal(t, "", clientip.Get(ctx))
}

func TestNGINXHeader(t *testing.T) {
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4", "10.0.0.1")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))
}

func TestGCPLBHeader(t *testing.T) {
	flags.Set(t, "auth.trust_xforwardedfor_header", true)

	// If there are exactly two values, we use the second IP from the right.
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), "4.3.2.1, 1.2.3.4", "10.0.0.1")
	require.True(t, ok)
	require.Equal(t, "4.3.2.1", clientip.Get(ctx))

	// If there are more than two values, we still use the second IP from the right.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), "8.8.8.8, 4.3.2.1, 1.2.3.4", "10.0.0.1")
	require.True(t, ok)
	require.Equal(t, "4.3.2.1", clientip.Get(ctx))
}

func setTrustedPeers(t *testing.T, cidrs []string) {
	// Registered before flags.Set so that it runs after the flag is restored,
	// re-parsing the restored value.
	t.Cleanup(func() { require.NoError(t, clientip.Init()) })
	flags.Set(t, "auth.trusted_xforwardedfor_peers", cidrs)
	require.NoError(t, clientip.Init())
}

func TestTrustedPeers(t *testing.T) {
	// The header is trusted for matching peers even when
	// auth.trust_xforwardedfor_header is false.
	setTrustedPeers(t, []string{"10.0.0.0/8", "2001:db8::/32"})

	// Peer within a trusted CIDR.
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4", "10.1.2.3")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))

	// IPv6 peer within a trusted CIDR.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4", "2001:db8::1")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))

	// Peer outside the trusted CIDRs.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4", "11.1.2.3")
	require.False(t, ok)
	require.Equal(t, "", clientip.Get(ctx))

	// Unknown peer.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4", "")
	require.False(t, ok)
	require.Equal(t, "", clientip.Get(ctx))
}

func TestInit(t *testing.T) {
	t.Cleanup(func() { require.NoError(t, clientip.Init()) })

	flags.Set(t, "auth.trusted_xforwardedfor_peers", []string{"10.0.0.0/8"})
	require.NoError(t, clientip.Init())

	flags.Set(t, "auth.trusted_xforwardedfor_peers", []string{"10.0.0.0/8", "not-a-cidr"})
	require.Error(t, clientip.Init())
}

func TestTrustHeaderOverridesTrustedPeers(t *testing.T) {
	// If auth.trust_xforwardedfor_header is true, the header is trusted from
	// every peer, even those outside the trusted CIDRs.
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	setTrustedPeers(t, []string{"10.0.0.0/8"})
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), "1.2.3.4", "11.1.2.3")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))
}
