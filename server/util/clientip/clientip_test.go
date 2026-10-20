package clientip_test

import (
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestDisabled(t *testing.T) {
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), []string{"1.2.3.4"}, "10.0.0.1")
	require.False(t, ok)
	require.Equal(t, "", clientip.Get(ctx))
}

func TestNGINXHeader(t *testing.T) {
	flags.Set(t, "auth.trust_xforwardedfor_header", true)
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), []string{"1.2.3.4"}, "10.0.0.1")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))
}

func TestGCPLBHeader(t *testing.T) {
	flags.Set(t, "auth.trust_xforwardedfor_header", true)

	// If there are exactly two values, we use the second IP from the right.
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), []string{"4.3.2.1, 1.2.3.4"}, "10.0.0.1")
	require.True(t, ok)
	require.Equal(t, "4.3.2.1", clientip.Get(ctx))

	// If there are more than two values, we still use the second IP from the right.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), []string{"8.8.8.8, 4.3.2.1, 1.2.3.4"}, "10.0.0.1")
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
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), []string{"1.2.3.4"}, "10.1.2.3")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))

	// IPv6 peer within a trusted CIDR.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), []string{"1.2.3.4"}, "2001:db8::1")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))

	// Peer outside the trusted CIDRs.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), []string{"1.2.3.4"}, "11.1.2.3")
	require.False(t, ok)
	require.Equal(t, "", clientip.Get(ctx))

	// Unknown peer.
	ctx, ok = clientip.SetFromXForwardedForHeader(context.Background(), []string{"1.2.3.4"}, "")
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
	ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), []string{"1.2.3.4"}, "11.1.2.3")
	require.True(t, ok)
	require.Equal(t, "1.2.3.4", clientip.Get(ctx))
}

func TestXForwardedForTrustedHops(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hops    int
		peers   []string
		header  string
		peer    string
		wantIP  string
		wantSet bool
	}{
		{name: "unset: single entry", header: "1.2.3.4", peer: "10.0.0.1", wantIP: "1.2.3.4", wantSet: true},
		{name: "unset: GCLB uses second to last", header: "forged, client, lb", peer: "10.0.0.1", wantIP: "client", wantSet: true},
		{name: "unset: untrusted peer", peers: []string{"192.168.0.0/16"}, header: "forged, client, lb", peer: "10.0.0.1"},
		{name: "1 hop uses last entry", hops: 1, header: "forged, real", peer: "10.0.0.1", wantIP: "real", wantSet: true},
		{name: "1 hop single entry", hops: 1, header: "real", peer: "10.0.0.1", wantIP: "real", wantSet: true},
		{name: "2 hops uses second to last", hops: 2, header: "forged, client, lb", peer: "10.0.0.1", wantIP: "client", wantSet: true},
		{name: "2 hops trims whitespace", hops: 2, header: "forged,  client  ,lb", peer: "10.0.0.1", wantIP: "client", wantSet: true},
		{name: "2 hops with one entry falls back to peer", hops: 2, header: "client", peer: "10.0.0.1"},
		{name: "3 hops with two entries falls back to peer", hops: 3, header: "client, lb", peer: "10.0.0.1"},
		{name: "empty entry falls back to peer", hops: 1, header: "client, ", peer: "10.0.0.1"},
		{name: "untrusted peer", hops: 1, peers: []string{"192.168.0.0/16"}, header: "forged, real", peer: "10.0.0.1"},
		{name: "trusted peer", hops: 1, peers: []string{"10.0.0.0/8"}, header: "forged, real", peer: "10.0.0.1", wantIP: "real", wantSet: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.peers == nil {
				flags.Set(t, "auth.trust_xforwardedfor_header", true)
			}
			setTrustedPeers(t, tc.peers)
			flags.Set(t, "auth.xforwardedfor_trusted_hops", tc.hops)
			require.NoError(t, clientip.Init())

			ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), []string{tc.header}, tc.peer)
			require.Equal(t, tc.wantSet, ok)
			require.Equal(t, tc.wantIP, clientip.Get(ctx))
		})
	}
}

func TestXForwardedForMultipleHeaderLines(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hops    int
		values  []string
		wantIP  string
		wantSet bool
	}{
		{name: "1 hop uses last line", hops: 1, values: []string{"forged", "real"}, wantIP: "real", wantSet: true},
		{name: "2 hops, entries in last line", hops: 2, values: []string{"forged", "client, lb"}, wantIP: "client", wantSet: true},
		{name: "2 hops, entries split across lines", hops: 2, values: []string{"forged, client", "lb"}, wantIP: "client", wantSet: true},
		{name: "2 hops counts across lines", hops: 2, values: []string{"a", "b"}, wantIP: "a", wantSet: true},
		{name: "3 hops with two entries falls back to peer", hops: 3, values: []string{"a", "b"}},
		{name: "unset uses only the first line", hops: 0, values: []string{"x", "y, z"}, wantIP: "x", wantSet: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "auth.trust_xforwardedfor_header", true)
			setTrustedPeers(t, nil)
			flags.Set(t, "auth.xforwardedfor_trusted_hops", tc.hops)
			require.NoError(t, clientip.Init())

			ctx, ok := clientip.SetFromXForwardedForHeader(context.Background(), tc.values, "10.0.0.1")
			require.Equal(t, tc.wantSet, ok)
			require.Equal(t, tc.wantIP, clientip.Get(ctx))
		})
	}
}

func TestInitTrustedHops(t *testing.T) {
	for _, tc := range []struct {
		name    string
		hops    int
		trust   bool
		peers   []string
		wantErr bool
	}{
		{name: "hops without trust", hops: 1, wantErr: true},
		{name: "hops with trust header", hops: 1, trust: true},
		{name: "hops with trusted peers", hops: 1, peers: []string{"10.0.0.0/8"}},
		{name: "no hops without trust", hops: 0},
		{name: "negative hops", hops: -1, trust: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() { require.NoError(t, clientip.Init()) })
			flags.Set(t, "auth.trust_xforwardedfor_header", tc.trust)
			flags.Set(t, "auth.trusted_xforwardedfor_peers", tc.peers)
			flags.Set(t, "auth.xforwardedfor_trusted_hops", tc.hops)
			if tc.wantErr {
				require.Error(t, clientip.Init())
			} else {
				require.NoError(t, clientip.Init())
			}
		})
	}
}

func TestHeaderIsInternal(t *testing.T) {
	require.True(t, strings.HasPrefix(clientip.HeaderName, authutil.InternalHeaderPrefix))
}
