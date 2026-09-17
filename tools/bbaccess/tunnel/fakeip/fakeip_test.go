package fakeip

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLookupIsStableAcrossTables(t *testing.T) {
	// Applications cache DNS answers well past the TTL, so a restarted daemon
	// has to hand out the same address for the same name or those caches point
	// at an address it no longer recognizes.
	prefix := netip.MustParsePrefix("198.18.0.0/16")
	a, err := NewTable(prefix)
	require.NoError(t, err)
	b, err := NewTable(prefix)
	require.NoError(t, err)

	for _, name := range []string{"host.foo.bb.internal", "otel.monitor-dev.svc.cluster.local"} {
		addrA, err := a.Lookup(name)
		require.NoError(t, err)
		addrB, err := b.Lookup(name)
		require.NoError(t, err)
		require.Equal(t, addrA, addrB, "%s should get the same address from a fresh table", name)
	}
}

func TestLookupIsIdempotent(t *testing.T) {
	table, err := NewTable(netip.MustParsePrefix("198.18.0.0/16"))
	require.NoError(t, err)
	first, err := table.Lookup("host.foo.bb.internal")
	require.NoError(t, err)
	second, err := table.Lookup("host.foo.bb.internal")
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 1, table.Len())
}

func TestReverseLookup(t *testing.T) {
	table, err := NewTable(netip.MustParsePrefix("198.18.0.0/16"))
	require.NoError(t, err)
	addr, err := table.Lookup("host.foo.bb.internal")
	require.NoError(t, err)

	name, ok := table.Name(addr)
	require.True(t, ok)
	require.Equal(t, "host.foo.bb.internal", name)

	_, ok = table.Name(netip.MustParseAddr("10.0.0.1"))
	require.False(t, ok)
}

func TestAddressesAreUniqueAndInRange(t *testing.T) {
	prefix := netip.MustParsePrefix("198.18.0.0/16")
	table, err := NewTable(prefix)
	require.NoError(t, err)

	seen := make(map[netip.Addr]string)
	for i := range 5000 {
		name := fmt.Sprintf("host-%d.foo.bb.internal", i)
		addr, err := table.Lookup(name)
		require.NoError(t, err)
		require.True(t, prefix.Contains(addr), "%s is outside %s", addr, prefix)
		// The first two addresses are reserved for the interface itself.
		require.NotEqual(t, "198.18.0.0", addr.String())
		require.NotEqual(t, "198.18.0.1", addr.String())
		if prev, dup := seen[addr]; dup {
			t.Fatalf("%s and %s both got %s", prev, name, addr)
		}
		seen[addr] = name
	}
}

func TestExhaustion(t *testing.T) {
	// /30: 4 addresses, minus 2 reserved at the start and the broadcast
	// address, leaves exactly 1 usable.
	table, err := NewTable(netip.MustParsePrefix("198.18.0.0/30"))
	require.NoError(t, err)
	_, err = table.Lookup("first.foo.bb.internal")
	require.NoError(t, err)
	_, err = table.Lookup("second.foo.bb.internal")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exhausted")
}

func TestRejectsIPv6Prefix(t *testing.T) {
	_, err := NewTable(netip.MustParsePrefix("fd00::/64"))
	require.Error(t, err)
}
