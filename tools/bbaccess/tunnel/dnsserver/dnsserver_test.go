package dnsserver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddrFromARPA(t *testing.T) {
	for _, name := range []string{"4.3.2.1.in-addr.arpa.", "4.3.2.1.in-addr.arpa", "4.3.2.1.IN-ADDR.ARPA."} {
		addr, ok := addrFromARPA(name)
		require.True(t, ok, name)
		require.Equal(t, "1.2.3.4", addr.String())
	}
	// Only names in the reverse zone qualify: four numeric labels alone are
	// not a reverse lookup.
	for _, name := range []string{"1.2.3.4", "4.3.2.1.example.", "3.2.1.in-addr.arpa.", "x.3.2.1.in-addr.arpa.", "in-addr.arpa."} {
		_, ok := addrFromARPA(name)
		require.False(t, ok, name)
	}
}
