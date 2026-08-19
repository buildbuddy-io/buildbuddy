package maxmind

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetASNLiteDB(t *testing.T) {
	db, err := GetASNLiteDB()
	require.NoError(t, err)
	require.NotNil(t, db)
}

func TestLookupASN(t *testing.T) {
	// 8.8.8.8 is Google's public DNS, in AS15169 (Google LLC).
	asn, err := LookupASN(netip.MustParseAddr("8.8.8.8"))
	require.NoError(t, err)
	assert.Equal(t, uint(15169), asn.Number)
	assert.Contains(t, asn.Organization, "Google")
}

func TestLookupASN_BuildBuddy(t *testing.T) {
	for _, ip := range []string{
		"23.176.168.49",  // SJC
		"216.226.68.241", // NUQ
	} {
		t.Run(ip, func(t *testing.T) {
			asn, err := LookupASN(netip.MustParseAddr(ip))
			require.NoError(t, err)
			assert.Equal(t, uint(17095), asn.Number)
			assert.Equal(t, "BuildBuddy", asn.Organization)
		})
	}
}

func TestLookupASN_Unknown(t *testing.T) {
	// Private-range addresses are not present in the database; the lookup
	// should succeed but return a zero-valued ASN rather than an error.
	asn, err := LookupASN(netip.MustParseAddr("10.0.0.1"))
	require.NoError(t, err)
	assert.Zero(t, asn.Number)
}
