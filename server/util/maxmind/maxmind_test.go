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

func TestLookupASN_Unknown(t *testing.T) {
	// Private-range addresses are not present in the database; the lookup
	// should succeed but return a zero-valued ASN rather than an error.
	asn, err := LookupASN(netip.MustParseAddr("10.0.0.1"))
	require.NoError(t, err)
	assert.Zero(t, asn.Number)
}
