package keys

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateKey_Length(t *testing.T) {
	k, err := GenerateKey()
	require.NoError(t, err)
	require.Len(t, k, KeyLen)
}

func TestGeneratePrivateKey_Clamping(t *testing.T) {
	for i := 0; i < 20; i++ {
		k, err := GeneratePrivateKey()
		require.NoError(t, err)
		// Per https://cr.yp.to/ecdh.html: low 3 bits of byte 0 cleared.
		require.Equal(t, byte(0), k[0]&7, "byte[0] low 3 bits must be cleared")
		// High bit of byte 31 cleared.
		require.Equal(t, byte(0), k[31]&128, "byte[31] high bit must be cleared")
		// Second-highest bit of byte 31 set.
		require.Equal(t, byte(64), k[31]&64, "byte[31] bit 6 must be set")
	}
}

func TestNewKey_WrongLength(t *testing.T) {
	_, err := NewKey([]byte{1, 2, 3})
	require.Error(t, err)

	_, err = NewKey(make([]byte, KeyLen+1))
	require.Error(t, err)
}

func TestNewKey_CorrectLength(t *testing.T) {
	b := make([]byte, KeyLen)
	for i := range b {
		b[i] = byte(i)
	}
	k, err := NewKey(b)
	require.NoError(t, err)
	require.Equal(t, b, k[:])
}

func TestPublicKey_Deterministic(t *testing.T) {
	priv, err := GeneratePrivateKey()
	require.NoError(t, err)
	pub1 := priv.PublicKey()
	pub2 := priv.PublicKey()
	require.Equal(t, pub1, pub2)
}

func TestKeyString_IsHex(t *testing.T) {
	k, err := GenerateKey()
	require.NoError(t, err)
	s := k.String()
	decoded, err := hex.DecodeString(s)
	require.NoError(t, err, "String() must return valid hex")
	require.Len(t, decoded, KeyLen)
}

// TestParseKey_StringRoundTrip documents the inconsistency between String()
// (which returns hex) and ParseKey() (which expects base64). These should
// round-trip but currently do not — this test will fail until the bug is fixed.
func TestParseKey_StringRoundTrip(t *testing.T) {
	k, err := GenerateKey()
	require.NoError(t, err)
	k2, err := ParseKey(k.String())
	require.NoError(t, err)
	require.Equal(t, k, k2)
}
