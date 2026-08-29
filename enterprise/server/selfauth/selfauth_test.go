package selfauth

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
)

// The JWKS document must be exactly what the jwx library produced before
// (RFC 7518 RSA public key: kty/n/e, base64url without padding), since
// OIDC clients fetch it to verify the ID tokens we sign.
func TestJwks(t *testing.T) {
	o, err := NewSelfAuth()
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	o.Jwks(rr, httptest.NewRequest(http.MethodGet, "/.well-known/jwks.json", nil))
	require.Equal(t, http.StatusOK, rr.Code)

	var set struct {
		Keys []map[string]string `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &set))
	require.Len(t, set.Keys, 1)
	key := set.Keys[0]
	require.Equal(t, "RSA", key["kty"])
	require.Equal(t, "AQAB", key["e"])
	require.Len(t, key, 3)
	n, err := base64.RawURLEncoding.DecodeString(key["n"])
	require.NoError(t, err)
	require.Equal(t, 0, new(big.Int).SetBytes(n).Cmp(o.rsaPrivateKey.N))
}

// The ID token must verify against the published key and carry the same
// claims as before.
func TestAccessTokenSignsVerifiableIDToken(t *testing.T) {
	o, err := NewSelfAuth()
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	o.AccessToken(rr, httptest.NewRequest(http.MethodPost, "/token", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	var resp tokenJSON
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	claims := jwt.MapClaims{}
	tok, err := jwt.ParseWithClaims(resp.IdToken, claims, func(tok *jwt.Token) (any, error) {
		require.Equal(t, "RS256", tok.Method.Alg())
		return &o.rsaPrivateKey.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, tok.Valid)
	require.Equal(t, "buildbuddy", claims["aud"])
	require.Equal(t, "buildbuddy@example.com", claims["email"])
	require.Equal(t, "LkjhI6Ijpj638f0mirBH2g", claims["at_hash"])
	require.Equal(t, "", claims["sub"])
	require.NotEmpty(t, claims["jti"])
	var _ *rsa.PublicKey = &o.rsaPrivateKey.PublicKey
}
