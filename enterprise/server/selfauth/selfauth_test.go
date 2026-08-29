package selfauth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coreos/go-oidc/v3/oidc"
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

// The ID token must verify against the published key (through go-oidc, the
// client the server itself uses) and carry the same claims, with the same
// JSON types, as the jwx-produced token did.
func TestAccessTokenSignsVerifiableIDToken(t *testing.T) {
	o, err := NewSelfAuth()
	require.NoError(t, err)
	jwks := httptest.NewServer(http.HandlerFunc(o.Jwks))
	defer jwks.Close()

	rr := httptest.NewRecorder()
	o.AccessToken(rr, httptest.NewRequest(http.MethodPost, "/token", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	var resp tokenJSON
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	// Header.
	parts := strings.Split(resp.IdToken, ".")
	require.Len(t, parts, 3)
	hdr, err := base64.RawURLEncoding.DecodeString(parts[0])
	require.NoError(t, err)
	require.JSONEq(t, `{"alg":"RS256","typ":"JWT"}`, string(hdr))

	// Raw claim JSON types.
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &raw))
	require.Equal(t, `["buildbuddy"]`, string(raw["aud"]))
	require.Regexp(t, `^[0-9]+$`, string(raw["exp"]))
	require.Regexp(t, `^[0-9]+$`, string(raw["iat"]))
	require.Equal(t, `""`, string(raw["sub"]))
	require.Equal(t, `"buildbuddy"`, string(raw["name"]))
	require.Equal(t, `"Default"`, string(raw["given_name"]))
	require.Equal(t, `"buildbuddy@example.com"`, string(raw["email"]))
	require.Equal(t, `"LkjhI6Ijpj638f0mirBH2g"`, string(raw["at_hash"]))
	require.Equal(t, `"`+o.IssuerURL().String()+`"`, string(raw["iss"]))
	require.NotEmpty(t, raw["jti"])

	// Signature, via go-oidc fetching the served JWKS.
	keySet := oidc.NewRemoteKeySet(context.Background(), jwks.URL)
	verifier := oidc.NewVerifier(o.IssuerURL().String(), keySet, &oidc.Config{ClientID: "buildbuddy"})
	idToken, err := verifier.Verify(context.Background(), resp.IdToken)
	require.NoError(t, err)
	require.Equal(t, []string{"buildbuddy"}, idToken.Audience)

	// And via golang-jwt with the in-memory key.
	claims := jwt.MapClaims{}
	tok, err := jwt.ParseWithClaims(resp.IdToken, claims, func(tok *jwt.Token) (any, error) {
		return &o.rsaPrivateKey.PublicKey, nil
	})
	require.NoError(t, err)
	require.True(t, tok.Valid)
	var _ *rsa.PublicKey = &o.rsaPrivateKey.PublicKey
}
