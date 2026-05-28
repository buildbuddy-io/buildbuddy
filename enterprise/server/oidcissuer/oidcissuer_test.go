package oidcissuer

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"
)

func TestProvider_MetadataAndSigning(t *testing.T) {
	key := testSigningKey(t)
	provider := testProvider(t, key)

	// The issuer publishes discovery metadata that consumers use to find the
	// JWKS and token endpoint.
	rec := httptest.NewRecorder()
	provider.ServeWellKnownOpenIDConfiguration(rec, httptest.NewRequest(http.MethodGet, "/oidc"+ConfigurationPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg ProviderMetadata
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
	require.Equal(t, ProviderMetadata{
		Issuer:                           "https://buildbuddy.example.com/issuer",
		AuthorizationEndpoint:            "https://buildbuddy.example.com/issuer/authorize",
		JWKSURI:                          "https://buildbuddy.example.com/issuer/keys.json",
		TokenEndpoint:                    "https://buildbuddy.example.com/issuer/token",
		SubjectTypesSupported:            []string{"public"},
		ResponseTypesSupported:           []string{"code"},
		GrantTypesSupported:              []string{"authorization_code"},
		ClaimsSupported:                  []string{"sub", "aud"},
		IdTokenSigningAlgValuesSupported: []string{"RS256"},
		ScopesSupported:                  []string{"openid"},
	}, cfg)

	// The issuer publishes its public signing key so token consumers can verify
	// JWT signatures.
	rec = httptest.NewRecorder()
	provider.ServeJWKS(rec, httptest.NewRequest(http.MethodGet, "/oidc/.well-known/jwks", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var keySet JWKS
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &keySet))
	require.Len(t, keySet.Keys, 1)
	require.NotEmpty(t, keySet.Keys[0].Kid)
	require.NotEmpty(t, keySet.Keys[0].N)
	keySet.Keys[0].Kid = ""
	keySet.Keys[0].N = ""
	require.Equal(t, JWKS{
		Keys: []*JWK{{
			Kty: "RSA",
			Use: "sig",
			Alg: "RS256",
			E:   "AQAB",
		}},
	}, keySet)

	// Tokens signed by the issuer parse with the provider's public key and
	// preserve the caller supplied claims.
	claims := &jwt.RegisteredClaims{
		Audience:  jwt.ClaimStrings{"test-audience"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		Issuer:    provider.Issuer(),
		Subject:   "test-subject",
	}
	tokenString, err := provider.Sign(claims)
	require.NoError(t, err)

	parsedClaims := &jwt.RegisteredClaims{}
	parsed, err := provider.ParseClaims(tokenString, parsedClaims)
	require.NoError(t, err)
	require.True(t, parsed.Valid)
	require.Equal(t, claims, parsedClaims)
}

func testProvider(t *testing.T, key *rsa.PrivateKey) *Provider {
	t.Helper()
	provider, err := New(Config{
		PrivateKeyPEM:          privateKeyPEM(key),
		IssuerURL:              "https://buildbuddy.example.com/issuer",
		JWKSURI:                "https://buildbuddy.example.com/issuer/keys.json",
		AuthorizationEndpoint:  "https://buildbuddy.example.com/issuer/authorize",
		TokenEndpoint:          "https://buildbuddy.example.com/issuer/token",
		ClaimsSupported:        []string{"sub", "aud"},
		ScopesSupported:        []string{"openid"},
		ResponseTypesSupported: []string{"code"},
		GrantTypesSupported:    []string{"authorization_code"},
		SubjectTypesSupported:  []string{"public"},
	})
	require.NoError(t, err)
	return provider
}

func TestNew_RequiresSubjectTypesSupported(t *testing.T) {
	key := testSigningKey(t)

	// OpenID Connect Discovery requires issuers to advertise whether their
	// subjects are public or pairwise, so callers must make that choice
	// explicitly.
	_, err := New(Config{
		PrivateKeyPEM: privateKeyPEM(key),
		IssuerURL:     "https://buildbuddy.example.com/issuer",
		JWKSURI:       "https://buildbuddy.example.com/issuer/keys.json",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "subject_types_supported")
}

func TestProvider_OmitsEmptyOptionalMetadata(t *testing.T) {
	key := testSigningKey(t)
	provider, err := New(Config{
		PrivateKeyPEM: privateKeyPEM(key),
		IssuerURL:     "https://buildbuddy.example.com/issuer",
		JWKSURI:       "https://buildbuddy.example.com/issuer/keys.json",
		SubjectTypesSupported: []string{
			"public",
		},
	})
	require.NoError(t, err)

	// Empty optional metadata arrays are omitted instead of being served as
	// null or empty arrays.
	rec := httptest.NewRecorder()
	provider.ServeWellKnownOpenIDConfiguration(rec, httptest.NewRequest(http.MethodGet, "/oidc"+ConfigurationPath, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.NotContains(t, raw, "claims_supported")
	require.NotContains(t, raw, "grant_types_supported")
	require.NotContains(t, raw, "response_types_supported")
	require.NotContains(t, raw, "scopes_supported")
}

func testSigningKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

func privateKeyPEM(key *rsa.PrivateKey) string {
	der := x509.MarshalPKCS1PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: der,
	}))
}
