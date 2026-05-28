// Package oidcissuer implements reusable OpenID Provider machinery.
//
// It owns issuer metadata, JWKS publication, and RS256 token signing and
// verification, which are standardized OIDC concerns shared by provider
// implementations. Consumers of this package handle auth flows, token exchange
// handlers, and custom claims, which can involve application-specific business
// logic.
//
// Only the RS256 signing algorithm is supported, currently.
package oidcissuer

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v4"
)

const (
	// ConfigurationPath is the standard well-known path for
	// OpenID Provider Configuration metadata.
	ConfigurationPath = "/.well-known/openid-configuration"

	signingAlgorithmRS256 = "RS256"
)

// Config contains key material and OpenID Provider Metadata for an OIDC issuer.
type Config struct {
	// PrivateKeyPEM is the PEM-encoded RSA private key used to sign RS256
	// tokens.
	PrivateKeyPEM string

	// Standard OpenID Provider Metadata fields.
	// See https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata

	IssuerURL              string   // Ex: "https://buildbuddy.example.com/oidc"
	AuthorizationEndpoint  string   // Ex: "https://buildbuddy.example.com/oauth/authorize"
	JWKSURI                string   // Ex: "https://buildbuddy.example.com/.well-known/jwks"
	TokenEndpoint          string   // Ex: "https://buildbuddy.example.com/oauth/token"
	UserinfoEndpoint       string   // Ex: "https://buildbuddy.example.com/userinfo"
	SubjectTypesSupported  []string // Ex: []string{"public"}
	ResponseTypesSupported []string // Ex: []string{"code"}
	GrantTypesSupported    []string // Ex: []string{"authorization_code"}
	ClaimsSupported        []string // Ex: []string{"sub", "aud"}
	ScopesSupported        []string // Ex: []string{"openid"}
}

// ProviderMetadata contains standard OIDC provider metadata fields.
// See https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata
type ProviderMetadata struct {
	Issuer                           string   `json:"issuer"`
	AuthorizationEndpoint            string   `json:"authorization_endpoint,omitempty"`
	JWKSURI                          string   `json:"jwks_uri"`
	TokenEndpoint                    string   `json:"token_endpoint,omitempty"`
	UserinfoEndpoint                 string   `json:"userinfo_endpoint,omitempty"`
	SubjectTypesSupported            []string `json:"subject_types_supported"`
	ResponseTypesSupported           []string `json:"response_types_supported,omitempty"`
	GrantTypesSupported              []string `json:"grant_types_supported,omitempty"`
	ClaimsSupported                  []string `json:"claims_supported,omitempty"`
	IdTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
	ScopesSupported                  []string `json:"scopes_supported,omitempty"`
}

// JWKS contains standard JSON Web Key Set fields.
// See https://www.rfc-editor.org/rfc/rfc7517#section-5
type JWKS struct {
	Keys []*JWK `json:"keys"`
}

// JWK contains standard RSA JSON Web Key fields.
// See https://www.rfc-editor.org/rfc/rfc7517#section-4 and
// https://www.rfc-editor.org/rfc/rfc7518#section-6.3.1
type JWK struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	N   string `json:"n,omitempty"`
	E   string `json:"e,omitempty"`
}

// Provider is a configured OIDC issuer backed by an RSA signing key.
//
// It serves discovery metadata and a JWKS for its issuer URL, and signs or
// verifies JWTs with the configured key.
type Provider struct {
	privateKey *rsa.PrivateKey
	keyID      string

	issuerURL string

	providerMetadataResponse string
	jwksResponse             string
}

// New returns a Provider configured with the given OIDC metadata and signing
// key.
func New(cfg Config) (*Provider, error) {
	key, keyID, err := signingKey(cfg.PrivateKeyPEM)
	if err != nil {
		return nil, err
	}
	if cfg.IssuerURL == "" {
		return nil, fmt.Errorf("OIDC issuer URL is required")
	}
	if cfg.JWKSURI == "" {
		return nil, fmt.Errorf("OIDC jwks_uri is required")
	}
	if len(cfg.SubjectTypesSupported) == 0 {
		return nil, fmt.Errorf("OIDC subject_types_supported is required")
	}
	// Pre-marshal the static provider configuration and JWKS JSON.
	providerMetadataJSON, err := json.Marshal(&ProviderMetadata{
		Issuer:                           cfg.IssuerURL,
		AuthorizationEndpoint:            cfg.AuthorizationEndpoint,
		JWKSURI:                          cfg.JWKSURI,
		TokenEndpoint:                    cfg.TokenEndpoint,
		UserinfoEndpoint:                 cfg.UserinfoEndpoint,
		SubjectTypesSupported:            cfg.SubjectTypesSupported,
		ResponseTypesSupported:           cfg.ResponseTypesSupported,
		GrantTypesSupported:              cfg.GrantTypesSupported,
		ClaimsSupported:                  cfg.ClaimsSupported,
		IdTokenSigningAlgValuesSupported: []string{signingAlgorithmRS256},
		ScopesSupported:                  cfg.ScopesSupported,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal OIDC provider metadata: %w", err)
	}
	jwksJSONBytes, err := json.Marshal(&JWKS{
		Keys: []*JWK{
			publicJWK(&key.PublicKey, keyID),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal OIDC JWKS: %w", err)
	}
	return &Provider{
		privateKey: key,
		keyID:      keyID,
		issuerURL:  cfg.IssuerURL,

		providerMetadataResponse: string(providerMetadataJSON) + "\n",
		jwksResponse:             string(jwksJSONBytes) + "\n",
	}, nil
}

// Issuer returns this provider's issuer URL.
func (p *Provider) Issuer() string {
	return p.issuerURL
}

// ServeWellKnownOpenIDConfiguration writes this issuer's OpenID Provider
// Configuration metadata.
func (p *Provider) ServeWellKnownOpenIDConfiguration(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	_, _ = io.WriteString(w, p.providerMetadataResponse)
}

// ServeJWKS writes this issuer's JSON Web Key Set.
func (p *Provider) ServeJWKS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	_, _ = io.WriteString(w, p.jwksResponse)
}

// Sign signs claims as a JWT using this issuer's key.
func (p *Provider) Sign(c jwt.Claims) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	token.Header["kid"] = p.keyID
	token.Header["typ"] = "JWT"
	return token.SignedString(p.privateKey)
}

// ParseClaims verifies tokenString as a JWT signed by this provider and
// stores the parsed claims in c.
func (p *Provider) ParseClaims(tokenString string, c jwt.Claims) (*jwt.Token, error) {
	return jwt.ParseWithClaims(tokenString, c, func(token *jwt.Token) (any, error) {
		if token.Method != jwt.SigningMethodRS256 {
			return nil, fmt.Errorf("unexpected signing method %q", token.Method.Alg())
		}
		if kid, _ := token.Header["kid"].(string); kid != "" && kid != p.keyID {
			return nil, fmt.Errorf("unknown key ID %q", kid)
		}
		return &p.privateKey.PublicKey, nil
	})
}

func signingKey(keyPEM string) (*rsa.PrivateKey, string, error) {
	if keyPEM == "" {
		return nil, "", fmt.Errorf("OIDC issuer private key must be configured")
	}
	pem := strings.ReplaceAll(keyPEM, `\n`, "\n")
	key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(pem))
	if err != nil {
		return nil, "", fmt.Errorf("parse OIDC RSA private key: %w", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, "", fmt.Errorf("marshal OIDC public key: %w", err)
	}
	sum := sha256.Sum256(der)
	return key, base64.RawURLEncoding.EncodeToString(sum[:]), nil
}

func publicJWK(key *rsa.PublicKey, keyID string) *JWK {
	return &JWK{
		Kty: "RSA",
		Use: "sig",
		Kid: keyID,
		Alg: signingAlgorithmRS256,
		N:   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
}
