// Package relayauth authenticates WireGuard gateway clients using
// short-lived X.509 client certificates.
//
// The client signs a JWT using the private key and the relay gateway verifies
// that the user certificate is signed by the tunnel CA.
package relayauth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/golang-jwt/jwt/v4"
)

// CredentialHeader is the gRPC metadata key carrying the credential.
const CredentialHeader = "x-buildbuddy-tunnel-credential"

// credentialType is the JWT "typ" header.
const credentialType = "tunnel-credential+jwt"

// MaxAssertionLifetime bounds how far in the future a credential may expire.
// The signed JWT is only used during registration so it only needs to be valid
// for a short amount of time.
const MaxAssertionLifetime = 10 * time.Minute

// DefaultAssertionLifetime is the expiry clients should ask for. Registration
// is a single short RPC, so this only needs to cover one call plus skew.
const DefaultAssertionLifetime = 2 * time.Minute

type claims struct {
	jwt.RegisteredClaims
	// WireGuardPublicKey is the hex-encoded WireGuard public key this
	// credential authorizes registering.
	WireGuardPublicKey string `json:"wg"`
}

// allowedMethods restricts signing methods. We only allow ES256.
var allowedMethods = []string{jwt.SigningMethodES256.Alg()}

// Signer holds a certificate and its private key and mints credentials.
type Signer struct {
	certDER []byte
	cert    *x509.Certificate
	key     crypto.Signer
}

// NewSigner parses a PEM certificate and private key pair, as issued by the
// tunnel CA.
func NewSigner(certPEM, keyPEM []byte) (*Signer, error) {
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil {
		return nil, fmt.Errorf("relayauth: certificate is not valid PEM")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("relayauth: parse certificate: %w", err)
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, fmt.Errorf("relayauth: private key is not valid PEM")
	}
	key, err := parsePrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, err
	}

	// A cert and key that don't belong together produce signatures that fail
	// verification remotely, with an error that points nowhere near the cause.
	// Catch it here instead.
	pub, ok := key.Public().(interface{ Equal(crypto.PublicKey) bool })
	if !ok {
		return nil, fmt.Errorf("relayauth: private key of type %T cannot be compared to the certificate", key)
	}
	if !pub.Equal(cert.PublicKey) {
		return nil, fmt.Errorf("relayauth: private key does not match certificate")
	}

	if err := checkKey(cert.PublicKey); err != nil {
		return nil, err
	}

	return &Signer{certDER: certBlock.Bytes, cert: cert, key: key}, nil
}

// parsePrivateKey parses the PKCS#8 key bbcert generates alongside the
// certificate request.
func parsePrivateKey(der []byte) (crypto.Signer, error) {
	key, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("relayauth: parse private key: %w", err)
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("relayauth: private key of type %T cannot sign", key)
	}
	return signer, nil
}

// Email returns the identity the certificate asserts.
func (s *Signer) Email() string { return s.cert.Subject.CommonName }

// NotAfter returns when the underlying certificate expires. Callers use it to
// tell a user their credential needs renewing.
func (s *Signer) NotAfter() time.Time { return s.cert.NotAfter }

// Sign mints a credential authorizing the registration of wgPublicKey at the
// gateway identified by audience.
func (s *Signer) Sign(audience, wgPublicKey string, lifetime time.Duration) (string, error) {
	if audience == "" {
		return "", fmt.Errorf("relayauth: audience is required")
	}
	if wgPublicKey == "" {
		return "", fmt.Errorf("relayauth: WireGuard public key is required")
	}
	if lifetime <= 0 || lifetime > MaxAssertionLifetime {
		return "", fmt.Errorf("relayauth: lifetime %s is outside (0, %s]", lifetime, MaxAssertionLifetime)
	}

	now := time.Now()
	token := jwt.NewWithClaims(jwt.SigningMethodES256, &claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(lifetime)),
		},
		WireGuardPublicKey: wgPublicKey,
	})
	token.Header["typ"] = credentialType
	token.Header["x5c"] = []string{base64.StdEncoding.EncodeToString(s.certDER)}

	cred, err := token.SignedString(s.key)
	if err != nil {
		return "", fmt.Errorf("relayauth: sign credential: %w", err)
	}
	return cred, nil
}

// Identity is the verified result of a credential.
type Identity struct {
	// Email is the certificate's common name.
	Email string
	// WireGuardPublicKey is the key the credential authorizes. Callers MUST
	// check it against the key in the request being authenticated.
	WireGuardPublicKey string
	// CertNotAfter is when the certificate expires. Callers use it to bound
	// the lifetime of whatever they grant.
	CertNotAfter time.Time
}

// Verifier checks credentials against the tunnel CA and audience.
type Verifier struct {
	roots    *x509.CertPool
	audience string
	// now is overridable in tests.
	now func() time.Time
}

// NewVerifier builds a Verifier trusting the PEM CA certificate(s) in caPEM.
// audience must match what clients sign, and identifies this gateway.
func NewVerifier(caPEM []byte, audience string) (*Verifier, error) {
	if audience == "" {
		return nil, fmt.Errorf("relayauth: audience is required")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("relayauth: no CA certificates found in PEM input")
	}
	return &Verifier{roots: roots, audience: audience, now: time.Now}, nil
}

// Verify checks a credential and returns the identity it establishes.
func (v *Verifier) Verify(credential string) (*Identity, error) {
	// The parser enforces the algorithm allowlist and the signature, using
	// the key from the x5c certificate once that chains to our CA. Claims are
	// checked below rather than by the parser, which only knows the wall
	// clock and does not insist on an expiry.
	var leaf *x509.Certificate
	c := &claims{}
	parser := jwt.NewParser(jwt.WithValidMethods(allowedMethods), jwt.WithoutClaimsValidation())
	token, err := parser.ParseWithClaims(credential, c, func(t *jwt.Token) (interface{}, error) {
		cert, err := v.leafCertificate(t.Header)
		if err != nil {
			return nil, err
		}
		leaf = cert
		return cert.PublicKey, nil
	})
	if err != nil {
		var verr *jwt.ValidationError
		if errors.As(err, &verr) {
			switch {
			case verr.Errors&jwt.ValidationErrorSignatureInvalid != 0:
				return nil, fmt.Errorf("relayauth: credential signature is invalid: %w", err)
			case verr.Inner != nil:
				// leafCertificate's own error: an untrusted or malformed
				// certificate.
				return nil, fmt.Errorf("relayauth: %w", verr.Inner)
			}
		}
		return nil, fmt.Errorf("relayauth: malformed credential: %w", err)
	}

	if typ, _ := token.Header["typ"].(string); typ != credentialType {
		return nil, fmt.Errorf("relayauth: credential has type %q, not %q", typ, credentialType)
	}

	now := v.now()
	if !c.VerifyAudience(v.audience, true) {
		return nil, fmt.Errorf("relayauth: credential is for audience %v, not %q", []string(c.Audience), v.audience)
	}
	if c.ExpiresAt == nil {
		return nil, fmt.Errorf("relayauth: credential has no expiry")
	}
	exp := c.ExpiresAt.Time
	if !exp.After(now) {
		return nil, fmt.Errorf("relayauth: credential expired at %s", exp.UTC().Format(time.RFC3339))
	}
	if exp.After(now.Add(MaxAssertionLifetime)) {
		return nil, fmt.Errorf("relayauth: credential expires at %s, more than %s out",
			exp.UTC().Format(time.RFC3339), MaxAssertionLifetime)
	}
	if c.WireGuardPublicKey == "" {
		return nil, fmt.Errorf("relayauth: credential does not name a WireGuard public key")
	}

	if leaf.Subject.CommonName == "" {
		return nil, fmt.Errorf("relayauth: certificate has no common name to identify the holder")
	}

	return &Identity{
		Email:              leaf.Subject.CommonName,
		WireGuardPublicKey: c.WireGuardPublicKey,
		CertNotAfter:       leaf.NotAfter,
	}, nil
}

// leafCertificate reads the user certificate out of a JWT's x5c header and
// verifies it against the pinned CA for client authentication.
func (v *Verifier) leafCertificate(header map[string]interface{}) (*x509.Certificate, error) {
	x5c, _ := header["x5c"].([]interface{})
	if len(x5c) != 1 {
		return nil, fmt.Errorf("credential must carry exactly one certificate in x5c, has %d", len(x5c))
	}
	encoded, ok := x5c[0].(string)
	if !ok {
		return nil, fmt.Errorf("malformed x5c header")
	}
	der, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}

	// Chain to a trusted CA. This also enforces the validity window, so an
	// expired certificate is rejected here.
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:       v.roots,
		CurrentTime: v.now(),
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return nil, fmt.Errorf("certificate is not trusted: %w", err)
	}
	if !slices.Contains(cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		return nil, fmt.Errorf("certificate is not trusted: not valid for client authentication")
	}
	return cert, nil
}

// checkKey requires the one key type the credential is pinned to (see
// allowedMethods).
func checkKey(pub crypto.PublicKey) error {
	switch k := pub.(type) {
	case *ecdsa.PublicKey:
		if k.Curve == elliptic.P256() {
			return nil
		}
		return fmt.Errorf("relayauth: certificate key must be ECDSA P-256, got curve %s", k.Curve.Params().Name)
	default:
		return fmt.Errorf("relayauth: certificate key must be ECDSA P-256, got %T", pub)
	}
}
