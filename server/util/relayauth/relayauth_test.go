package relayauth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAudience = "relay.moon.buildbuddy.io"
	testWGKey    = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
)

type testCA struct {
	cert   *x509.Certificate
	key    crypto.Signer
	pem    []byte
	serial int64
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Tunnel CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

type certOpts struct {
	email     string
	notBefore time.Time
	notAfter  time.Time
	// noClientAuth omits the client-auth extended key usage.
	noClientAuth bool
	// rsaKey issues an RSA cert instead of the default P-256 one.
	rsaKey bool
	// curve overrides the default P-256 for ECDSA certs.
	curve elliptic.Curve
}

// issue returns a client cert and its key, both PEM encoded.
func (ca *testCA) issue(t *testing.T, opts certOpts) (certPEM, keyPEM []byte) {
	t.Helper()
	if opts.email == "" {
		opts.email = "vadim@buildbuddy.io"
	}
	if opts.notBefore.IsZero() {
		opts.notBefore = time.Now().Add(-time.Minute)
	}
	if opts.notAfter.IsZero() {
		opts.notAfter = time.Now().Add(12 * time.Hour)
	}

	var key crypto.Signer
	var err error
	if opts.rsaKey {
		key, err = rsa.GenerateKey(rand.Reader, 2048)
	} else {
		curve := opts.curve
		if curve == nil {
			curve = elliptic.P256()
		}
		key, err = ecdsa.GenerateKey(curve, rand.Reader)
	}
	require.NoError(t, err)

	ca.serial++
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(ca.serial + 100),
		Subject:               pkix.Name{CommonName: opts.email},
		NotBefore:             opts.notBefore,
		NotAfter:              opts.notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	if !opts.noClientAuth {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, key.Public(), ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func TestRoundTrip(t *testing.T) {
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{email: "vadim@buildbuddy.io"})

	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	assert.Equal(t, "vadim@buildbuddy.io", signer.Email())
	assert.WithinDuration(t, time.Now().Add(12*time.Hour), signer.NotAfter(), time.Minute)

	cred, err := signer.Sign(testAudience, testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)

	v, err := NewVerifier(ca.pem, testAudience)
	require.NoError(t, err)
	id, err := v.Verify(cred)
	require.NoError(t, err)

	assert.Equal(t, "vadim@buildbuddy.io", id.Email)
	assert.Equal(t, testWGKey, id.WireGuardPublicKey)
	assert.WithinDuration(t, time.Now().Add(12*time.Hour), id.CertNotAfter, time.Minute)
}

func TestOnlyP256KeysAreAccepted(t *testing.T) {
	// The credential is pinned to ES256: bbcert generates P-256 keys and
	// certgenerator certifies nothing else, so a signer refuses any other key
	// up front rather than minting a token the gateway would reject.
	ca := newTestCA(t)
	for name, opts := range map[string]certOpts{
		"RSA":   {rsaKey: true},
		"P-384": {curve: elliptic.P384()},
	} {
		t.Run(name, func(t *testing.T) {
			certPEM, keyPEM := ca.issue(t, opts)
			_, err := NewSigner(certPEM, keyPEM)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "P-256")
		})
	}
}

func TestWrongAudienceIsRejected(t *testing.T) {
	// A credential minted for the dev gateway must not work against prod.
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	cred, err := signer.Sign("gateway.dev.buildbuddy.io", testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)

	v, err := NewVerifier(ca.pem, "gateway.prod.buildbuddy.io")
	require.NoError(t, err)
	_, err = v.Verify(cred)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "audience")
}

func TestExpiredAssertionIsRejected(t *testing.T) {
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	cred, err := signer.Sign(testAudience, testWGKey, time.Minute)
	require.NoError(t, err)

	v, err := NewVerifier(ca.pem, testAudience)
	require.NoError(t, err)
	// The certificate is still good; only the assertion has aged out.
	v.now = func() time.Time { return time.Now().Add(2 * time.Minute) }

	_, err = v.Verify(cred)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expired")
}

func TestAssertionTooFarInTheFutureIsRejected(t *testing.T) {
	// Bounds the replay window even if a client asks for a long-lived
	// assertion, so a captured credential goes stale quickly.
	ca := newTestCA(t)
	// Backdate the certificate so that rewinding the verifier's clock below
	// tests the assertion bound rather than the certificate's own window.
	certPEM, keyPEM := ca.issue(t, certOpts{notBefore: time.Now().Add(-24 * time.Hour)})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)

	_, err = signer.Sign(testAudience, testWGKey, 2*time.Hour)
	require.Error(t, err, "the signer should refuse to mint a long-lived assertion")

	// A client that bypasses the signer still can't get one past a verifier.
	cred, err := signer.Sign(testAudience, testWGKey, MaxAssertionLifetime)
	require.NoError(t, err)
	v, err := NewVerifier(ca.pem, testAudience)
	require.NoError(t, err)
	v.now = func() time.Time { return time.Now().Add(-time.Hour) }
	_, err = v.Verify(cred)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "more than")
}

func TestCertFromAnotherCAIsRejected(t *testing.T) {
	trusted := newTestCA(t)
	other := newTestCA(t)
	certPEM, keyPEM := other.issue(t, certOpts{})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	cred, err := signer.Sign(testAudience, testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)

	v, err := NewVerifier(trusted.pem, testAudience)
	require.NoError(t, err)
	_, err = v.Verify(cred)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not trusted")
}

func TestExpiredCertIsRejected(t *testing.T) {
	// This is what makes a 12h certificate actually expire: the chain
	// check enforces the certificate's validity window.
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{
		notBefore: time.Now().Add(-24 * time.Hour),
		notAfter:  time.Now().Add(-time.Hour),
	})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	cred, err := signer.Sign(testAudience, testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)

	v, err := NewVerifier(ca.pem, testAudience)
	require.NoError(t, err)
	_, err = v.Verify(cred)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not trusted")
}

func TestCertWithoutClientAuthIsRejected(t *testing.T) {
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{noClientAuth: true})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	cred, err := signer.Sign(testAudience, testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)

	v, err := NewVerifier(ca.pem, testAudience)
	require.NoError(t, err)
	_, err = v.Verify(cred)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not trusted")
}

func TestSubstitutedWireGuardKeyIsRejected(t *testing.T) {
	// The whole point of the binding: an attacker who captures a credential
	// must not be able to point it at a WireGuard key they control.
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	cred, err := signer.Sign(testAudience, testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)

	attackerKey := strings.Repeat("f", 64)
	forged, err := signer.Sign(testAudience, attackerKey, DefaultAssertionLifetime)
	require.NoError(t, err)

	// Splice the attacker's payload onto the captured credential, keeping the
	// original signature.
	origParts := strings.Split(cred, ".")
	forgedParts := strings.Split(forged, ".")
	require.Len(t, origParts, 3)
	spliced := strings.Join([]string{origParts[0], forgedParts[1], origParts[2]}, ".")

	v, err := NewVerifier(ca.pem, testAudience)
	require.NoError(t, err)
	_, err = v.Verify(spliced)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signature")
}

func TestBorrowedCertIsRejected(t *testing.T) {
	// Certificates are public. Presenting someone else's, with an assertion
	// signed by your own key, must fail.
	ca := newTestCA(t)
	victimCert, _ := ca.issue(t, certOpts{email: "victim@buildbuddy.io"})
	attackerCert, attackerKey := ca.issue(t, certOpts{email: "attacker@buildbuddy.io"})

	signer, err := NewSigner(attackerCert, attackerKey)
	require.NoError(t, err)
	cred, err := signer.Sign(testAudience, testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)

	victimBlock, _ := pem.Decode(victimCert)
	require.NotNil(t, victimBlock)
	parts := strings.Split(cred, ".")
	require.Len(t, parts, 3)
	spliced := strings.Join([]string{headerSegment(t, "ES256", victimBlock.Bytes), parts[1], parts[2]}, ".")

	v, err := NewVerifier(ca.pem, testAudience)
	require.NoError(t, err)
	_, err = v.Verify(spliced)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signature")
}

func TestMismatchedCertAndKeyIsCaughtLocally(t *testing.T) {
	ca := newTestCA(t)
	certPEM, _ := ca.issue(t, certOpts{})
	_, otherKeyPEM := ca.issue(t, certOpts{})

	_, err := NewSigner(certPEM, otherKeyPEM)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")
}

func TestMalformedCredentials(t *testing.T) {
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	valid, err := signer.Sign(testAudience, testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)
	parts := strings.Split(valid, ".")

	for name, cred := range map[string]string{
		"empty":         "",
		"one segment":   parts[0],
		"two segments":  parts[0] + "." + parts[1],
		"four segments": valid + ".extra",
		"bad base64":    "!!!." + parts[1] + "." + parts[2],
		"not a cert":    headerSegment(t, "ES256", []byte("hello")) + "." + parts[1] + "." + parts[2],
		"empty payload": parts[0] + ".." + parts[2],
		"no signature":  parts[0] + "." + parts[1] + ".",
	} {
		t.Run(name, func(t *testing.T) {
			v, err := NewVerifier(ca.pem, testAudience)
			require.NoError(t, err)
			_, err = v.Verify(cred)
			assert.Error(t, err)
		})
	}
}

func TestVerifierRequiresCAAndAudience(t *testing.T) {
	ca := newTestCA(t)

	_, err := NewVerifier(ca.pem, "")
	assert.Error(t, err, "an empty audience would accept assertions minted for any gateway")

	_, err = NewVerifier([]byte("not a pem file"), testAudience)
	assert.Error(t, err)
}

func TestSignerRejectsIncompleteArguments(t *testing.T) {
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)

	_, err = signer.Sign("", testWGKey, DefaultAssertionLifetime)
	assert.Error(t, err)
	_, err = signer.Sign(testAudience, "", DefaultAssertionLifetime)
	assert.Error(t, err)
	_, err = signer.Sign(testAudience, testWGKey, 0)
	assert.Error(t, err)

	_, err = NewSigner([]byte("nope"), keyPEM)
	assert.Error(t, err)
	_, err = NewSigner(certPEM, []byte("nope"))
	assert.Error(t, err)
}

// headerSegment returns a base64url-encoded JWT header for alg, carrying
// certDER in x5c when non-nil.
func headerSegment(t *testing.T, alg string, certDER []byte) string {
	t.Helper()
	h := map[string]interface{}{"alg": alg, "typ": credentialType}
	if certDER != nil {
		h["x5c"] = []string{base64.StdEncoding.EncodeToString(certDER)}
	}
	b, err := json.Marshal(h)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(b)
}

func TestHostileHeadersAreRejected(t *testing.T) {
	// The JWT header is attacker-controlled. Nothing in it may pick the
	// verification key or the algorithm: the key comes from a certificate that
	// chains to our CA, and the algorithm must be one that key supports.
	ca := newTestCA(t)
	certPEM, keyPEM := ca.issue(t, certOpts{})
	signer, err := NewSigner(certPEM, keyPEM)
	require.NoError(t, err)
	valid, err := signer.Sign(testAudience, testWGKey, DefaultAssertionLifetime)
	require.NoError(t, err)
	parts := strings.Split(valid, ".")
	require.Len(t, parts, 3)
	certBlock, _ := pem.Decode(certPEM)
	require.NotNil(t, certBlock)
	keyBlock, _ := pem.Decode(keyPEM)
	require.NotNil(t, keyBlock)
	key, err := parsePrivateKey(keyBlock.Bytes)
	require.NoError(t, err)

	withHeader := func(alg string, extra map[string]interface{}) string {
		t.Helper()
		token := jwt.NewWithClaims(jwt.GetSigningMethod(alg), &claims{
			RegisteredClaims: jwt.RegisteredClaims{
				Audience:  jwt.ClaimStrings{testAudience},
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(DefaultAssertionLifetime)),
			},
			WireGuardPublicKey: testWGKey,
		})
		token.Header["typ"] = credentialType
		token.Header["x5c"] = []string{base64.StdEncoding.EncodeToString(certBlock.Bytes)}
		for k, v := range extra {
			token.Header[k] = v
		}
		var signingKey interface{} = key
		switch alg {
		case "none":
			signingKey = jwt.UnsafeAllowNoneSignatureType
		case "HS256":
			signingKey = []byte("a secret the presenter chose")
		}
		cred, err := token.SignedString(signingKey)
		require.NoError(t, err)
		return cred
	}

	// A P-384 certificate the CA did sign, presented with the matching ES384
	// token: a valid signature by a valid certificate, but not the one
	// algorithm the verifier is pinned to.
	p384Cert, p384Key := ca.issue(t, certOpts{curve: elliptic.P384()})
	es384 := func() string {
		cb, _ := pem.Decode(p384Cert)
		require.NotNil(t, cb)
		kb, _ := pem.Decode(p384Key)
		require.NotNil(t, kb)
		p384, err := parsePrivateKey(kb.Bytes)
		require.NoError(t, err)
		token := jwt.NewWithClaims(jwt.SigningMethodES384, &claims{
			RegisteredClaims: jwt.RegisteredClaims{
				Audience:  jwt.ClaimStrings{testAudience},
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(DefaultAssertionLifetime)),
			},
			WireGuardPublicKey: testWGKey,
		})
		token.Header["typ"] = credentialType
		token.Header["x5c"] = []string{base64.StdEncoding.EncodeToString(cb.Bytes)}
		cred, err := token.SignedString(p384)
		require.NoError(t, err)
		return cred
	}()

	for name, tc := range map[string]struct {
		cred string
		want string
	}{
		"alg none": {
			cred: withHeader("none", nil),
			want: "signature",
		},
		"ES384 with a P-384 certificate": {
			cred: es384,
			want: "signature",
		},
		"HMAC with a presenter-chosen key": {
			cred: withHeader("HS256", nil),
			want: "signature",
		},
		"no certificate": {
			cred: headerSegment(t, "ES256", nil) + "." + parts[1] + "." + parts[2],
			want: "certificate",
		},
		// x5c is not a chain here: the CA is pinned, so anything beyond the
		// user certificate is something to distrust rather than verify.
		"two certificates": {
			cred: headerWithX5C(t, certBlock.Bytes, certBlock.Bytes) + "." + parts[1] + "." + parts[2],
			want: "exactly one",
		},
		"wrong type": {
			cred: withHeader("ES256", map[string]interface{}{"typ": "JWT"}),
			want: "type",
		},
		// A jwk/jku header naming the attacker's key must be ignored in
		// favor of the certificate: the signature is by the wrong key.
		"header-supplied key": {
			cred: headerSegment(t, "ES256", certBlock.Bytes) + "." + parts[1] + "." + strings.Repeat("A", len(parts[2])),
			want: "signature",
		},
	} {
		t.Run(name, func(t *testing.T) {
			v, err := NewVerifier(ca.pem, testAudience)
			require.NoError(t, err)
			_, err = v.Verify(tc.cred)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// headerWithX5C returns a base64url-encoded ES256 JWT header carrying certs in
// x5c.
func headerWithX5C(t *testing.T, certs ...[]byte) string {
	t.Helper()
	x5c := make([]string, 0, len(certs))
	for _, c := range certs {
		x5c = append(x5c, base64.StdEncoding.EncodeToString(c))
	}
	b, err := json.Marshal(map[string]interface{}{"alg": "ES256", "typ": credentialType, "x5c": x5c})
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(b)
}
