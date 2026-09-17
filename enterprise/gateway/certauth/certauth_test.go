package certauth

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/gateway/gatewayauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauthrelay"
	"github.com/buildbuddy-io/buildbuddy/server/util/relayauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/wgkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testGatewayAudience = "relay.moon.buildbuddy.io"

// newAuthenticator builds an authenticator trusting ca, with the test audience.
func newAuthenticator(t testing.TB, ca *testauthrelay.CA) gatewayauth.Authenticator {
	t.Helper()
	flags.Set(t, "gateway.cert_auth.ca", ca.PEM())
	flags.Set(t, "gateway.cert_auth.audience", testGatewayAudience)
	a, err := New()
	require.NoError(t, err)
	return a
}

func newPubKeyHex(t testing.TB) string {
	t.Helper()
	priv, err := wgkeys.GeneratePrivateKey()
	require.NoError(t, err)
	return priv.PublicKey().Hex()
}

func credCtx(t testing.TB, ca *testauthrelay.CA, email, wgPubKey string) context.Context {
	t.Helper()
	return testauthrelay.CredentialContext(t, ca.Signer(t, email), testGatewayAudience, wgPubKey)
}

func TestAuthenticate_Succeeds(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	pubKey := newPubKeyHex(t)
	p, err := a.Authenticate(credCtx(t, ca, "vadim@buildbuddy.io", pubKey), pubKey)
	require.NoError(t, err)

	// The principal is attributed to the human, so log lines name a person,
	// and lives in a namespace that can never collide with a BuildBuddy group
	// ID.
	assert.Equal(t, "vadim@buildbuddy.io", p.User)
	assert.Equal(t, "cert:vadim@buildbuddy.io", p.Namespace)
	assert.False(t, p.ExpiresAt.IsZero(), "a certificate principal must carry an expiry")
}

func TestAuthenticate_EachEmployeeIsADistinctNamespace(t *testing.T) {
	// Namespaces key network allocation, so distinct namespaces is what gives
	// each employee their own /48 — and workstation-to-workstation isolation.
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	key1, key2 := newPubKeyHex(t), newPubKeyHex(t)
	p1, err := a.Authenticate(credCtx(t, ca, "vadim@buildbuddy.io", key1), key1)
	require.NoError(t, err)
	p2, err := a.Authenticate(credCtx(t, ca, "someone-else@buildbuddy.io", key2), key2)
	require.NoError(t, err)
	assert.NotEqual(t, p1.Namespace, p2.Namespace)

	// The flip side: the same employee's machines share one namespace, so
	// they can see each other.
	key3 := newPubKeyHex(t)
	p3, err := a.Authenticate(credCtx(t, ca, "vadim@buildbuddy.io", key3), key3)
	require.NoError(t, err)
	assert.Equal(t, p1.Namespace, p3.Namespace)
}

func TestAuthenticate_CredentialBoundToADifferentKeyIsRejected(t *testing.T) {
	// Capturing a credential must not let an attacker register a tunnel of
	// their own: the assertion names one WireGuard key.
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	victimKey := newPubKeyHex(t)
	attackerKey := newPubKeyHex(t)
	_, err := a.Authenticate(credCtx(t, ca, "vadim@buildbuddy.io", victimKey), attackerKey)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestAuthenticate_NoKeyNamedSkipsBinding(t *testing.T) {
	// RPCs that don't name a WireGuard key (List) pass "" and are still
	// authenticated; the credential remains audience-scoped and short-lived.
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	pubKey := newPubKeyHex(t)
	_, err := a.Authenticate(credCtx(t, ca, "vadim@buildbuddy.io", pubKey), "")
	require.NoError(t, err)
}

func TestAuthenticate_ExpiredCertIsRejected(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	pubKey := newPubKeyHex(t)
	expired := ca.SignerWithExpiry(t, "vadim@buildbuddy.io", time.Now().Add(-time.Minute))
	ctx := testauthrelay.CredentialContext(t, expired, testGatewayAudience, pubKey)

	_, err := a.Authenticate(ctx, pubKey)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuthenticate_WrongAudienceIsRejected(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	pubKey := newPubKeyHex(t)
	ctx := testauthrelay.CredentialContext(t, ca.Signer(t, "vadim@buildbuddy.io"), "gateway.dev.buildbuddy.io", pubKey)

	_, err := a.Authenticate(ctx, pubKey)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuthenticate_CertFromAnotherCAIsRejected(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	other := testauthrelay.NewCA(t)
	pubKey := newPubKeyHex(t)
	_, err := a.Authenticate(credCtx(t, other, "attacker@buildbuddy.io", pubKey), pubKey)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuthenticate_MalformedCredentialIsRejected(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(relayauth.CredentialHeader, "this-is-not-a-credential"))

	_, err := a.Authenticate(ctx, newPubKeyHex(t))
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuthenticate_MissingCredentialIsRejected(t *testing.T) {
	// There is no other credential to fall back to: no tunnel certificate
	// means no access, whatever else the caller may hold.
	ca := testauthrelay.NewCA(t)
	a := newAuthenticator(t, ca)

	_, err := a.Authenticate(context.Background(), newPubKeyHex(t))
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestNew_RequiresAudience(t *testing.T) {
	ca := testauthrelay.NewCA(t)
	flags.Set(t, "gateway.cert_auth.ca", ca.PEM())

	// Without an audience the gateway would accept credentials minted for any
	// other gateway, so it must refuse to start instead.
	_, err := New()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "audience")
}

func TestNew_RequiresCA(t *testing.T) {
	// A tunnel gateway with no CA cannot authenticate anyone; refuse to start
	// rather than coming up unusable.
	flags.Set(t, "gateway.cert_auth.audience", testGatewayAudience)
	_, err := New()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cert_auth.ca")
}
