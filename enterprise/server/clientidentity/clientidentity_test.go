package clientidentity_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt/v4"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func newService(t testing.TB, clock clockwork.Clock) *clientidentity.Service {
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))
	si, err := clientidentity.New(clock)
	require.NoError(t, err)
	return si
}

func TestIdentity(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	origin := "space"
	client := "aliens"
	headerValue, err := sis.NewIdentityHeader(&interfaces.ClientIdentity{
		Origin: origin,
		Client: client,
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(authutil.ClientIdentityHeaderName, headerValue))
	ctx, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)

	si, err := sis.IdentityFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, origin, si.Origin)
	require.Equal(t, client, si.Client)
}

func TestDuplicateHeaders(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	origin := "space"
	client := "aliens"
	headerValue, err := sis.NewIdentityHeader(&interfaces.ClientIdentity{
		Origin: origin,
		Client: client,
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	headers := metadata.Pairs(
		authutil.ClientIdentityHeaderName, headerValue,
		authutil.ClientIdentityHeaderName, headerValue)
	ctx := metadata.NewIncomingContext(t.Context(), headers)
	ctx, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)

	si, err := sis.IdentityFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, origin, si.Origin)
	require.Equal(t, client, si.Client)
}

func TestMultipleHeaders(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	headers := metadata.Pairs(
		authutil.ClientIdentityHeaderName, "value1",
		authutil.ClientIdentityHeaderName, "value2")
	ctx := metadata.NewIncomingContext(t.Context(), headers)
	_, err := sis.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestStaleIdentity(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)
	jwt.TimeFunc = func() time.Time {
		return clock.Now()
	}
	t.Cleanup(func() {
		jwt.TimeFunc = time.Now
	})

	origin := "space"
	client := "aliens"
	headerValue, err := sis.NewIdentityHeader(&interfaces.ClientIdentity{
		Origin: origin,
		Client: client,
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	clock.Advance(clientidentity.DefaultExpiration + time.Second)

	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(authutil.ClientIdentityHeaderName, headerValue))
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
}

func TestRequired(t *testing.T) {
	flags.Set(t, "app.client_identity.required", true)

	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	origin := "space"
	client := "aliens"
	headerValue, err := sis.NewIdentityHeader(&interfaces.ClientIdentity{
		Origin: origin,
		Client: client,
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	headers := metadata.Pairs(
		authutil.ClientIdentityHeaderName, headerValue)
	ctx := metadata.NewIncomingContext(t.Context(), headers)
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)

	ctx = metadata.NewIncomingContext(t.Context(), nil)
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
}

func newServiceWithKeys(t testing.TB, clock clockwork.Clock, signingKey, additionalVerificationKey string) *clientidentity.Service {
	flags.Set(t, "app.client_identity.key", signingKey)
	flags.Set(t, "app.client_identity.additional_verification_key", additionalVerificationKey)
	si, err := clientidentity.New(clock)
	require.NoError(t, err)
	return si
}

func TestAdditionalVerificationKey(t *testing.T) {
	clock := clockwork.NewFakeClock()
	// Commas are preserved in both key flags.
	oldKey := "old,key"
	newKey := "new,key"

	oldSigner := newServiceWithKeys(t, clock, oldKey, "")
	newSigner := newServiceWithKeys(t, clock, newKey, "")
	// Rotation phase 1: still signing with the old key, new key trusted.
	oldSignerTrustingNew := newServiceWithKeys(t, clock, oldKey, newKey)
	// Rotation phase 2: signing with the new key, old key still trusted.
	newSignerTrustingOld := newServiceWithKeys(t, clock, newKey, oldKey)

	identity := &interfaces.ClientIdentity{Origin: "internal", Client: "app"}
	oldSignedHeader, err := oldSigner.NewIdentityHeader(identity, clientidentity.DefaultExpiration)
	require.NoError(t, err)
	newSignedHeader, err := newSigner.NewIdentityHeader(identity, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		verifier    *clientidentity.Service
		header      string
		expectValid bool
	}{
		{name: "phase1_accepts_old_signed", verifier: oldSignerTrustingNew, header: oldSignedHeader, expectValid: true},
		{name: "phase1_accepts_new_signed", verifier: oldSignerTrustingNew, header: newSignedHeader, expectValid: true},
		{name: "phase2_accepts_old_signed", verifier: newSignerTrustingOld, header: oldSignedHeader, expectValid: true},
		{name: "phase2_accepts_new_signed", verifier: newSignerTrustingOld, header: newSignedHeader, expectValid: true},
		{name: "old_only_rejects_new_signed", verifier: oldSigner, header: newSignedHeader, expectValid: false},
		{name: "new_only_rejects_old_signed", verifier: newSigner, header: oldSignedHeader, expectValid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(authutil.ClientIdentityHeaderName, tc.header))
			ctx, err := tc.verifier.ValidateIncomingIdentity(ctx)
			if !tc.expectValid {
				require.Error(t, err)
				require.True(t, status.IsPermissionDeniedError(err))
				return
			}
			require.NoError(t, err)
			si, err := tc.verifier.IdentityFromContext(ctx)
			require.NoError(t, err)
			require.Equal(t, identity.Origin, si.Origin)
			require.Equal(t, identity.Client, si.Client)
		})
	}
}

func TestAdditionalVerificationKey_RejectsUnknownKey(t *testing.T) {
	clock := clockwork.NewFakeClock()
	keys := make([]string, 3)
	for i := range keys {
		k, err := random.RandomString(16)
		require.NoError(t, err)
		keys[i] = string(k)
	}

	unknownSigner := newServiceWithKeys(t, clock, keys[2], "")
	header, err := unknownSigner.NewIdentityHeader(&interfaces.ClientIdentity{Origin: "internal", Client: "app"}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	verifier := newServiceWithKeys(t, clock, keys[0], keys[1])
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(authutil.ClientIdentityHeaderName, header))
	_, err = verifier.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestAdditionalVerificationKey_ExpiredTokenReportsExpiry(t *testing.T) {
	clock := clockwork.NewFakeClock()
	jwt.TimeFunc = func() time.Time {
		return clock.Now()
	}
	t.Cleanup(func() {
		jwt.TimeFunc = time.Now
	})

	oldKey, err := random.RandomString(16)
	require.NoError(t, err)
	newKey, err := random.RandomString(16)
	require.NoError(t, err)

	oldSigner := newServiceWithKeys(t, clock, string(oldKey), "")
	header, err := oldSigner.NewIdentityHeader(&interfaces.ClientIdentity{Origin: "internal", Client: "app"}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	clock.Advance(clientidentity.DefaultExpiration + time.Second)

	// The expired token was signed with a trusted (additional) key; the error
	// should surface the expiry rather than a signature mismatch from the
	// other keys that were tried.
	verifier := newServiceWithKeys(t, clock, string(newKey), string(oldKey))
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(authutil.ClientIdentityHeaderName, header))
	_, err = verifier.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expired")
}

func TestClearIdentity(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	headerValue, err := sis.NewIdentityHeader(&interfaces.ClientIdentity{
		Origin: "origin",
		Client: "client",
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(authutil.ClientIdentityHeaderName, headerValue))
	ctx, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)
	_, err = sis.IdentityFromContext(ctx)
	require.NoError(t, err)

	ctx = clientidentity.ClearIdentity(ctx)
	_, err = sis.IdentityFromContext(ctx)
	require.Error(t, err)
}

func TestAddIdentityToContext_PreservesExisting(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	existingHeader, err := sis.NewIdentityHeader(&interfaces.ClientIdentity{
		Origin: "upstream-origin",
		Client: "upstream",
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(authutil.ClientIdentityHeaderName, existingHeader))
	ctx, err = sis.AddIdentityToContext(ctx)
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	vals := md.Get(authutil.ClientIdentityHeaderName)
	require.Equal(t, 1, len(vals), "expected only the original header")
	require.Equal(t, existingHeader, vals[0])
}

func TestAddIdentityToContext_AddsWhenMissing(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)
	flags.Set(t, "app.client_identity.client", "local")
	flags.Set(t, "app.client_identity.origin", "local-origin")

	ctx := t.Context()
	ctx, err := sis.AddIdentityToContext(ctx)
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	vals := md.Get(authutil.ClientIdentityHeaderName)
	require.Equal(t, 1, len(vals), "expected a new header to be added")
}

func TestCachedIdentityHeader(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	si := &interfaces.ClientIdentity{Origin: "internal", Client: "grpc-proxy"}

	h1, err := sis.CachedIdentityHeader(si)
	require.NoError(t, err)

	// Within the cache TTL, the same header is returned without re-signing
	// (a re-sign at a later time would change the JWT's expiration claim).
	clock.Advance(30 * time.Second)
	h2, err := sis.CachedIdentityHeader(si)
	require.NoError(t, err)
	require.Equal(t, h1, h2)

	// A different identity is cached independently.
	other, err := sis.CachedIdentityHeader(&interfaces.ClientIdentity{Origin: "internal", Client: "app"})
	require.NoError(t, err)
	require.NotEqual(t, h1, other)

	// Past the cache TTL, the header is re-signed.
	clock.Advance(2 * time.Minute)
	h3, err := sis.CachedIdentityHeader(si)
	require.NoError(t, err)
	require.NotEqual(t, h1, h3)

	// The cached header is a valid identity for the requested client.
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(authutil.ClientIdentityHeaderName, h3))
	ctx, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)
	got, err := sis.IdentityFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, "grpc-proxy", got.Client)
	require.Equal(t, "internal", got.Origin)
}

func TestNew_RejectsExpirationBelowCacheWindow(t *testing.T) {
	key, err := random.RandomString(16)
	require.NoError(t, err)
	flags.Set(t, "app.client_identity.key", string(key))

	// A JWT lifetime shorter than the header cache window would let the cache
	// serve an already-expired token, so New must reject it.
	flags.Set(t, "app.client_identity.expiration", 30*time.Second)
	_, err = clientidentity.New(clockwork.NewFakeClock())
	require.Error(t, err)
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %v", err)
}

func BenchmarkAddIdentityToContext(b *testing.B) {
	sis := newService(b, clockwork.NewRealClock())

	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := sis.AddIdentityToContext(ctx)
			require.NoError(b, err)
		}
	})
}
