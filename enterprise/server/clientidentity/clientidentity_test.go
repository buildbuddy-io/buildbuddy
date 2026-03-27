package clientidentity_test

import (
	"context"
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
	headerValue, err := sis.IdentityHeader(&interfaces.ClientIdentity{
		Origin: origin,
		Client: client,
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, headerValue))
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
	headerValue, err := sis.IdentityHeader(&interfaces.ClientIdentity{
		Origin: origin,
		Client: client,
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	headers := metadata.Pairs(
		authutil.ClientIdentityHeaderName, headerValue,
		authutil.ClientIdentityHeaderName, headerValue)
	ctx := metadata.NewIncomingContext(context.Background(), headers)
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
	ctx := metadata.NewIncomingContext(context.Background(), headers)
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
	headerValue, err := sis.IdentityHeader(&interfaces.ClientIdentity{
		Origin: origin,
		Client: client,
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	clock.Advance(clientidentity.DefaultExpiration + time.Second)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, headerValue))
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
}

func TestRequired(t *testing.T) {
	flags.Set(t, "app.client_identity.required", true)

	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	origin := "space"
	client := "aliens"
	headerValue, err := sis.IdentityHeader(&interfaces.ClientIdentity{
		Origin: origin,
		Client: client,
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	headers := metadata.Pairs(
		authutil.ClientIdentityHeaderName, headerValue)
	ctx := metadata.NewIncomingContext(context.Background(), headers)
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)

	ctx = metadata.NewIncomingContext(context.Background(), nil)
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
}

func TestClearIdentity(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	headerValue, err := sis.IdentityHeader(&interfaces.ClientIdentity{
		Origin: "origin",
		Client: "client",
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, headerValue))
	ctx, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)
	_, err = sis.IdentityFromContext(ctx)
	require.NoError(t, err)

	ctx = clientidentity.ClearIdentity(ctx)
	_, err = sis.IdentityFromContext(ctx)
	require.Error(t, err)
}

func TestOverridePropagated_DefaultTrue(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)
	flags.Set(t, "app.client_identity.client", "local")
	flags.Set(t, "app.client_identity.origin", "local-origin")

	// Start with an existing identity header in the outgoing context.
	existingHeader, err := sis.IdentityHeader(&interfaces.ClientIdentity{
		Origin: "upstream-origin",
		Client: "upstream",
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, existingHeader))
	ctx, err = sis.AddIdentityToContext(ctx)
	require.NoError(t, err)

	// Default behavior (override_propagated=true): a new header should be appended.
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	vals := md.Get(authutil.ClientIdentityHeaderName)
	require.Equal(t, 2, len(vals), "expected both old and new headers")
}

func TestOverridePropagated_False_PreservesExisting(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)
	flags.Set(t, "app.client_identity.override_propagated", false)
	flags.Set(t, "app.client_identity.client", "local")
	flags.Set(t, "app.client_identity.origin", "local-origin")

	existingHeader, err := sis.IdentityHeader(&interfaces.ClientIdentity{
		Origin: "upstream-origin",
		Client: "upstream",
	}, clientidentity.DefaultExpiration)
	require.NoError(t, err)

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(authutil.ClientIdentityHeaderName, existingHeader))
	ctx, err = sis.AddIdentityToContext(ctx)
	require.NoError(t, err)

	// With override_propagated=false, the existing header should be preserved and no new one added.
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	vals := md.Get(authutil.ClientIdentityHeaderName)
	require.Equal(t, 1, len(vals), "expected only the original header")
	require.Equal(t, existingHeader, vals[0])
}

func TestOverridePropagated_False_AddsWhenMissing(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)
	flags.Set(t, "app.client_identity.override_propagated", false)
	flags.Set(t, "app.client_identity.client", "local")
	flags.Set(t, "app.client_identity.origin", "local-origin")

	// No existing identity header in the outgoing context.
	ctx := context.Background()
	ctx, err := sis.AddIdentityToContext(ctx)
	require.NoError(t, err)

	// Even with override_propagated=false, a header should be added when none exists.
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	vals := md.Get(authutil.ClientIdentityHeaderName)
	require.Equal(t, 1, len(vals), "expected a new header to be added")
}

func BenchmarkAddIdentityToContext(b *testing.B) {
	sis := newService(b, clockwork.NewRealClock())

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := sis.AddIdentityToContext(ctx)
			require.NoError(b, err)
		}
	})
}
