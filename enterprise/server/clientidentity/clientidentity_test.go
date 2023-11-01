package clientidentity_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func newService(t *testing.T, clock clockwork.Clock) *clientidentity.Service {
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

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clientidentity.IdentityHeaderName, headerValue))
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
		clientidentity.IdentityHeaderName, headerValue,
		clientidentity.IdentityHeaderName, headerValue)
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
		clientidentity.IdentityHeaderName, "value1",
		clientidentity.IdentityHeaderName, "value2")
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

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clientidentity.IdentityHeaderName, headerValue))
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
}
