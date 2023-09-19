package clientidentity_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func newService(t *testing.T, clock clockwork.Clock) *clientidentity.Service {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	bs, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	privateKeyBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: bs,
	})
	flags.Set(t, "app.client_identity.private_key", string(privateKeyBytes))

	bs, err = x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	publicKeyBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "BEGIN PUBLIC KEY",
		Bytes: bs,
	})
	flags.Set(t, "app.client_identity.public_key", string(publicKeyBytes))

	si, err := clientidentity.New(clock)
	require.NoError(t, err)
	return si
}

func TestIdentity(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	origin := "space"
	client := "aliens"
	headerValue, err := sis.IdentityHeader(&interfaces.ServerIdentity{
		Origin: origin,
		Client: client,
	})
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clientidentity.IdentityHeaderName, headerValue))
	ctx, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)

	si, err := sis.IdentityFromContext(ctx)
	require.NoError(t, err)
	require.Equal(t, origin, si.Origin)
	require.Equal(t, client, si.Client)

	signature := ""
	timestamp := int64(0)
	for _, kvPair := range strings.Split(headerValue, ";") {
		pts := strings.Split(strings.TrimSpace(kvPair), "=")
		switch pts[0] {
		case "signature":
			signature = pts[1]
		case "timestamp":
			timestamp, err = strconv.ParseInt(pts[1], 10, 64)
			require.NoError(t, err)
		}
	}
	require.NotEmpty(t, signature, "could not find signature")

	md := metadata.MD{}
	// Valid header, same as above.
	md.Set(clientidentity.IdentityHeaderName, fmt.Sprintf("origin=%s; client=%s; timestamp=%d; signature=%s", origin, client, timestamp, signature))
	ctx = metadata.NewIncomingContext(context.Background(), md)
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)

	// Modified origin, verification should fail.
	md.Set(clientidentity.IdentityHeaderName, fmt.Sprintf("origin=earth; client=%s; timestamp=%d; signature=%s", client, timestamp, signature))
	ctx = metadata.NewIncomingContext(context.Background(), md)
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.ErrorContains(t, err, "could not verify signature")

	// Modified client, verification should fail.
	md.Set(clientidentity.IdentityHeaderName, fmt.Sprintf("origin=%s; client=zelda; timestamp=%d; signature=%s", origin, timestamp, signature))
	ctx = metadata.NewIncomingContext(context.Background(), md)
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.ErrorContains(t, err, "could not verify signature")

	// Modified timestamp, verification should fail.
	md.Set(clientidentity.IdentityHeaderName, fmt.Sprintf("origin=%s; client=%s; timestamp=%d; signature=%s", origin, client, timestamp+1, signature))
	ctx = metadata.NewIncomingContext(context.Background(), md)
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.ErrorContains(t, err, "could not verify signature")
}

func TestStaleIdentity(t *testing.T) {
	clock := clockwork.NewFakeClock()
	sis := newService(t, clock)

	origin := "space"
	client := "aliens"
	headerValue, err := sis.IdentityHeader(&interfaces.ServerIdentity{
		Origin: origin,
		Client: client,
	})
	require.NoError(t, err)

	clock.Advance(clientidentity.DefaultAgeTolerance)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clientidentity.IdentityHeaderName, headerValue))
	_, err = sis.ValidateIncomingIdentity(ctx)
	require.Error(t, err)
}
