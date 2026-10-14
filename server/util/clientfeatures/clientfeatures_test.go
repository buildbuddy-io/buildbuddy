package clientfeatures_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/clientfeatures"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// asIncoming turns the outgoing metadata on ctx into the incoming metadata a
// server would see for the same RPC.
func asIncoming(t *testing.T, ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok, "expected outgoing metadata")
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestAdvertiseRoundTrip(t *testing.T) {
	ctx := clientfeatures.Advertise(context.Background(), clientfeatures.ByteStreamWriteAlreadyExists)

	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	require.Equal(t, []string{clientfeatures.ByteStreamWriteAlreadyExists}, md.Get(clientfeatures.HeaderName))

	in := asIncoming(t, ctx)
	require.True(t, clientfeatures.Supports(in, clientfeatures.ByteStreamWriteAlreadyExists))
	require.False(t, clientfeatures.Supports(in, "some-other-feature"))
}

func TestAdvertiseMultipleFeatures(t *testing.T) {
	// Several features in one call are joined into one header value.
	ctx := clientfeatures.Advertise(context.Background(), "feature-a", "feature-b")
	in := asIncoming(t, ctx)
	require.True(t, clientfeatures.Supports(in, "feature-a"))
	require.True(t, clientfeatures.Supports(in, "feature-b"))
	require.False(t, clientfeatures.Supports(in, "feature-a,feature-b"), "the joined list is not itself a feature")

	// Separate calls add header values; earlier features are kept.
	ctx = clientfeatures.Advertise(ctx, "feature-c")
	in = asIncoming(t, ctx)
	require.True(t, clientfeatures.Supports(in, "feature-a"))
	require.True(t, clientfeatures.Supports(in, "feature-c"))
}

func TestAdvertiseNothing(t *testing.T) {
	ctx := clientfeatures.Advertise(context.Background())
	_, ok := metadata.FromOutgoingContext(ctx)
	require.False(t, ok, "advertising no features must not add a header")
}

func TestSupportsWithoutHeader(t *testing.T) {
	require.False(t, clientfeatures.Supports(context.Background(), clientfeatures.ByteStreamWriteAlreadyExists))
	in := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-other-header", "value"))
	require.False(t, clientfeatures.Supports(in, clientfeatures.ByteStreamWriteAlreadyExists))
}

func TestSupportsTolerantParsing(t *testing.T) {
	// Values written by other implementations may contain spaces, empty
	// entries or different casing of the key.
	in := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"X-BuildBuddy-Client-Features", " feature-a , ,feature-b ",
	))
	require.True(t, clientfeatures.Supports(in, "feature-a"))
	require.True(t, clientfeatures.Supports(in, "feature-b"))
	require.False(t, clientfeatures.Supports(in, ""), "an empty token is not a feature")
	// Tokens are case-sensitive.
	require.False(t, clientfeatures.Supports(in, "Feature-A"))
}
