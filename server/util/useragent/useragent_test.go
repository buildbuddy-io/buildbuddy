package useragent_test

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/util/useragent"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestGetUnset(t *testing.T) {
	require.Equal(t, "", useragent.Get(context.Background()))
}

func TestSetFromHeader(t *testing.T) {
	ctx := useragent.SetFromHeader(context.Background(), "Mozilla/5.0 (X11; Linux x86_64)")
	require.Equal(t, "Mozilla/5.0 (X11; Linux x86_64)", useragent.Get(ctx))
}

func TestSetFromEmptyHeaderLeavesContextUnset(t *testing.T) {
	ctx := useragent.SetFromHeader(context.Background(), "")
	require.Equal(t, "", useragent.Get(ctx))
}

func TestLongHeaderIsTruncated(t *testing.T) {
	ctx := useragent.SetFromHeader(context.Background(), strings.Repeat("a", 10_000))
	require.Len(t, useragent.Get(ctx), useragent.MaxLength)
}

func TestTruncationDoesNotSplitRunes(t *testing.T) {
	// A multi-byte rune straddling the truncation boundary must be dropped
	// rather than cut in half, since the result is written to a DB column.
	// "€" is 3 bytes, which doesn't divide MaxLength evenly, so the byte-wise
	// cut necessarily lands mid-rune.
	ctx := useragent.SetFromHeader(context.Background(), strings.Repeat("€", 10_000))
	ua := useragent.Get(ctx)
	require.True(t, utf8.ValidString(ua))
	require.Equal(t, useragent.MaxLength-useragent.MaxLength%3, len(ua))
}

func TestInvalidUTF8IsStripped(t *testing.T) {
	ctx := useragent.SetFromHeader(context.Background(), "curl/8.4\xff\xfe")
	require.Equal(t, "curl/8.4", useragent.Get(ctx))
}

func TestFallsBackToGRPCMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("user-agent", "grpc-go/1.60.0"))
	require.Equal(t, "grpc-go/1.60.0", useragent.Get(ctx))
}

func TestContextValueTakesPrecedenceOverGRPCMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("user-agent", "grpc-go/1.60.0"))
	ctx = useragent.SetFromHeader(ctx, "curl/8.4")
	require.Equal(t, "curl/8.4", useragent.Get(ctx))
}
