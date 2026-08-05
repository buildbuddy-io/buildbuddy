package findmissing_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/findmissing"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestPurposeRoundTrip(t *testing.T) {
	ctx := findmissing.ContextWithPurpose(context.Background(), repb.FindMissingBlobsRequest_WRITE_DEDUPE)
	require.Equal(t, repb.FindMissingBlobsRequest_WRITE_DEDUPE, findmissing.PurposeFromContext(ctx))
}

func TestPurposeDefaultsToUnknown(t *testing.T) {
	require.Equal(t, repb.FindMissingBlobsRequest_UNKNOWN, findmissing.PurposeFromContext(context.Background()))
}

func TestPurposeOverwrite(t *testing.T) {
	ctx := findmissing.ContextWithPurpose(context.Background(), repb.FindMissingBlobsRequest_CONTAINS)
	ctx = findmissing.ContextWithPurpose(ctx, repb.FindMissingBlobsRequest_ATIME_UPDATE)
	require.Equal(t, repb.FindMissingBlobsRequest_ATIME_UPDATE, findmissing.PurposeFromContext(ctx))
}
