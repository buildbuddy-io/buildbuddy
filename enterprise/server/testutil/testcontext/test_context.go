package testcontext

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/v2/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/v2/server/util/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/v2/proto/remote_execution"
)

func AttachInvocationIDToContext(t *testing.T, ctx context.Context, invocationID string) context.Context {
	requestMetadata := &repb.RequestMetadata{
		ToolInvocationId: invocationID,
	}
	mdBytes, err := proto.Marshal(requestMetadata)
	require.NoError(t, err)

	res := metadata.AppendToOutgoingContext(ctx, bazel_request.RequestMetadataKey, string(mdBytes))
	return res
}
