package testcontext

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
