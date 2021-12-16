package testcontext

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func AttachInvocationIDToContext(ctx context.Context, invocationID string) context.Context {
	requestMetadata := &repb.RequestMetadata{
		ToolInvocationId: invocationID,
	}
	mdBytes, err := proto.Marshal(requestMetadata)
	if err != nil {
		log.Errorf("luluz-Debug: failed to marshal")
	}

	res := metadata.AppendToOutgoingContext(ctx, bazel_request.RequestMetadataKey, string(mdBytes))

	return res
}
