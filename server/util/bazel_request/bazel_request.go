package bazel_request

import (
	"context"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const RequestMetadataKey = "build.bazel.remote.execution.v2.requestmetadata-bin"

func GetRequestMetadata(ctx context.Context) *repb.RequestMetadata {
	if grpcMD, ok := metadata.FromIncomingContext(ctx); ok {
		rmdVals := grpcMD[RequestMetadataKey]
		for _, rmdVal := range rmdVals {
			rmd := &repb.RequestMetadata{}
			if err := proto.Unmarshal([]byte(rmdVal), rmd); err == nil {
				return rmd
			}
		}
	}
	return nil
}

func GetInvocationID(ctx context.Context) string {
	iid := ""
	if rmd := GetRequestMetadata(ctx); rmd != nil {
		iid = rmd.GetToolInvocationId()
	}
	return iid
}
