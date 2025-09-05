package routing_byte_stream_client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type RoutingByteStreamClient struct {
	acClient map[string]bspb.ByteStreamClient
	router   interfaces.CacheRoutingService
}

func NewClient(env environment.Env) bspb.ByteStreamClient {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		panic("No routing service configured.")
	}

	return &RoutingByteStreamClient{
		router: routingService,
	}
}

func (r *RoutingByteStreamClient) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest, opts ...grpc.CallOption) (*bspb.QueryWriteStatusResponse, error) {
	primaryClient, err := r.router.GetPrimaryBSClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	return primaryClient.QueryWriteStatus(ctx, req, opts...)
}

func (r *RoutingByteStreamClient) Read(ctx context.Context, req *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	primaryClient, err := r.router.GetPrimaryBSClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	return primaryClient.Read(ctx, req, opts...)
}

func (r *RoutingByteStreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	primaryClient, err := r.router.GetPrimaryBSClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	return primaryClient.Write(ctx, opts...)
}
