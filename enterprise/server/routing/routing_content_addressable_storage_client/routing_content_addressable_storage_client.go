package routing_content_addressable_storage_client

import (
	"context"

	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type RoutingCASClient struct {
	casClients map[string]repb.ContentAddressableStorageClient
	router     interfaces.CacheRoutingService
}

func New(env environment.Env) (*RoutingCASClient, error) {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	return &RoutingCASClient{
		router: routingService,
	}, nil
}

func (r *RoutingCASClient) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	return primaryClient.FindMissingBlobs(ctx, req, opts...)
}

func (r *RoutingCASClient) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	return primaryClient.BatchUpdateBlobs(ctx, req, opts...)
}

func (r *RoutingCASClient) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest, opts ...grpc.CallOption) (*repb.BatchReadBlobsResponse, error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	return primaryClient.BatchReadBlobs(ctx, req, opts...)
}

func (r *RoutingCASClient) GetTree(ctx context.Context, req *repb.GetTreeRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[repb.GetTreeResponse], error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	return primaryClient.GetTree(ctx, req, opts...)
}

func (r *RoutingCASClient) SpliceBlob(ctx context.Context, req *repb.SpliceBlobRequest, opts ...grpc.CallOption) (*repb.SpliceBlobResponse, error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	return primaryClient.SpliceBlob(ctx, req, opts...)
}

func (r *RoutingCASClient) SplitBlob(ctx context.Context, req *repb.SplitBlobRequest, opts ...grpc.CallOption) (*repb.SplitBlobResponse, error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	return primaryClient.SplitBlob(ctx, req, opts...)
}
