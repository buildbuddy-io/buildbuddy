package routing_content_addressable_storage_client

import (
	"context"
	"time"

	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type RoutingCASClient struct {
	casClients map[string]repb.ContentAddressableStorageClient
	router     interfaces.CacheRoutingService
}

func NewClient(env environment.Env) *RoutingCASClient {
	return &RoutingCASClient{
		router: env.GetCacheRoutingService(),
	}
}

func (r *RoutingCASClient) FindMissingBlobs(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
	if r.router == nil {
		return nil, status.InternalError("No routing service configured")
	}

	// Get the primary CAS client
	primaryClient, err := r.router.GetPrimaryCASClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}

	// Make synchronous call to primary cache
	response, err := primaryClient.FindMissingBlobs(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	// Schedule asynchronous call to secondary cache with all original digests if any were found
	if len(response.GetMissingBlobDigests()) > 0 {
		go func() {
			secondaryClient, err := r.router.GetSecondaryCASClient(ctx)
			if err != nil {
				// Secondary cache is optional, so we just log the error
				return
			}
			if secondaryClient == nil {
				return
			}
			
			// Use extended context for the secondary cache call
			extendedCtx, cancel := background.ExtendContextForFinalization(ctx, 10*time.Second)
			defer cancel()
			
			// Send all original digests to touch them in the secondary cache
			secondaryClient.FindMissingBlobs(extendedCtx, in, opts...)
		}()
	}

	return response, nil
}

func (r *RoutingCASClient) BatchUpdateBlobs(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
	return nil, status.UnimplementedErrorf("BatchUpdateBlobs RPC is not currently implemented")
}

func (r *RoutingCASClient) BatchReadBlobs(ctx context.Context, in *repb.BatchReadBlobsRequest, opts ...grpc.CallOption) (*repb.BatchReadBlobsResponse, error) {
	return nil, status.UnimplementedErrorf("BatchReadBlobs RPC is not currently implemented")
}

func (r *RoutingCASClient) GetTree(ctx context.Context, in *repb.GetTreeRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[repb.GetTreeResponse], error) {
	return nil, status.UnimplementedErrorf("GetTree RPC is not currently implemented")
}

// SpliceBlob implements remote_execution.ContentAddressableStorageClient.
func (r *RoutingCASClient) SpliceBlob(ctx context.Context, in *repb.SpliceBlobRequest, opts ...grpc.CallOption) (*repb.SpliceBlobResponse, error) {
	return nil, status.UnimplementedErrorf("SpliceBlob RPC is not currently implemented")
}

// SplitBlob implements remote_execution.ContentAddressableStorageClient.
func (r *RoutingCASClient) SplitBlob(ctx context.Context, in *repb.SplitBlobRequest, opts ...grpc.CallOption) (*repb.SplitBlobResponse, error) {
	return nil, status.UnimplementedErrorf("SplitBlob RPC is not currently implemented")
}
