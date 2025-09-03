package routing_content_addressable_storage_client

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type RoutingCASClient struct {
	casClients               map[string]repb.ContentAddressableStorageClient
	router                   interfaces.CacheRoutingService
	findMissingBatchOperator batch_operator.BatchDigestOperator
}

func newFindMissingBatcher(env environment.Env) batch_operator.BatchDigestOperator {
  f := func(ctx context.Context, u *batch_operator.DigestBatch) error {
	client, err := env.GetCacheRoutingService().GetSecondaryCASClient(ctx)
	if err != nil {
		return err
	}
	if client == nil {
		// XXX: Shouldn't happen. include groupid.
		return status.InternalError("Failed to find secondary cache route for batch..")
	}
	req := &repb.FindMissingBlobsRequest{
		InstanceName:   u.InstanceName,
		BlobDigests:    make([]*repb.Digest, len(u.Digests)),
		DigestFunction: u.DigestFunction,
	}
	i := 0
	for d := range u.Digests {
		req.BlobDigests[i] = d.ToDigest()
		i++
	}
	_, err = client.FindMissingBlobs(ctx, req)
	// XXX: schedule copy for missing stuff that's in original?
	return err
  }

  operator, err := batch_operator.New(env, f, batch_operator.BatchDigestOperatorConfig{})
  if err != nil {
	panic(err)
  }
  operator.Start(env.GetHealthChecker())
  return operator
}

func NewClient(env environment.Env) *RoutingCASClient {
	return &RoutingCASClient{
		router: env.GetCacheRoutingService(),
		findMissingBatchOperator: newFindMissingBatcher(env),
	}
}

func (r *RoutingCASClient) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
	if r.router == nil {
		return nil, status.InternalError("No routing service configured")
	}

	// Get the primary CAS client
	primaryClient, err := r.router.GetPrimaryCASClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}

	// Make synchronous call to primary cache
	response, err := primaryClient.FindMissingBlobs(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	secondaryClient, err := r.router.GetSecondaryCASClient(ctx)
	if err == nil && secondaryClient != nil{
		// XXX: Logging in error case?
		r.findMissingBatchOperator.Enqueue(ctx, req.GetInstanceName(), req.GetBlobDigests(), req.GetDigestFunction())
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
