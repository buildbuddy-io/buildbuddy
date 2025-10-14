package routing_content_addressable_storage_client

import (
	"context"
	"math/rand/v2"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type RoutingCASClient struct {
	casClients map[string]repb.ContentAddressableStorageClient
	router     interfaces.CacheRoutingService
	copyOp     batch_operator.DigestOperator
	readOp     batch_operator.DigestOperator
	readVerifyOp     batch_operator.DigestOperator
	treeOp     batch_operator.DigestOperator
}

func New(
	env environment.Env,
	copyOp batch_operator.DigestOperator,
	readOp batch_operator.DigestOperator,
	readVerifyOp batch_operator.DigestOperator,
	treeOp batch_operator.DigestOperator) (*RoutingCASClient, error) {

	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	return &RoutingCASClient{
		router: routingService,
		copyOp: copyOp,
		readOp: readOp,
		readVerifyOp: readVerifyOp,
		treeOp: treeOp,
	}, nil
}

func (r *RoutingCASClient) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	rsp, err := primaryClient.FindMissingBlobs(ctx, req, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}

	if rand.Float64() < c.GetBackgroundCopyFraction() {
		foundDigestsMap := map[string]*repb.Digest{}
		for _, d := range req.BlobDigests {
			foundDigestsMap[d.GetHash()] = d
		}
		for _, missing := range rsp.MissingBlobDigests {
			delete(foundDigestsMap, missing.GetHash())
		}
		digestsToSync := make([]*repb.Digest, 0, len(foundDigestsMap))
		for _, d := range foundDigestsMap {
			digestsToSync = append(digestsToSync, d)
		}
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), digestsToSync, req.GetDigestFunction())
	}
	return rsp, nil
}

func (r *RoutingCASClient) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	rsp, err := primaryClient.BatchUpdateBlobs(ctx, req, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}

	if rand.Float64() < c.GetBackgroundCopyFraction() {
		digestsToSync := make([]*repb.Digest, 0, len(req.GetRequests()))
		for _, d := range req.GetRequests() {
			digestsToSync = append(digestsToSync, d.GetDigest())
		}
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), digestsToSync, req.GetDigestFunction())
	}

	return rsp, nil
}

func (r *RoutingCASClient) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest, opts ...grpc.CallOption) (*repb.BatchReadBlobsResponse, error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	rsp, err := primaryClient.BatchReadBlobs(ctx, req, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}
	singleRandValue := rand.Float64()
	if singleRandValue < c.GetBackgroundReadVerifyFraction() {
		r.readVerifyOp.Enqueue(ctx, req.GetInstanceName(), req.GetDigests(), req.GetDigestFunction())
	} else if singleRandValue < c.GetBackgroundReadFraction() {
		r.readOp.Enqueue(ctx, req.GetInstanceName(), req.GetDigests(), req.GetDigestFunction())
	} else if (singleRandValue) < c.GetBackgroundCopyFraction() {
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), req.GetDigests(), req.GetDigestFunction())
	}

	return rsp, nil
}

func (r *RoutingCASClient) GetTree(ctx context.Context, req *repb.GetTreeRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[repb.GetTreeResponse], error) {
	primaryClient, _, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	rsp, err := primaryClient.GetTree(ctx, req, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}
	if rand.Float64() < c.GetBackgroundCopyFraction() {
		r.treeOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetRootDigest()}, req.GetDigestFunction())
	}
	return rsp, nil
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
