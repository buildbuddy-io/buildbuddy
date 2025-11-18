package routing_content_addressable_storage_client

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/migration_operators"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"

	gstatus "google.golang.org/grpc/status"
)

var (
	casMigrationOpTimeout = flag.Duration("cache_proxy.migration.cas_timeout", 60*time.Second, "Timeout for CAS operations that happen during cache migrations.")
)

type RoutingCASClient struct {
	casClients   map[string]repb.ContentAddressableStorageClient
	router       interfaces.CacheRoutingService
	copyOp       batch_operator.DigestOperator
	readOp       batch_operator.DigestOperator
	readVerifyOp batch_operator.DigestOperator
	treeOp       batch_operator.DigestOperator
}

func New(env environment.Env) (*RoutingCASClient, error) {

	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, status.FailedPreconditionError("An authenticator is required to route and migrate between caches.")
	}

	bytestreamCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ByteStreamCopy(ctx, env.GetCacheRoutingService(), groupID, b)
	}
	bsCopyOp := batch_operator.NewImmediateDigestOperator(authenticator, "bytestream-copy", bytestreamCopyClosure, *casMigrationOpTimeout)

	casCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.CASBatchCopy(ctx, env.GetCacheRoutingService(), groupID, b)
	}
	casCopyOp := batch_operator.NewImmediateDigestOperator(authenticator, "cas-copy", casCopyClosure, *casMigrationOpTimeout)

	routedCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.RoutedCopy(ctx, groupID, casCopyOp, bsCopyOp, b)
	}
	routedCopyOp := batch_operator.NewImmediateDigestOperator(authenticator, "routed-copy", routedCopyClosure, *casMigrationOpTimeout)

	casReadClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.CASBatchReadAndVerify(ctx, env.GetCacheRoutingService(), false, groupID, b)
	}
	casReadAndVerifyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.CASBatchReadAndVerify(ctx, env.GetCacheRoutingService(), true, groupID, b)
	}
	treeMirrorClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.GetTreeMirrorOperator(ctx, env.GetCacheRoutingService(), routedCopyOp, groupID, b)
	}

	readOp := batch_operator.NewImmediateDigestOperator(authenticator, "cas-read", casReadClosure, *casMigrationOpTimeout)
	readVerifyOp := batch_operator.NewImmediateDigestOperator(authenticator, "cas-read-verify", casReadAndVerifyClosure, *casMigrationOpTimeout)
	treeOp := batch_operator.NewImmediateDigestOperator(authenticator, "cas-tree-mirror", treeMirrorClosure, *casMigrationOpTimeout)

	return &RoutingCASClient{
		router:       routingService,
		copyOp:       casCopyOp,
		readOp:       readOp,
		readVerifyOp: readVerifyOp,
		treeOp:       treeOp,
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
	primaryClient, secondaryClient, err := r.router.GetCASClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary CAS client: %s", err)
	}
	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		// This should never happen, but if it does, we at least know where the
		// primary request should go, so let's send that.
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return primaryClient.BatchUpdateBlobs(ctx, req, opts...)
	}

	singleRandValue := rand.Float64()
	dualWrite := singleRandValue < c.GetDualWriteFraction()
	if dualWrite && secondaryClient != nil {
		var wg sync.WaitGroup
		wg.Go(func() {
			secondaryCtx := usageutil.DisableUsageTracking(ctx)
			results, err := secondaryClient.BatchUpdateBlobs(secondaryCtx, req, opts...)
			if err != nil {
				log.CtxInfof(secondaryCtx, "synchronous secondary cas write failed: %s", err)
				metrics.ProxySecondarySyncWriteDigests.WithLabelValues(
					"cas_server",
					gstatus.Code(err).String(),
				).Add(float64(len(req.GetRequests())))
				return
			}
			statusTotals := make(map[string]int64)
			for _, r := range results.GetResponses() {
				statusTotals[gstatus.FromProto(r.GetStatus()).Code().String()] += 1
			}
			for s, count := range statusTotals {
				metrics.ProxySecondarySyncWriteDigests.WithLabelValues(
					"cas_server",
					s,
				).Add(float64(count))
			}
		})
		defer wg.Wait()
		return primaryClient.BatchUpdateBlobs(ctx, req, opts...)
	}

	rsp, err := primaryClient.BatchUpdateBlobs(ctx, req, opts...)
	if err != nil {
		return rsp, err
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
