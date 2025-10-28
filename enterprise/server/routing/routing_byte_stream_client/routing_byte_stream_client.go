package routing_byte_stream_client

import (
	"context"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/migration_operators"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	bytestreamMigrationOpTimeout = flag.Duration("cache_proxy.migration.bytestream_timeout", 60*time.Second, "Timeout for bytestream operations that happen during cache migrations.")
)

type RoutingByteStreamClient struct {
	acClient     map[string]bspb.ByteStreamClient
	router       interfaces.CacheRoutingService
	copyOp       batch_operator.DigestOperator
	readOp       batch_operator.DigestOperator
	readVerifyOp batch_operator.DigestOperator
}

func New(env environment.Env) (bspb.ByteStreamClient, error) {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	bytestreamCopyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ByteStreamCopy(ctx, env.GetCacheRoutingService(), groupID, b)
	}
	bytestreamReadClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ByteStreamReadAndVerify(ctx, env.GetCacheRoutingService(), false, groupID, b)
	}
	bytestreamVerifyClosure := func(ctx context.Context, groupID string, b *batch_operator.DigestBatch) error {
		return migration_operators.ByteStreamReadAndVerify(ctx, env.GetCacheRoutingService(), true, groupID, b)
	}
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, status.FailedPreconditionError("An authenticator is required to route and migrate between caches.")
	}
	copyOp := batch_operator.NewImmediateDigestOperator(authenticator, "bytestream-copy", bytestreamCopyClosure, *bytestreamMigrationOpTimeout)
	readOp := batch_operator.NewImmediateDigestOperator(authenticator, "bytestream-read", bytestreamReadClosure, *bytestreamMigrationOpTimeout)
	readVerifyOp := batch_operator.NewImmediateDigestOperator(authenticator, "bytestream-read-verify", bytestreamVerifyClosure, *bytestreamMigrationOpTimeout)

	return &RoutingByteStreamClient{
		router:       routingService,
		copyOp:       copyOp,
		readOp:       readOp,
		readVerifyOp: readVerifyOp,
	}, nil
}

func (r *RoutingByteStreamClient) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest, opts ...grpc.CallOption) (*bspb.QueryWriteStatusResponse, error) {
	primaryClient, _, err := r.router.GetBSClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	return primaryClient.QueryWriteStatus(ctx, req, opts...)
}

func (r *RoutingByteStreamClient) Read(ctx context.Context, req *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	primaryClient, _, err := r.router.GetBSClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	rsp, err := primaryClient.Read(ctx, req, opts...)
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
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			r.readVerifyOp.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
		}
	} else if singleRandValue < c.GetBackgroundReadFraction() {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			r.readOp.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
		}
	} else if (singleRandValue) < c.GetBackgroundCopyFraction() {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			r.copyOp.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
		}
	}
	return rsp, nil
}

type wrappedWriteStream struct {
	ctx context.Context
	op  batch_operator.DigestOperator
	rn  *digest.CASResourceName
	bspb.ByteStream_WriteClient
	success bool
}

func (w *wrappedWriteStream) Send(req *bspb.WriteRequest) error {
	if req.GetResourceName() != "" {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			w.rn = rn
		}
	}

	err := w.ByteStream_WriteClient.Send(req)
	if err == nil && req.GetFinishWrite() && w.rn != nil {
		w.success = true
	}
	return err
}

func (w *wrappedWriteStream) CloseAndRecv() (*bspb.WriteResponse, error) {
	resp, err := w.ByteStream_WriteClient.CloseAndRecv()
	if w.success && err == nil && w.rn != nil {
		w.op.EnqueueByResourceName(w.ctx, w.rn)
	}
	return resp, err
}

func (r *RoutingByteStreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	primaryClient, _, err := r.router.GetBSClients(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	rsp, err := primaryClient.Write(ctx, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}
	singleRandValue := rand.Float64()
	if (singleRandValue) < c.GetBackgroundCopyFraction() {
		return &wrappedWriteStream{
			ctx:                    ctx,
			op:                     r.copyOp,
			ByteStream_WriteClient: rsp,
		}, nil
	}
	return rsp, nil
}
