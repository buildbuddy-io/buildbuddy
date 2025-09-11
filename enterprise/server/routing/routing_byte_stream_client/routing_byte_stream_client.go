package routing_byte_stream_client

import (
	"context"
	"math/rand/v2"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type RoutingByteStreamClient struct {
	acClient map[string]bspb.ByteStreamClient
	router   interfaces.CacheRoutingService
	copyOp   batch_operator.BatchDigestOperator
	readOp   batch_operator.BatchDigestOperator
	verifyOp batch_operator.BatchDigestOperator
}

func New(env environment.Env) (bspb.ByteStreamClient, error) {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	return &RoutingByteStreamClient{
		router: routingService,
	}, nil
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
	rsp, err := primaryClient.Read(ctx, req, opts...)
	if err != nil {
		return rsp, err
	}

	c, err := r.router.GetCacheRoutingConfig(ctx)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to fetch routing config: %s", err)
		return rsp, nil
	}
	singleRandValue := rand.Float32()
	if singleRandValue < c.GetBackgroundReadVerifyFraction() {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			r.verifyOp.Enqueue(ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
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
	ctx    context.Context
	copyOp batch_operator.BatchDigestOperator
	bspb.ByteStream_WriteClient
}

func (w *wrappedWriteStream) Send(req *bspb.WriteRequest) error {
	if req.GetResourceName() != "" {
		if rn, err := digest.ParseDownloadResourceName(req.GetResourceName()); err == nil {
			w.copyOp.Enqueue(w.ctx, rn.GetInstanceName(), []*repb.Digest{rn.GetDigest()}, rn.GetDigestFunction())
		}
	}
	return w.ByteStream_WriteClient.Send(req)
}

func (r *RoutingByteStreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	primaryClient, err := r.router.GetPrimaryBSClient(ctx)
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
	singleRandValue := rand.Float32()
	if (singleRandValue) < c.GetBackgroundCopyFraction() {
		return &wrappedWriteStream{
			ctx:                    ctx,
			copyOp:                 r.copyOp,
			ByteStream_WriteClient: rsp,
		}, nil
	}
	return rsp, nil
}
