package routing_action_cache_client

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

type RoutingACClient struct {
	router   interfaces.CacheRoutingService
	copyOp   batch_operator.BatchDigestOperator
	readOp   batch_operator.BatchDigestOperator
	verifyOp batch_operator.BatchDigestOperator
}

func New(env environment.Env) (repb.ActionCacheClient, error) {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	return &RoutingACClient{
		router: routingService,
	}, nil
}

func (r *RoutingACClient) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	primaryClient, err := r.router.GetPrimaryACClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	rsp, err := primaryClient.GetActionResult(ctx, req, opts...)
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
		r.verifyOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	} else if singleRandValue < c.GetBackgroundReadFraction() {
		r.readOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	} else if (singleRandValue) < c.GetBackgroundCopyFraction() {
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	}
	return rsp, nil
}

func (r *RoutingACClient) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	primaryClient, err := r.router.GetPrimaryACClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	rsp, err := primaryClient.UpdateActionResult(ctx, req, opts...)
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
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	}
	return rsp, nil
}
