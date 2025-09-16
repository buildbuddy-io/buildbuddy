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
	router interfaces.CacheRoutingService
	copyOp batch_operator.BatchDigestOperator
	readOp batch_operator.BatchDigestOperator
}

func handleCopy(ctx context.Context, groupID string, b *batch_operator.DigestBatch, routingService interfaces.CacheRoutingService) error {
	primary, secondary, err := routingService.GetACClients(ctx)
	if err != nil {
		return err
	}
	for _, d := range b.Digests {
		r, err := primary.GetActionResult(ctx, &repb.GetActionResultRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			ActionDigest:   d,
		})
		if err != nil {
			// XXX: log error, drop rest.  this isn't a huge deal.
			return err
		}
		_, err = secondary.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			ActionDigest:   d,
			ActionResult:   r,
		})
		if err != nil {
			// XXX: log error, drop rest.  this isn't a huge deal.
			return err
		}
	}
	return nil
}

func handleRead(ctx context.Context, groupID string, b *batch_operator.DigestBatch, routingService interfaces.CacheRoutingService) error {
	// Performing this read implicitly validates the full contents of the ActionResult, assuming it exists.
	_, secondary, err := routingService.GetACClients(ctx)
	if err != nil {
		return err
	}
	for _, d := range b.Digests {
		_, err = secondary.GetActionResult(ctx, &repb.GetActionResultRequest{
			InstanceName:   b.InstanceName,
			DigestFunction: b.DigestFunction,
			ActionDigest:   d,
		})
		if err != nil {
			// XXX: log error, read rest: we're trying to validate.
			continue
		}
	}
	return nil
}

func New(env environment.Env) (repb.ActionCacheClient, error) {
	routingService := env.GetCacheRoutingService()

	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	copyOp, err := batch_operator.New(
		env,
		"action_cache_copier",
		func(ctx context.Context, groupID string, u *batch_operator.DigestBatch) error {
			return handleCopy(ctx, groupID, u, routingService)
		},
		batch_operator.BatchDigestOperatorConfig{
			// XXX
		})
	if err != nil {
		return nil, err
	}
	readOp, err := batch_operator.New(
		env,
		"action_cache_reader",
		func(ctx context.Context, groupID string, u *batch_operator.DigestBatch) error {
			return handleRead(ctx, groupID, u, routingService)
		},
		batch_operator.BatchDigestOperatorConfig{
			// XXX
		})
	if err != nil {
		return nil, err
	}

	return &RoutingACClient{
		router: routingService,
		copyOp: copyOp,
		readOp: readOp,
	}, nil
}

func (r *RoutingACClient) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	primaryClient, _, err := r.router.GetACClients(ctx)
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
	if singleRandValue < c.GetBackgroundReadFraction() {
		r.readOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	} else if (singleRandValue) < c.GetBackgroundCopyFraction() {
		r.copyOp.Enqueue(ctx, req.GetInstanceName(), []*repb.Digest{req.GetActionDigest()}, req.GetDigestFunction())
	}
	return rsp, nil
}

func (r *RoutingACClient) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	primaryClient, _, err := r.router.GetACClients(ctx)
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
