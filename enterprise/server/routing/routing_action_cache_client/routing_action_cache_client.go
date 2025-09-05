package routing_action_cache_client

import (
	"context"

	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type RoutingACClient struct {
	router interfaces.CacheRoutingService
}

func NewClient(env environment.Env) repb.ActionCacheClient {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		panic("No routing service configured.")
	}

	return &RoutingACClient{
		router: routingService,
	}
}

func (r *RoutingACClient) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	primaryClient, err := r.router.GetPrimaryACClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	return primaryClient.GetActionResult(ctx, req, opts...)
}

func (r *RoutingACClient) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	primaryClient, err := r.router.GetPrimaryACClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	return primaryClient.UpdateActionResult(ctx, req, opts...)
}
