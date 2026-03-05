package routing_capabilities_client

import (
	"context"

	"google.golang.org/grpc"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type RoutingCapabilitiesClient struct {
	acClient map[string]repb.CapabilitiesClient
	router   interfaces.CacheRoutingService
}

func New(env environment.Env) (repb.CapabilitiesClient, error) {
	routingService := env.GetCacheRoutingService()
	if routingService == nil {
		return nil, status.FailedPreconditionError("No routing service configured.")
	}

	return &RoutingCapabilitiesClient{
		router: routingService,
	}, nil
}

// GetCapabilities implements remote_execution.CapabilitiesClient.
func (r *RoutingCapabilitiesClient) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest, opts ...grpc.CallOption) (*repb.ServerCapabilities, error) {
	primaryClient, err := r.router.GetPrimaryCapabilitiesClient(ctx)
	if err != nil {
		return nil, status.InternalErrorf("Failed to get primary AC client: %s", err)
	}
	return primaryClient.GetCapabilities(ctx, req, opts...)
}
