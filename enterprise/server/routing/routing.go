package routing

import (
	"context"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ropb "github.com/buildbuddy-io/buildbuddy/proto/routing"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	enableCacheRouting = flag.Bool("remote_cache.routing.enabled", false, "The size of the channel to use for buffering enqueued hits.")
)

type routingService struct {
	efp        interfaces.ExperimentFlagProvider
	casClients map[string]repb.ContentAddressableStorageClient
	acClients  map[string]repb.ActionCacheClient
	bsClients  map[string]bspb.ByteStreamClient
	capClients map[string]repb.CapabilitiesClient
}

func RegisterRoutingService(env *real_environment.RealEnv) error {
	if !*enableCacheRouting {
		panic("Attempted to register ")
	}
	efp := env.GetExperimentFlagProvider()
	if efp == nil {
		panic("Routing requires experiments")
	}
	// XXX: Register.
	env.SetCacheRoutingService(&routingService{
		efp: env.GetExperimentFlagProvider(),
	})

	return nil
}

func (r *routingService) GetCacheRoutingConfig(ctx context.Context) (*ropb.CacheRoutingConfig, error) {
	return &ropb.CacheRoutingConfig{}, nil
}

func (r *routingService) getPrimaryRoute(ctx context.Context) (string, error) {
	conf, err := r.GetCacheRoutingConfig(ctx)
	if err != nil {
		return "", err
	}
	if conf.GetPrimaryCache() == "" {
		// XXX: improve error (include group ID? Debounce?)
		return "", status.InternalError("Missing primary cache config")
	}
	return conf.GetPrimaryCache(), nil
}

func (r *routingService) getSecondaryRoute(ctx context.Context) (string, error) {
	conf, err := r.GetCacheRoutingConfig(ctx)
	if err != nil {
		return "", err
	}
	return conf.GetSecondaryCache(), nil
}

func (r *routingService) GetPrimaryCASClient(ctx context.Context) (repb.ContentAddressableStorageClient, error) {
	route, err := r.getPrimaryRoute(ctx)
	if err != nil {
		return nil, err
	}
	client, ok := r.casClients[route]
	if !ok || client == nil {
		return nil, status.InternalErrorf("No CAS client configured for route %s", route)
	}
	return client, nil
}

func (r *routingService) GetSecondaryCASClient(ctx context.Context) (repb.ContentAddressableStorageClient, error) {
	route, err := r.getSecondaryRoute(ctx)
	if err != nil || route == "" {
		return nil, err
	}
	client, ok := r.casClients[route]
	if !ok || client == nil {
		return nil, status.InternalErrorf("No CAS client configured for route %s", route)
	}
	return client, nil
}
