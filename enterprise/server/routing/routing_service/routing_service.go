package routing_service

import (
	"context"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ropb "github.com/buildbuddy-io/buildbuddy/proto/routing"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	availableRoutes = flag.Slice("remote_cache.routing.available_routes", []string{}, "The set of GRPCS cache endpoints to use for routing.  If any endpoints are defined, routing is enabled.")
)

type clientSet struct {
	cas repb.ContentAddressableStorageClient
	ac  repb.ActionCacheClient
	bs  bspb.ByteStreamClient
	cap repb.CapabilitiesClient
}

type routingService struct {
	efp        interfaces.ExperimentFlagProvider
	clientSets map[string]*clientSet
}

func RegisterRoutingService(env *real_environment.RealEnv) error {
	if len(*availableRoutes) == 0 {
		panic("Attempted to register routing service when no routes are available.")
	}
	efp := env.GetExperimentFlagProvider()
	if efp == nil {
		panic("Routing requires experiments")
	}

	clientSets := map[string]*clientSet{}
	for _, r := range *availableRoutes {
		if _, ok := clientSets[r]; ok {
			continue
		}
		conn, err := grpc_client.DialInternal(env, r)
		if err != nil {
			// The default Dial() behavior doesn't wait for the connection, so
			// this indicates some issue other than the server being down.
			log.Fatalf("Error dialing remote cache: %s", err.Error())
		}

		clientSets[r] = &clientSet{
			cas: repb.NewContentAddressableStorageClient(conn),
			ac:  repb.NewActionCacheClient(conn),
			bs:  bspb.NewByteStreamClient(conn),
			cap: repb.NewCapabilitiesClient(conn),
		}
	}

	// Now that all clients are set up, we can register.
	env.SetCacheRoutingService(&routingService{
		efp:        env.GetExperimentFlagProvider(),
		clientSets: clientSets,
	})
	return nil
}

func IsCacheRoutingEnabled() bool {
	return len(*availableRoutes) > 0
}

func (r *routingService) GetCacheRoutingConfig(ctx context.Context) (*ropb.CacheRoutingConfig, error) {
	return &ropb.CacheRoutingConfig{}, nil
}

func (r *routingService) getPrimaryClientSet(ctx context.Context) (*clientSet, error) {
	conf, err := r.GetCacheRoutingConfig(ctx)
	if err != nil {
		alert.CtxUnexpectedEvent(ctx, "routing-failure", "A routingService request failed: %s", err)
		return nil, err
	}
	route := conf.GetPrimaryCache()
	if route == "" {
		alert.CtxUnexpectedEvent(ctx, "routing-empty-route", "A routingService route had no primary cache defined.")
		return nil, status.InternalError("Missing primary cache config")
	}

	clientSet, ok := r.clientSets[route]
	if !ok || clientSet == nil {
		alert.CtxUnexpectedEvent(ctx, "routing-unknown-route", "routingService is not configured to support route %s", route)
		return nil, status.InternalErrorf("Unsupported route %s", route)
	}
	return clientSet, nil
}

func (r *routingService) GetPrimaryCASClient(ctx context.Context) (repb.ContentAddressableStorageClient, error) {
	clientSet, err := r.getPrimaryClientSet(ctx)
	if err != nil {
		return nil, err
	}
	return clientSet.cas, nil
}

func (r *routingService) GetPrimaryACClient(ctx context.Context) (repb.ActionCacheClient, error) {
	clientSet, err := r.getPrimaryClientSet(ctx)
	if err != nil {
		return nil, err
	}
	return clientSet.ac, nil
}

func (r *routingService) GetPrimaryBSClient(ctx context.Context) (bspb.ByteStreamClient, error) {
	clientSet, err := r.getPrimaryClientSet(ctx)
	if err != nil {
		return nil, err
	}
	return clientSet.bs, nil
}

func (r *routingService) GetPrimaryCapabilitiesClient(ctx context.Context) (repb.CapabilitiesClient, error) {
	clientSet, err := r.getPrimaryClientSet(ctx)
	if err != nil {
		return nil, err
	}
	return clientSet.cap, nil
}
