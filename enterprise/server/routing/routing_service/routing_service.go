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
	"google.golang.org/grpc"
)

const (
	routingServiceConfigFlag                = "routing-service-config"
	primaryCacheConfigField                 = "primary-cache"
	secondaryCacheConfigField               = "secondary-cache"
	backgroundCopyFractionConfigField       = "background-copy-fraction"
	backgroundReadFractionConfigField       = "background-read-fraction"
	backgroundReadVerifyFractionConfigField = "background-read-verify-fraction"
	dualWriteFractionConfigField            = "dual-write-fraction"
)

var (
	availableRoutes = flag.Slice("remote_cache.routing.available_routes", []string{}, "The set of GRPCS cache endpoints to use for routing.  If any endpoints are defined, routing is enabled.")
	emptyClientSet  = &clientSet{}
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
		return status.FailedPreconditionErrorf("Attempted to register routing service when no routes are available.")
	}
	efp := env.GetExperimentFlagProvider()
	if efp == nil {
		return status.FailedPreconditionErrorf("Routing requires experiments")
	}

	// TODO(jdhollen): validate that a reasonable default is set and that we can
	// connect to it.

	clientSets := map[string]*clientSet{}
	for _, r := range *availableRoutes {
		if _, ok := clientSets[r]; ok {
			continue
		}
		conn, err := grpc_client.DialInternal(env, r, grpc.WithInsecure())
		if err != nil {
			// The default Dial() behavior doesn't wait for the connection, so
			// this indicates some issue other than the server being down.
			return status.FailedPreconditionErrorf("Error dialing remote cache: %s", err.Error())
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
	c := r.efp.Object(ctx, routingServiceConfigFlag, nil)
	if c == nil {
		return nil, status.NotFoundError("Cache config not found (no default?)")
	}
	out := &ropb.CacheRoutingConfig{}
	primary, ok := c[primaryCacheConfigField]
	if !ok || primary == "" {
		alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "Missing PrimaryCache in routing config.")
		return nil, status.InternalErrorf("Cache config didn't have a primary cache defined.")
	}
	if primaryCache, ok := primary.(string); ok {
		if _, ok := r.clientSets[primaryCache]; ok {
			out.PrimaryCache = primaryCache
		} else {
			alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "primary-cache is not a known cache endpoint: %s", primaryCache)
			return nil, status.InternalErrorf("Invalid primary cache: %s", primaryCache)
		}
		out.PrimaryCache = primaryCache
	} else {
		alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "primary-cache is not a string: %T(%v)", primary, primary)
		return nil, status.InternalErrorf("Cache config didn't have a primary cache defined.")
	}
	if v, ok := c[secondaryCacheConfigField]; ok {
		if secondaryCache, ok := v.(string); ok {
			if secondaryCache == "" {
				return out, nil
			} else if _, ok := r.clientSets[secondaryCache]; ok {
				out.SecondaryCache = secondaryCache
			} else {
				alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "secondary-cache is not a known cache endpoint: %s", secondaryCache)
				return out, nil
			}
		} else {
			alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "secondary-cache is not a string: %T(%v)", v, v)
			return out, nil
		}
	}
	if v, ok := c[backgroundCopyFractionConfigField]; ok {
		if bgCopyFrac, ok := v.(float64); ok {
			out.BackgroundCopyFraction = bgCopyFrac
		} else {
			alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "background-copy-fraction is not a float64: %T(%v)", v, v)
			return out, nil
		}
	}
	if v, ok := c[backgroundReadFractionConfigField]; ok {
		if bgReadFrac, ok := v.(float64); ok {
			out.BackgroundReadFraction = bgReadFrac
		} else {
			alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "background-read-fraction is not a float64: %T(%v)", v, v)
			return out, nil
		}
	}
	if v, ok := c[backgroundReadVerifyFractionConfigField]; ok {
		if bgRVFrac, ok := v.(float64); ok {
			out.BackgroundReadVerifyFraction = bgRVFrac
		} else {
			alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "background-read-verify-fraction is not a float64: %T(%v)", v, v)
			return out, nil
		}
	}
	if v, ok := c[dualWriteFractionConfigField]; ok {
		if dualWriteFrac, ok := v.(float64); ok {
			out.DualWriteFraction = dualWriteFrac
		} else {
			alert.CtxUnexpectedEvent(ctx, "routing-service-invalid-config", "dual-write-fraction is not a float64: %T(%v)", v, v)
			return out, nil
		}
	}

	log.Printf("primary: %s", out.PrimaryCache)
	return out, nil
}

func (r *routingService) getClientSets(ctx context.Context) (*clientSet, *clientSet, error) {
	conf, err := r.GetCacheRoutingConfig(ctx)
	if err != nil {
		alert.CtxUnexpectedEvent(ctx, "routing-failure", "A routingService request failed to fetch config: %s", err)
		return nil, nil, status.InternalErrorf("Invalid or missing cache specification.")
	}
	primaryRoute := conf.GetPrimaryCache()
	primaryClientSet, ok := r.clientSets[primaryRoute]
	if !ok || primaryClientSet == nil {
		alert.CtxUnexpectedEvent(ctx, "routing-unknown-route", "routingService is not configured to support route '%s'", primaryRoute)
		return nil, nil, status.InternalErrorf("Invalid or missing cache specification.")
	}
	secondaryRoute := conf.GetSecondaryCache()
	if secondaryRoute == "" {
		return primaryClientSet, emptyClientSet, nil
	}
	secondaryClientSet, ok := r.clientSets[secondaryRoute]
	if !ok || secondaryClientSet == nil {
		alert.CtxUnexpectedEvent(ctx, "routing-unknown-route", "routingService is not configured to support route '%s'", secondaryRoute)
		return nil, nil, status.InternalErrorf("Invalid or missing cache specification.")
	}

	return primaryClientSet, secondaryClientSet, nil
}

func (r *routingService) GetCASClients(ctx context.Context) (repb.ContentAddressableStorageClient, repb.ContentAddressableStorageClient, error) {
	primary, secondary, err := r.getClientSets(ctx)
	if err != nil {
		return nil, nil, err
	}
	return primary.cas, secondary.cas, nil
}

func (r *routingService) GetACClients(ctx context.Context) (repb.ActionCacheClient, repb.ActionCacheClient, error) {
	primary, secondary, err := r.getClientSets(ctx)
	if err != nil {
		return nil, nil, err
	}
	return primary.ac, secondary.ac, nil
}

func (r *routingService) GetBSClients(ctx context.Context) (bspb.ByteStreamClient, bspb.ByteStreamClient, error) {
	primary, secondary, err := r.getClientSets(ctx)
	if err != nil {
		return nil, nil, err
	}
	return primary.bs, secondary.bs, nil
}

func (r *routingService) GetPrimaryCapabilitiesClient(ctx context.Context) (repb.CapabilitiesClient, error) {
	primary, _, err := r.getClientSets(ctx)
	if err != nil {
		return nil, err
	}
	return primary.cap, nil
}
