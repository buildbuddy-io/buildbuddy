package featureflag_client

import (
	"context"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	ffpb "github.com/buildbuddy-io/buildbuddy/proto/featureflag"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/featureflag/featureflag_cache"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type FeatureFlagClient struct {
	env              environment.Env
	featureFlagCache *featureflag_cache.FeatureFlagCache
	grpcClient       ffpb.FeatureFlagServiceClient
}

func NewFeatureFlagClient(env environment.Env, grpcClient ffpb.FeatureFlagServiceClient) (*FeatureFlagClient, error) {
	cache, err := featureflag_cache.NewCache(*featureflag_cache.CacheTTL)
	if err != nil {
		return nil, err
	}
	return &FeatureFlagClient{
		env:              env,
		featureFlagCache: cache,
		grpcClient:       grpcClient,
	}, nil
}

func (ffs *FeatureFlagClient) IsEnabled(ctx context.Context, flagName string) (bool, error) {
	flagEntry, cached := ffs.featureFlagCache.Get(flagName)
	if !cached {
		req := &ffpb.GetFeatureFlagRequest{Name: flagName}
		rsp, err := ffs.grpcClient.GetFeatureFlag(ctx, req)
		if err != nil {
			return false, err
		}

		configuredGroupMap := make(map[string]struct{})
		for _, gid := range rsp.ConfiguredGroupIds {
			configuredGroupMap[gid] = struct{}{}
		}
		flagEntry = &featureflag_cache.FlagCacheEntry{
			Enabled:            rsp.Enabled,
			ConfiguredGroupIds: configuredGroupMap,
		}
		ffs.featureFlagCache.Add(flagName, flagEntry)
	}

	if !flagEntry.Enabled {
		return false, nil
	}
	// If no groups are assigned to the experiment, it's default on for all groups
	if len(flagEntry.ConfiguredGroupIds) == 0 {
		return true, nil
	}

	gid, err := groupID(ctx)
	if err != nil {
		return false, status.WrapError(err, "get group id")
	}

	_, inExperiment := flagEntry.ConfiguredGroupIds[gid]
	return inExperiment, nil
}

func groupID(ctx context.Context) (string, error) {
	user, err := auth.UserFromTrustedJWT(ctx)
	if err != nil && !authutil.IsAnonymousUserError(err) {
		return "", err
	}
	gid := ""
	if user != nil {
		gid = user.GetGroupID()
	}
	return gid, nil
}
