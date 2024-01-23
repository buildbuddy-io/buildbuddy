package featureflag

import (
	"context"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	enableFunTabFlag = "enable-fun-tab"
)

func (ffs *FeatureFlagService) IsEnabled(ctx context.Context, flagName string) (bool, error) {
	if ffs.env.GetDBHandle() == nil {
		return false, status.UnimplementedError("feature flag service not supported")
	}

	flagEntry, cached := ffs.featureFlagCache.Get(flagName)
	if !cached {
		flag, err := ffs.FetchFlag(ctx, flagName)
		if err != nil {
			return false, status.WrapError(err, "fetch flag")
		}
		configuredGroupMap := make(map[string]struct{})
		for _, gid := range flag.ExperimentGroupIds {
			configuredGroupMap[gid] = struct{}{}
		}
		flagEntry = &flagCacheEntry{
			enabled:            flag.Enabled,
			configuredGroupIds: configuredGroupMap,
		}
		ffs.featureFlagCache.Add(flagName, flagEntry)
	}

	if !flagEntry.enabled {
		return false, nil
	}
	// If no groups are assigned to the experiment, it's default on for all groups
	if len(flagEntry.configuredGroupIds) == 0 {
		return true, nil
	}

	gid, err := groupID(ctx, ffs.env)
	if err != nil {
		return false, status.WrapError(err, "get group id")
	}

	_, inExperiment := flagEntry.configuredGroupIds[gid]
	return inExperiment, nil
}

func groupID(ctx context.Context, env environment.Env) (string, error) {
	var gid string
	u, err := perms.AuthenticatedUser(ctx, env)
	if err == nil {
		gid = u.GetGroupID()
	} else if err != nil && !authutil.IsAnonymousUserError(err) {
		return "", err
	}
	return gid, nil
}
