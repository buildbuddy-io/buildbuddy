package featureflag

import (
	"context"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	enableFunTabFlag = "enable-fun-tab"
)

func (ffs *FeatureFlagService) IsEnabled(ctx context.Context, flagName string) (bool, error) {
	flag, err := ffs.GetFlag(ctx, flagName)
	if err != nil {
		return false, status.WrapError(err, "read flag")
	}
	if !flag.Enabled {
		return false, nil
	}

	// If no groups are assigned to the experiment, it's default on for all groups
	if len(flag.ExperimentGroupIds) == 0 {
		return true, nil
	}

	gid, err := groupID(ctx, ffs.env)
	if err != nil {
		return false, status.WrapError(err, "get group id")
	}

	return contains(flag.ExperimentGroupIds, gid), nil
}

func groupID(ctx context.Context, env environment.Env) (string, error) {
	var gid string
	u, err := perms.AuthenticatedUser(ctx, env)
	if err == nil {
		gid = u.GetGroupID()
	} else if err != nil && !authutil.IsAnonymousUserError(err) && !*container.DebugEnableAnonymousRecycling {
		return "", err
	}
	return gid, nil
}

func contains(s []string, target string) bool {
	for _, elem := range s {
		if elem == target {
			return true
		}
	}
	return false
}
