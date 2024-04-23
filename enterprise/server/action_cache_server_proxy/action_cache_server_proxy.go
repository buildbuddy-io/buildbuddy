package action_cache_server_proxy

import (
	"context"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type ActionCacheServerProxy struct {
	env          environment.Env
	local_cache  interfaces.Cache
	remote_cache repb.ActionCacheClient
}

func Register(env *real_environment.RealEnv) error {
	actionCacheServer, err := NewActionCacheServerProxy(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ActionCacheServerProxy: %s", err)
	}
	env.SetActionCacheServer(actionCacheServer)
	return nil
}

func NewActionCacheServerProxy(env environment.Env) (*ActionCacheServerProxy, error) {
	local_cache := env.GetCache()
	if local_cache == nil {
		return nil, fmt.Errorf("A cache is required to enable the ActionCacheServerProxy")
	}
	remote_cache := env.GetActionCacheClient()
	if remote_cache == nil {
		return nil, fmt.Errorf("An ActionCacheClient is required to enable the ActionCacheServerProxy")
	}
	return &ActionCacheServerProxy{
		env:          env,
		local_cache:  local_cache,
		remote_cache: remote_cache,
	}, nil
}

// TODO(iain): don't cache these locally
func (s *ActionCacheServerProxy) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	return s.remote_cache.GetActionResult(ctx, req)
}

// TODO(iain): don't cache these locally
func (s *ActionCacheServerProxy) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	return s.remote_cache.UpdateActionResult(ctx, req)
}
