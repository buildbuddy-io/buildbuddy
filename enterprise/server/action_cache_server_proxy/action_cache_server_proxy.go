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
	env         environment.Env
	localCache  interfaces.Cache
	remoteCache repb.ActionCacheClient
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
	localCache := env.GetCache()
	if localCache == nil {
		return nil, fmt.Errorf("A cache is required to enable the ActionCacheServerProxy")
	}
	remoteCache := env.GetActionCacheClient()
	if remoteCache == nil {
		return nil, fmt.Errorf("An ActionCacheClient is required to enable the ActionCacheServerProxy")
	}
	return &ActionCacheServerProxy{
		env:         env,
		localCache:  localCache,
		remoteCache: remoteCache,
	}, nil
}

// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. Thus, always fetch them from
// the authoritative cache.
func (s *ActionCacheServerProxy) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	return s.remoteCache.GetActionResult(ctx, req)
}

// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. Thus, don't cache them
// locally when writing to the authoritative cache.
func (s *ActionCacheServerProxy) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	return s.remoteCache.UpdateActionResult(ctx, req)
}
