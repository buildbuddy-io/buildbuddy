package namespace

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

func CASCache(ctx context.Context, cache interfaces.Cache, instanceName string) (interfaces.Cache, error) {
	return cache.WithIsolation(ctx, resource.CacheType_CAS, instanceName)
}

func ActionCache(ctx context.Context, cache interfaces.Cache, instanceName string) (interfaces.Cache, error) {
	return cache.WithIsolation(ctx, resource.CacheType_AC, instanceName)
}
