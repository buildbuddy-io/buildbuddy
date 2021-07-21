package namespace

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

func CASCache(ctx context.Context, cache interfaces.Cache, instanceName string) (interfaces.Cache, error) {
	return cache.WithIsolation(ctx, interfaces.CASCacheType, instanceName)
}

func ActionCache(ctx context.Context, cache interfaces.Cache, instanceName string) (interfaces.Cache, error) {
	return cache.WithIsolation(ctx, interfaces.ActionCacheType, instanceName)
}
