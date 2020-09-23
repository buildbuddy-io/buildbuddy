package namespace

import (
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

const (
	acCachePrefix = "ac"
)

func CASCache(cache interfaces.Cache, instanceName string) interfaces.Cache {
	c := cache
	if instanceName != "" {
		c = c.WithPrefix(instanceName)
	}
	return c
}

func ActionCache(cache interfaces.Cache, instanceName string) interfaces.Cache {
	c := cache
	if instanceName != "" {
		c = c.WithPrefix(instanceName)
	}
	return c.WithPrefix(acCachePrefix)
}
