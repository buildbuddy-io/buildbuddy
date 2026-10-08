// Package cache_proxy_config holds cache proxy flags that are read by more
// than one package, so that each is defined exactly once.
package cache_proxy_config

import "flag"

var remoteCache = flag.String("cache_proxy.remote_cache", "", "The gRPC target of the backing remote cache. Cache hits are also reported to the hit-tracking service at this target. Required by the cache proxy; if unset, hit tracking is disabled.")

// RemoteCacheTarget returns the gRPC target of the backing remote cache.
func RemoteCacheTarget() string {
	return *remoteCache
}
