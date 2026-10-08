package config

import "flag"

var remoteCache = flag.String("cache_proxy.remote_cache", "grpcs://remote.buildbuddy.dev", "The gRPC target of the backing remote cache. Cache hits are also reported to the hit-tracking service at this target.")

// RemoteCacheTarget returns the gRPC target of the backing remote cache.
func RemoteCacheTarget() string {
	return *remoteCache
}
