package cache_proxy_config

import "flag"

var remoteCache = flag.String("cache_proxy.remote_cache", "", "The gRPC target of the backing remote cache. Cache hits are also reported to the hit-tracking service at this target. Required by the cache proxy; if unset, hit tracking is disabled.")

func RemoteCacheTarget() string {
	return *remoteCache
}
