package config

import "flag"

var cacheMaxSizeBytes = flag.Int64("cache.max_size_bytes", 0, "How big to allow the cache to be (in bytes).")

func MaxSizeBytes() int64 {
	return *cacheMaxSizeBytes
}
