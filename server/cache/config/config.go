package config

import "flag"

var cacheMaxSizeBytes = flag.Int64("cache.max_size_bytes", 10_000_000_000 /* 10 GB */, "How big to allow the cache to be (in bytes).")

func MaxSizeBytes() int64 {
	return *cacheMaxSizeBytes
}
