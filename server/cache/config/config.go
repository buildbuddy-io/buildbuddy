package config

import (
	"flag"
	"time"
)

var (
	cacheMaxSizeBytes = flag.Int64("cache.max_size_bytes", 10_000_000_000 /* 10 GB */, "How big to allow the cache to be (in bytes).")
	countTTL          = flag.Duration("cache.count_ttl", 24*time.Hour, "How long to go without receiving any cache requests for an invocation before deleting the invocation's counts from the metrics collector.")
)

func MaxSizeBytes() int64 {
	return *cacheMaxSizeBytes
}

func CountTTL() time.Duration {
	return *countTTL
}
