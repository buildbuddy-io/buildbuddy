package config

import "flag"

var (
	zstdTranscodingEnabled           = flag.Bool("cache.zstd_transcoding_enabled", true, "Whether to accept requests to read/write zstd-compressed blobs, compressing/decompressing outgoing/incoming blobs on the fly.")
	unconditionallyCompressWrites    = flag.Bool("cache.unconditionally_compress_writes", false, "EXPERIMENTAL: Whether to compress uncompressed write requests before forwarding to remote cache.")
	minSizeToUnconditionallyCompress = flag.Int64("cache.min_size_to_unconditionally_compress", 100, "Minimum size in bytes for blobs to be compressed when cache.unconditionally_compress_writes is enabled.")
)

func ZstdTranscodingEnabled() bool {
	return *zstdTranscodingEnabled
}

func UnconditionallyCompressWrites() bool {
	return *unconditionallyCompressWrites
}

func MinSizeToUnconditionallyCompress() int64 {
	return *minSizeToUnconditionallyCompress
}
