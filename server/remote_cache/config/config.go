package config

import "github.com/buildbuddy-io/buildbuddy/server/util/flag"

var zstdTranscodingEnabled = flag.Bool("cache.zstd_transcoding_enabled", true, "Whether to accept requests to read/write zstd-compressed blobs, compressing/decompressing outgoing/incoming blobs on the fly.")

// Benchmarks show 128KB, 256KB, and 512KB all perform about the same. This
// should be slightly smaller than 2^N, to allow for proto and gRPC
// overhead.
var ReadBufSizeBytes = flag.Int("cache.read_buf_size_bytes", 256*1000, "The buffer size used for reading from the cache")

func ZstdTranscodingEnabled() bool {
	return *zstdTranscodingEnabled
}
