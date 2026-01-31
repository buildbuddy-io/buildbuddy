package config

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
)

var zstdTranscodingEnabled = flag.Bool("cache.zstd_transcoding_enabled", true, "Whether to accept requests to read/write zstd-compressed blobs, compressing/decompressing outgoing/incoming blobs on the fly.")

// Benchmarks show 128KB, 256KB, and 512KB all perform about the same. This
// should be slightly smaller than 2^N, to allow for proto and gRPC
// overhead.
var ReadBufSizeBytes = flag.Int("cache.read_buf_size_bytes", 256*1000, "The buffer size used for reading from the cache")

var maxChunkSizeBytes = flag.Int64("cache.max_chunk_size_bytes", 2<<20, "Only blobs larger (non-inclusive) than this threshold will be chunked (default 2MB). This is also the maximum size of a chunk. The average chunk size will be 1/4 of this value, and the minimum will be 1/16 of this value.")

func ZstdTranscodingEnabled() bool {
	return *zstdTranscodingEnabled
}

func MaxChunkSizeBytes() int64 {
	return *maxChunkSizeBytes
}

func ChunkingEnabled(ctx context.Context, efp interfaces.ExperimentFlagProvider) bool {
	return efp != nil &&
		efp.Boolean(ctx, "cache.chunking_enabled", false) &&
		efp.Boolean(ctx, "cache.split_splice_enabled", false)
}
