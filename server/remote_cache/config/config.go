package config

import "flag"

var zstdTranscodingEnabled = flag.Bool("cache.zstd_transcoding_enabled", true, "Whether to accept requests to read/write zstd-compressed blobs, compressing/decompressing outgoing/incoming blobs on the fly.")

func ZstdTranscodingEnabled() bool {
	return *zstdTranscodingEnabled
}
