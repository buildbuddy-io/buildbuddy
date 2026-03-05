package cache_config

import (
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

type MigrationConfig struct {
	Src                  *CacheConfig `yaml:"src"`
	Dest                 *CacheConfig `yaml:"dest"`
	DoubleReadPercentage float64      `yaml:"double_read_percentage"`
	// The percentage of read results we will decompress and compare the number
	// of bytes when doubel read is turned on.
	DecompressPercentage float64 `yaml:"decompress_percentage"`
	// LogNotFoundErrors controls whether to log not found errors in the dest cache when double reading
	// At the beginning of the migration, we may want this to be false, or it may clog the logs
	// if a lot of data has not been copied over yet
	LogNotFoundErrors  bool `yaml:"log_not_found_errors"`
	CopyChanBufferSize int  `yaml:"copy_chan_buffer_size"`
	// CopyChanFullWarningIntervalMin controls how often we should log when the copy chan is full
	CopyChanFullWarningIntervalMin int64 `yaml:"copy_chan_full_warning_interval_min"`
	MaxCopiesPerSec                int   `yaml:"max_copies_per_sec"`
	NumCopyWorkers                 int   `yaml:"num_copy_workers"`
	// AsyncDestWrites controls whether we write to destination cache in the background
	AsyncDestWrites bool `yaml:"async_dest_writes"`
}

type CacheConfig struct {
	DiskConfig        *DiskCacheConfig        `yaml:"disk"`
	PebbleConfig      *PebbleCacheConfig      `yaml:"pebble"`
	DistributedConfig *DistributedCacheConfig `yaml:"distributed"`
	MetaConfig        *MetaCacheConfig        `yaml:"meta"`
}

type DiskCacheConfig struct {
	RootDirectory     string                  `yaml:"root_directory"`
	Partitions        []disk.Partition        `yaml:"partitions"`
	PartitionMappings []disk.PartitionMapping `yaml:"partition_mappings"`
	UseV2Layout       bool                    `yaml:"use_v2_layout"`
}

type GCSConfig struct {
	Bucket              string `yaml:"bucket" usage:"The name of the GCS bucket to store build artifact files in."`
	ProjectID           string `yaml:"project_id" usage:"The Google Cloud project ID of the project owning the above credentials and GCS bucket."`
	Credentials         string `yaml:"credentials" usage:"Credentials in JSON format that will be used to authenticate to GCS."`
	AppName             string `yaml:"app_name" usage:"The app name, under which blobstore data will be stored."`
	MinGCSFileSizeBytes *int64 `yaml:"min_gcs_file_size_bytes" usage:"Files larger than this may be stored in GCS (0 is disabled)."`
	TTLDays             *int64 `yaml:"ttl_days" usage:"An object TTL, specified in days, to apply to the GCS bucket (0 means disabled)."`
}

type PebbleCacheConfig struct {
	Name                        string                  `yaml:"name"`
	RootDirectory               string                  `yaml:"root_directory"`
	Partitions                  []disk.Partition        `yaml:"partitions"`
	PartitionMappings           []disk.PartitionMapping `yaml:"partition_mappings"`
	MaxSizeBytes                int64                   `yaml:"max_size_bytes"`
	BlockCacheSizeBytes         int64                   `yaml:"block_cache_size_bytes"`
	MaxInlineFileSizeBytes      int64                   `yaml:"max_inline_file_size_bytes"`
	AtimeUpdateThreshold        *time.Duration          `yaml:"atime_update_threshold"`
	AtimeBufferSize             *int                    `yaml:"atime_buffer_size"`
	MinEvictionAge              *time.Duration          `yaml:"min_eviction_age"`
	MinBytesAutoZstdCompression *int64                  `yaml:"min_bytes_auto_zstd_compression"`
	AverageChunkSizeBytes       int                     `yaml:"average_chunk_size_bytes"`
	ClearCacheOnStartup         bool                    `yaml:"clear_cache_on_startup"`
	ActiveKeyVersion            *int64                  `yaml:"active_key_version"`
	GCSConfig                   GCSConfig               `yaml:"gcs"`
	IncludeMetadataSize         bool                    `yaml:"include_metadata_size"`
	EnableAutoRatchet           bool                    `yaml:"enable_auto_ratchet"`
}

type MetaCacheConfig struct {
	Name                        string                  `yaml:"name" usage:"The name used in reporting cache metrics and status."`
	MetadataBackend             string                  `yaml:"metadata_backend" usage:"The metadata server to use (e.g., 'grpc://localhost:1991')."`
	PartitionMappings           []disk.PartitionMapping `yaml:"partition_mappings" usage:"Partition mappings for the cache."`
	MaxInlineFileSizeBytes      int64                   `yaml:"max_inline_file_size_bytes" usage:"Files smaller than this may be inlined directly into metadata storage."`
	MinBytesAutoZstdCompression int64                   `yaml:"min_bytes_auto_zstd_compression" usage:"Blobs larger than this will be zstd compressed before written to disk."`
	MaxWriteGoroutines          int                     `yaml:"max_write_goroutines" usage:"The maximum number of goroutines to write data in SetMulti."`
	GCSConfig                   GCSConfig               `yaml:"gcs" usage:"GCS configuration for storing large files."`
}

type DistributedCacheConfig struct {
	ListenAddr              string   `yaml:"listen_addr"`
	GroupName               string   `yaml:"group_name"`
	Nodes                   []string `yaml:"nodes"`
	NewNodes                []string `yaml:"new_nodes"`
	ReplicationFactor       int      `yaml:"replication_factor"`
	ClusterSize             int      `yaml:"cluster_size"`
	LookasideCacheSizeBytes int64    `yaml:"lookaside_cache_size_bytes"`
	EnableLocalWrites       bool     `yaml:"enable_local_writes"`
	ReadThroughLocalCache   bool     `yaml:"read_through_local_cache"`
}

func (cfg *MigrationConfig) SetConfigDefaults() {
	if cfg.CopyChanBufferSize == 0 {
		cfg.CopyChanBufferSize = 50000
	}
	if cfg.MaxCopiesPerSec == 0 {
		cfg.MaxCopiesPerSec = 5000
	}
	if cfg.NumCopyWorkers == 0 {
		cfg.NumCopyWorkers = 1
	}
}
