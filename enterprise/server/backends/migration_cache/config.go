package migration_cache

import (
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

type MigrationConfig struct {
	Src                  *CacheConfig `yaml:"src"`
	Dest                 *CacheConfig `yaml:"dest"`
	DoubleReadPercentage float64      `yaml:"double_read_percentage"`
	// LogNotFoundErrors controls whether to log not found errors in the dest cache when double reading
	// At the beginning of the migration, we may want this to be false, or it may clog the logs
	// if a lot of data has not been copied over yet
	LogNotFoundErrors  bool `yaml:"log_not_found_errors"`
	CopyChanBufferSize int  `yaml:"copy_chan_buffer_size"`
	// CopyChanFullWarningIntervalMin controls how often we should log when the copy chan is full
	CopyChanFullWarningIntervalMin int64 `yaml:"copy_chan_full_warning_interval_min"`
	MaxCopiesPerSec                int   `yaml:"max_copies_per_sec"`
}

type CacheConfig struct {
	DiskConfig   *DiskCacheConfig   `yaml:"disk"`
	PebbleConfig *PebbleCacheConfig `yaml:"pebble"`
}

type DiskCacheConfig struct {
	RootDirectory     string                  `yaml:"root_directory"`
	Partitions        []disk.Partition        `yaml:"partitions"`
	PartitionMappings []disk.PartitionMapping `yaml:"partition_mappings"`
	UseV2Layout       bool                    `yaml:"use_v2_layout"`
}

type PebbleCacheConfig struct {
	Name                   string                  `yaml:"name"`
	RootDirectory          string                  `yaml:"root_directory"`
	Partitions             []disk.Partition        `yaml:"partitions"`
	PartitionMappings      []disk.PartitionMapping `yaml:"partition_mappings"`
	MaxSizeBytes           int64                   `yaml:"max_size_bytes"`
	BlockCacheSizeBytes    int64                   `yaml:"block_cache_size_bytes"`
	MaxInlineFileSizeBytes int64                   `yaml:"max_inline_file_size_bytes"`
	AtimeUpdateThreshold   *time.Duration          `yaml:"atime_update_threshold"`
	AtimeWriteBatchSize    int                     `yaml:"atime_write_batch_size"`
	AtimeBufferSize        *int                    `yaml:"atime_buffer_size"`
	MinEvictionAge         *time.Duration          `yaml:"min_eviction_age"`
	IsolateByGroupIDs      bool                    `yaml:"isolate_by_group_ids"`

	EnableZstdCompression       bool  `yaml:"enable_zstd_compression"`
	MinBytesAutoZstdCompression int64 `yaml:"min_bytes_auto_zstd_compression"`
}

func (cfg *MigrationConfig) SetConfigDefaults() {
	if cfg.CopyChanBufferSize == 0 {
		cfg.CopyChanBufferSize = 50000
	}
	if cfg.MaxCopiesPerSec == 0 {
		cfg.MaxCopiesPerSec = 5000
	}
}
