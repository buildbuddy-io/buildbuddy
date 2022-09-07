package migration_cache

import (
	"context"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

type MigrationConfig struct {
	Src                  *CacheConfig `yaml:"src"`
	Dest                 *CacheConfig `yaml:"dest"`
	DoubleReadPercentage float64      `yaml:"double_read_percentage"`
	CopyChanBufferSize   int          `yaml:"copy_chan_buffer_size"`
}

type CacheConfig interface {
	CacheFromConfig(env environment.Env) (interfaces.Cache, error)
}

type DiskWrapper struct {
	Config DiskCacheConfig `yaml:"disk"`
}

type DiskCacheConfig struct {
	RootDirectory     string                  `yaml:"root_directory"`
	Partitions        []disk.Partition        `yaml:"partitions"`
	PartitionMappings []disk.PartitionMapping `yaml:"partition_mappings"`
	UseV2Layout       bool                    `yaml:"use_v2_layout"`
}

type PebbleWrapper struct {
	Config PebbleCacheConfig `yaml:"pebble"`
}

type PebbleCacheConfig struct {
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
}

func (cfg *MigrationConfig) SetConfigDefaults() {
	if cfg.CopyChanBufferSize == 0 {
		cfg.CopyChanBufferSize = 50000
	}
}

func (dc *DiskWrapper) CacheFromConfig(env environment.Env) (interfaces.Cache, error) {
	opts := &disk_cache.Options{
		RootDirectory:     dc.Config.RootDirectory,
		Partitions:        dc.Config.Partitions,
		PartitionMappings: dc.Config.PartitionMappings,
		UseV2Layout:       dc.Config.UseV2Layout,
	}
	return disk_cache.NewDiskCache(env, opts, cache_config.MaxSizeBytes())
}
func (pc *PebbleWrapper) CacheFromConfig(env environment.Env) (interfaces.Cache, error) {
	opts := &pebble_cache.Options{
		RootDirectory:          pc.Config.RootDirectory,
		Partitions:             pc.Config.Partitions,
		PartitionMappings:      pc.Config.PartitionMappings,
		MaxSizeBytes:           pc.Config.MaxSizeBytes,
		BlockCacheSizeBytes:    pc.Config.BlockCacheSizeBytes,
		MaxInlineFileSizeBytes: pc.Config.MaxInlineFileSizeBytes,
		AtimeUpdateThreshold:   pc.Config.AtimeUpdateThreshold,
		AtimeWriteBatchSize:    pc.Config.AtimeWriteBatchSize,
		AtimeBufferSize:        pc.Config.AtimeBufferSize,
		MinEvictionAge:         pc.Config.MinEvictionAge,
	}
	c, err := pebble_cache.NewPebbleCache(env, opts)
	if err != nil {
		return nil, err
	}

	c.Start()
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		return c.Stop()
	})
	return c, nil
}
