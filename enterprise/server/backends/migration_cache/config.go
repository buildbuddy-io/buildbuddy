package migration_cache

import (
	"context"
	"errors"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	cache_config "github.com/buildbuddy-io/buildbuddy/server/cache/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"reflect"
)

type MigrationConfig struct {
	Src  CacheConfig `yaml:"src"`
	Dest CacheConfig `yaml:"dest"`

	ClearDestCacheBeforeMigration bool `yaml:"clear_dest_cache_before_migration"`
}

type CacheConfig struct {
	DiskConfig   *DiskCacheConfig   `yaml:"disk"`
	PebbleConfig *PebbleCacheConfig `yaml:"pebble"`
}

type DiskCacheConfig struct {
	RootDirectory     string                  `yaml:"root_directory"`
	Partitions        []disk.Partition        `yaml:"partitions"`
	PartitionMappings []disk.PartitionMapping `yaml:"partition_mappings"`
}

type PebbleCacheConfig struct {
	RootDirectory       string `yaml:"root_directory"`
	BlockCacheSizeBytes int64  `yaml:"block_cache_size_bytes"`
	// TODO - Pass this as option in pebble_cache, instead of read from flag
	MaxInlineFileSizeBytes int64 `yaml:"max_inline_file_size_bytes"`
	// ...
}

func ParseConfig(env environment.Env) *MigrationCache {
	f, err := os.Open("enterprise/config/migration.yaml")
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("File does not exist")
			return nil
		}
		fmt.Printf("Can not open file %s", err)
		return nil
	}

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Printf("Can not read bytes %s", err)
		return nil
	}

	cfg := &MigrationConfig{}
	if err = yaml.Unmarshal(bytes, cfg); err != nil {
		fmt.Printf("Can not unmarshall config: %s", err)
		return nil
	}

	err = validateCacheConfig(cfg.Src)
	if err != nil {
		fmt.Printf("Error validating src config: %s", err)
	}
	err = validateCacheConfig(cfg.Dest)
	if err != nil {
		fmt.Printf("Error validating dest config: %s", err)
	}

	mc := &MigrationCache{}
	mc.setSrcCache(env, cfg.Src)
	mc.setDestCache(env, cfg.Dest)
	if cfg.ClearDestCacheBeforeMigration {
		os.RemoveAll(mc.Dest.RootDirectory)
	}

	return mc
}

func validateCacheConfig(config CacheConfig) error {
	numConfigs := 0
	v := reflect.ValueOf(config)
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).IsNil() {
			numConfigs++
		}
	}

	if numConfigs != 1 {
		return errors.New("exactly one config must be set")
	}

	return nil
}

func (mc *MigrationCache) setSrcCache(env environment.Env, config CacheConfig) error {
	if config.DiskConfig != nil {
		c, err := diskCacheFromConfig(env, config.DiskConfig)
		if err != nil {
			return errors.New("Could not initialize disk cache " + err.Error())
		}

		mc.Src = c
	} else if config.PebbleConfig != nil {
		c, err := pebbleCacheFromConfig(env, config.PebbleConfig)
		if err != nil {
			return errors.New("Could not initialize pebble cache " + err.Error())
		}

		mc.Src = c
	}
	return nil
}

func (mc *MigrationCache) setDestCache(env environment.Env, config CacheConfig) error {
	if config.DiskConfig != nil {
		c, err := diskCacheFromConfig(env, config.DiskConfig)
		if err != nil {
			return errors.New("Could not initialize disk cache " + err.Error())
		}

		mc.Dest = c
	} else if config.PebbleConfig != nil {
		c, err := pebbleCacheFromConfig(env, config.PebbleConfig)
		if err != nil {
			return errors.New("Could not initialize pebble cache " + err.Error())
		}

		mc.Dest = c
	}
	return nil
}

func diskCacheFromConfig(env environment.Env, cfg *DiskCacheConfig) (*disk_cache.DiskCache, error) {
	opts := &disk_cache.Options{
		RootDirectory:     cfg.RootDirectory,
		Partitions:        cfg.Partitions,
		PartitionMappings: cfg.PartitionMappings,
		UseV2Layout:       false,
	}
	return disk_cache.NewDiskCache(env, opts, cache_config.MaxSizeBytes())
}

func pebbleCacheFromConfig(env environment.Env, cfg *PebbleCacheConfig) (*pebble_cache.PebbleCache, error) {
	opts := &pebble_cache.Options{
		RootDirectory:       cfg.RootDirectory,
		Partitions:          nil,
		PartitionMappings:   nil,
		BlockCacheSizeBytes: cfg.BlockCacheSizeBytes,
		MaxSizeBytes:        cache_config.MaxSizeBytes(),
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
