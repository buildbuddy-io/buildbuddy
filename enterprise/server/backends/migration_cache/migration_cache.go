package migration_cache

import (
	"context"
	"flag"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
)

var (
	doubleReadPercentage      = flag.Float64("cache.migration_cache.double_read_percentage", 0, "The percentage of reads we should double read, to ensure the two caches have the same data. Float [0, 1]. 0 for no double reads, 1 for double reads on every read")
	migrateAndExit            = flag.Bool("cache.migration_cache.migrate_and_exit", false, "If true, attempt to migrate src cache to dest cache and exit upon completion.")
	clearCacheBeforeMigration = flag.Bool("cache.migration.clear_cache_before_migration", false, "If set, clear any existing cache content in the dest cache.")
	autoFixDoubleReadErrs     = flag.Bool("cache.migration.auto_fix_double_read_errs", false, "If set, automatically fix any double read errs in the dest cache")
)

type MigrationCache struct {
	Src        interfaces.Cache
	Dest       MigrationDestCache
	SrcRootDir string

	mu             sync.RWMutex
	eg             *errgroup.Group
	doubleReadChan chan *repb.Digest
}

type MigrationDestCache interface {
	interfaces.Cache

	// Implementation should be async, so that the double write doesn't slow down operations on the primary db
	// with 2 synchronous writes
	DoubleWrite(ctx context.Context, d DoubleWriteData) error
}

type DoubleWriteData struct {
	digest *repb.Digest
	add    *Add
	remove *Remove
}

type Add struct {
	Data []byte
}

type Remove struct{}

func Register(env environment.Env) error {
	mc := env.GetMigrationCache()
	if mc.Src == nil && mc.Dest == nil {
		return nil
	} else if mc.Src == nil || mc.Dest == nil {
		// Return error - both must be set
	}

	ctx := context.Background()
	mc.eg = &errgroup.Group{}
	env.SetCache(mc)

	if *migrateAndExit {
		if err := mc.Migrate(ctx, mc.srcRootDir); err != nil {
			log.Errorf("Migration failed: %s", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	mc.eg.Go(func() error {
		mc.ProcessDoubleReads(context.Background())
		return nil
	})

	return nil
}

func (c *MigrationCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	shouldDoubleRead := rand.Float64() <= *doubleReadPercentage
	if shouldDoubleRead {
		c.doubleReadChan <- d
	}

	return c.Src.Get(ctx, d)
}

func (c *MigrationCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.Src.Set(ctx, d, data); err != nil {
		return err
	}
	if err := c.Dest.DoubleWrite(ctx, DoubleWriteData{
		digest: d,
		add:    &Add{Data: data},
	}); err != nil {
		// Log error but don't fail the call
	}
	return nil
}

func (c *MigrationCache) ProcessDoubleReads(ctx context.Context) {
	for digest := range c.doubleReadChan {
		c.eg.Go(func() error {
			c.mu.Lock()
			defer c.mu.Unlock()

			primaryData, err := c.Src.Get(ctx, digest)
			secondaryData, err := c.Dest.Get(ctx, digest)

			if primaryData != secondaryData {
				// Log so we can identify and debug double read errors
			}

			//
			if *autoFixDoubleReadErrs {
				c.Dest.Set(ctx, digest, primaryData)
			}
		})
	}
}

// Migrate copies all data from the src cache to the dest cache
func (c *MigrationCache) Migrate(ctx context.Context, srcRootDir string) error {
	walkFn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil // keep going
		}
		rootDir := srcRootDir

		// Sanity checks on data

		digest, err := parseFilePath(rootDir, path)

		c.mu.Lock()
		data, err := c.Src.Get(ctx, digest)
		c.Dest.Set(ctx, digest, data)
		c.mu.Unlock()

		return nil
	}
	go func() {
		if err := filepath.WalkDir(srcRootDir, walkFn); err != nil {
			alert.UnexpectedEvent("walking_directory", "err: %s", err)
		}
	}()
	return nil
}
