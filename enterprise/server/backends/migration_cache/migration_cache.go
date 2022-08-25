package migration_cache

import (
	"context"
	"flag"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"sync"
	"time"
)

var (
	isMigrationEnabled = flag.Bool("cache.migration", false, "Whether a migration is happening. If true, read migration details from config/migration.yaml")

	// TODO - Move these flags to migration config
	doubleReadPercentage     = flag.Float64("cache.migration_cache.double_read_percentage", 0, "The percentage of reads we should double read, to ensure the two caches have the same data. Float [0, 1]. 0 for no double reads, 1 for double reads on every read")
	autoFixDoubleReadErrs    = flag.Bool("cache.migration.auto_fix_double_read_errs", false, "If set, automatically fix any double read errs in the dest cache")
	maxDoubleWritesPerSecond = flag.Int("cache.migration.max_double_writes_per_second", 1, "We will buffer double writes, so copying data doesn't monopolize resources (as copying will happen during Gets, which are very frequent)")
)

type MigrationCache struct {
	Src  interfaces.Cache
	Dest interfaces.Cache

	mu              sync.RWMutex
	eg              *errgroup.Group
	doubleReadChan  chan *repb.Digest
	doubleWriteChan chan *DoubleWriteData
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
	if !*isMigrationEnabled {
		return nil
	}

	mc := ParseConfig(env)
	if mc == nil {
		return nil
	}
	mc.eg = &errgroup.Group{}

	env.SetCache(mc)

	mc.eg.Go(func() error {
		mc.ProcessDoubleWrites(context.Background())
		return nil
	})
	mc.eg.Go(func() error {
		mc.ProcessDoubleReads(context.Background())
		return nil
	})

	return nil
}

func (c *MigrationCache) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	c.mu.Lock()
	srcData, err := c.Src.Get(ctx, d)

	// Enqueue non-blocking copying during Get
	select {
	case c.doubleWriteChan <- &DoubleWriteData{
		digest: d,
		add:    &Add{Data: srcData},
	}:
	default:
		// Log error that channel is full so we can increase the size if needed, but don't block
	}
	c.mu.Unlock()

	// Double read some proportion to guarantee that data is consistent between caches
	shouldDoubleRead := rand.Float64() <= *doubleReadPercentage
	if shouldDoubleRead {
		c.doubleReadChan <- d
	}

	return srcData, err
}

func (c *MigrationCache) Set(ctx context.Context, d *repb.Digest, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double write in parallel
	c.eg.Go(func() error {
		return c.Src.Set(ctx, d, data)
	})
	c.eg.Go(func() error {
		// Enqueue blocking double write
		c.doubleWriteChan <- &DoubleWriteData{
			digest: d,
			add:    &Add{Data: data},
		}
		return nil
	})

	// If error during write to source cache (source of truth), must delete from destination cache
	err := c.eg.Wait()
	if err != nil {
		c.doubleWriteChan <- &DoubleWriteData{
			digest: d,
			remove: &Remove{},
		}
	}

	return err
}

func (c *MigrationCache) ProcessDoubleReads(ctx context.Context) {
	for digest := range c.doubleReadChan {
		c.eg.Go(func() error {
			c.mu.Lock()
			defer c.mu.Unlock()

			primaryData, primaryErr := c.Src.Get(ctx, digest)
			secondaryData, secondaryErr := c.Dest.Get(ctx, digest)

			if primaryErr != nil {
				// Return early - just skip the double read
				return nil
			} else if secondaryData == nil {
				// Return early - data may have just not been copied yet
				return nil
			} else if secondaryErr != nil {
				// Log - so we know if there are read problems with new cache
			} else if primaryData != secondaryData {
				// Log so we can identify and debug double read errors

				if *autoFixDoubleReadErrs {
					c.doubleWriteChan <- &DoubleWriteData{
						digest: digest,
						add:    &Add{Data: primaryData},
					}
				}
			}
		})
	}
}

func (c *MigrationCache) ProcessDoubleWrites(ctx context.Context) {
	for doubleWriteData := range c.doubleWriteChan {
		c.eg.Go(func() error {
			c.mu.Lock()
			defer c.mu.Unlock()

			if doubleWriteData.remove != nil {
				c.Dest.Delete(ctx, doubleWriteData.digest)
			} else if doubleWriteData.add != nil {
				c.Dest.Set(ctx, doubleWriteData.digest, doubleWriteData.add.Data)
			} else {
				// Log - exactly one field should be set
			}
		})

		// Buffer double writes, to ensure copying data doesn't monopolize resources
		sleepMs := int((float64(1) / float64(*maxDoubleWritesPerSecond)) * 1000)
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
}
