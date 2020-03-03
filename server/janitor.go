package janitor

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/tryflame/buildbuddy/server/config"
	"github.com/tryflame/buildbuddy/server/database"
	"github.com/tryflame/buildbuddy/server/interfaces"
	"github.com/tryflame/buildbuddy/server/tables"
)

var (
	cleanupInterval   = flag.Duration("cleanup_interval", 10*60*time.Second, "How often the janitor cleanup tasks will run")
	cleanupWorkers    = flag.Int("cleanup_workers", 1, "How many cleanup tasks to run")
	logDeletionErrors = flag.Bool("log_deletion_errors", false, "If true; log errors when ttl-deleting expired data")
)

type Janitor struct {
	ticker *time.Ticker
	quit   chan struct{}

	bs interfaces.Blobstore
	db interfaces.Database

	ttl time.Duration
}

func NewJanitor(bs interfaces.Blobstore, db *database.Database, c *config.Configurator) *Janitor {
	return &Janitor{
		bs:  bs,
		db:  db,
		ttl: time.Duration(c.GetStorageTtlSeconds()) * time.Second,
	}
}

func (j *Janitor) deleteInvocation(invocation *tables.Invocation) {
	ctx := context.Background()
	if err := j.bs.DeleteBlob(ctx, invocation.BlobID); err != nil && *logDeletionErrors {
		log.Printf("Error deleting blob (%s): %s", invocation.BlobID, err)
	}

	// Try to delete the row too, even if blob deletion failed.
	if err := j.db.DeleteInvocation(ctx, invocation.InvocationID); err != nil && *logDeletionErrors {
		log.Printf("Error deleting invocation (%s): %s", invocation.InvocationID, err)
	}
}

func (j *Janitor) deleteExpiredInvocations() {
	ctx := context.Background()
	cutoff := time.Now().Add(-1 * j.ttl)
	expired, err := j.db.LookupExpiredInvocations(ctx, cutoff, 10)
	if err != nil && *logDeletionErrors {
		log.Printf("Error finding expired deletions: %s", err)
		return
	}

	for _, exp := range expired {
		j.deleteInvocation(exp)
	}
}

func (j *Janitor) Start() {
	j.ticker = time.NewTicker(*cleanupInterval)
	j.quit = make(chan struct{})

	if j.ttl == 0 {
		log.Printf("configured TTL was 0; disabling janitor")
		return
	}

	for i := 0; i < *cleanupWorkers; i++ {
		go func() {
			for {
				select {
				case <-j.ticker.C:
					j.deleteExpiredInvocations()
				case <-j.quit:
					log.Printf("Cleanup task %d exiting.", 0)
					break
				}
			}
		}()
	}
}

func (j *Janitor) Stop() {
	close(j.quit)
	j.ticker.Stop()
}
