package janitor

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/tryflame/buildbuddy/server/blobstore"
	"github.com/tryflame/buildbuddy/server/config"
	"github.com/tryflame/buildbuddy/server/database"
	"github.com/tryflame/buildbuddy/server/tables"
)

var (
	cleanupInterval = flag.Duration("cleanup_interval", 60 * time.Second, "How often the janitor cleanup tasks will run")
	cleanupWorkers = flag.Int("cleanup_workers", 1, "How many cleanup tasks to run")
	logDeletionErrors = flag.Bool("log_deletion_errors", false, "If true; log errors when ttl-deleting expired data")
)

type Janitor struct {
	ticker *time.Ticker
	quit chan struct{}

	bs blobstore.Blobstore
	db *database.Database

	ttl time.Duration
}

func NewJanitor(bs blobstore.Blobstore, db *database.Database, c *config.Configurator) *Janitor {
	return &Janitor{
		bs: bs,
		db: db,
		ttl: time.Duration(c.GetStorageTtlSeconds()) * time.Second,
	}
}

func (j *Janitor) deleteInvocation(invocation *tables.Invocation) {
	if err := j.bs.DeleteBlob(context.Background(), invocation.BlobID); err != nil {
		if *logDeletionErrors {
			log.Printf("Error deleting blob (%s): %s", invocation.BlobID, err)
		}
	}
	// Try to delete the row too, even if blob deletion failed.
	if err := j.db.GormDB.Delete(&tables.Invocation{InvocationID: invocation.InvocationID}).Error; err != nil {
		if *logDeletionErrors {
			log.Printf("Error deleting row (%s): %s", invocation.InvocationID, err)
		}
	}
}

func (j *Janitor) deleteExpiredInvocations() {
	cutoffUsec := time.Now().Add(j.ttl).UnixNano()
	rows, err := j.db.GormDB.Raw(`SELECT * FROM Invocations as i
                                      WHERE i.created_at_usec < ?
                                      LIMIT 10`, cutoffUsec).Rows()
	if err != nil {
		log.Printf("error querying rows to delete: %s", err)
		return
	}
	defer rows.Close()

	var invocation tables.Invocation
	for rows.Next() {
		if err := j.db.GormDB.ScanRows(rows, &invocation); err != nil {
			log.Printf("error scanning row: %s", err)
			break
		}
		defer j.deleteInvocation(&invocation)
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
				case <- j.ticker.C:
					j.deleteExpiredInvocations()
				case <- j.quit:
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
