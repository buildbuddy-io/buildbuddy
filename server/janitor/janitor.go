package janitor

import (
	"flag"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	ttlSeconds = flag.Int("storage.ttl_seconds", 0, "The time, in seconds, to keep invocations before deletion. 0 disables invocation deletion.")

	cleanupBatchSize  = flag.Int("storage.cleanup_batch_size", 10, "How many invocations to delete in each janitor cleanup task")
	cleanupInterval   = flag.Duration("cleanup_interval", 10*60*time.Second, "How often the janitor cleanup tasks will run")
	cleanupWorkers    = flag.Int("cleanup_workers", 1, "How many cleanup tasks to run")
	logDeletionErrors = flag.Bool("log_deletion_errors", false, "If true; log errors when ttl-deleting expired data")
)

type Janitor struct {
	ticker *time.Ticker
	quit   chan struct{}

	env environment.Env
	ttl time.Duration
}

func NewJanitor(env environment.Env) *Janitor {
	return &Janitor{
		env: env,
		ttl: time.Duration(*ttlSeconds) * time.Second,
	}
}

func (j *Janitor) deleteInvocation(invocation *tables.Invocation) {
	ctx := j.env.GetServerContext()
	if err := j.env.GetBlobstore().DeleteBlob(ctx, invocation.BlobID); err != nil && *logDeletionErrors {
		log.Warningf("Error deleting blob (%s): %s", invocation.BlobID, err)
	}

	// Try to delete the row too, even if blob deletion failed.
	if err := j.env.GetInvocationDB().DeleteInvocation(ctx, invocation.InvocationID); err != nil && *logDeletionErrors {
		log.Warningf("Error deleting invocation (%s): %s", invocation.InvocationID, err)
	}
}

func (j *Janitor) deleteExpiredInvocations() {
	ctx := j.env.GetServerContext()
	cutoff := time.Now().Add(-1 * j.ttl)
	expired, err := j.env.GetInvocationDB().LookupExpiredInvocations(ctx, cutoff, *cleanupBatchSize)
	if err != nil && *logDeletionErrors {
		log.Warningf("Error finding expired deletions: %s", err)
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
		log.Infof("Configured TTL was 0; disabling invocation janitor")
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
					return
				}
			}
		}()
	}
}

func (j *Janitor) Stop() {
	close(j.quit)
	j.ticker.Stop()
}
