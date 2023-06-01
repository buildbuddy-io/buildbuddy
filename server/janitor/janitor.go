package janitor

import (
	"context"
	"flag"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	// Flags shared by both invocation and execution janitor.
	logDeletionErrors = flag.Bool("log_deletion_errors", false, "If true; log errors when ttl-deleting expired data")

	// Flags for Invocation Janitor.
	invocationTTLSeconds = flag.Int("storage.ttl_seconds", 0, "The time, in seconds, to keep invocations before deletion. 0 disables invocation deletion.")

	invocationCleanupBatchSize = flag.Int("storage.cleanup_batch_size", 10, "How many invocations to delete in each janitor cleanup task")
	invocationCleanupInterval  = flag.Duration("cleanup_interval", 10*60*time.Second, "How often the janitor cleanup tasks will run")
	invocationCleanupWorkers   = flag.Int("cleanup_workers", 1, "How many cleanup tasks to run")

	// Flags for Execution Janitor.
	executionTTL = flag.Duration("storage.execution.ttl", 0, "The time, in seconds, to keep invocations before deletion. 0 disables invocation deletion.")

	executionCleanupBatchSize = flag.Int("storage.execution.cleanup_batch_size", 200, "How many invocations to delete in each janitor cleanup task")
	executionCleanupInterval  = flag.Duration("storage.execution.cleanup_interval", 5*time.Minute, "How often the janitor cleanup tasks will run")
	executionCleanupWorkers   = flag.Int("storage.execution.cleanup_workers", 1, "How many cleanup tasks to run")
)

type JanitorConfig struct {
	env                 environment.Env
	ttl                 time.Duration
	batchSize           int
	errorLoggingEnabled bool
}

type Janitor struct {
	ticker *time.Ticker
	quit   chan struct{}

	name       string
	interval   time.Duration
	numWorkers int

	config *JanitorConfig

	deleteFn func(c *JanitorConfig)
}

func deleteInvocation(c *JanitorConfig, invocation *tables.Invocation) {
	ctx := c.env.GetServerContext()
	if err := c.env.GetBlobstore().DeleteBlob(ctx, invocation.BlobID); err != nil && c.errorLoggingEnabled {
		log.Warningf("Error deleting blob (%s): %s", invocation.BlobID, err)
	}

	// Try to delete the row too, even if blob deletion failed.
	if err := c.env.GetInvocationDB().DeleteInvocation(ctx, invocation.InvocationID); err != nil && c.errorLoggingEnabled {
		log.Warningf("Error deleting invocation (%s): %s", invocation.InvocationID, err)
	}
}

func deleteExpiredInvocations(c *JanitorConfig) {
	ctx := c.env.GetServerContext()
	cutoff := time.Now().Add(-1 * c.ttl)
	expired, err := c.env.GetInvocationDB().LookupExpiredInvocations(ctx, cutoff, c.batchSize)
	if err != nil && c.errorLoggingEnabled {
		log.Warningf("Error finding expired deletions: %s", err)
		return
	}

	for _, exp := range expired {
		deleteInvocation(c, exp)
	}
}

func NewInvocationJanitor(env environment.Env) *Janitor {
	c := &JanitorConfig{
		env:                 env,
		ttl:                 time.Duration(*invocationTTLSeconds) * time.Second,
		batchSize:           *invocationCleanupBatchSize,
		errorLoggingEnabled: *logDeletionErrors,
	}
	return &Janitor{
		name:       "invocation janitor",
		config:     c,
		interval:   *invocationCleanupInterval,
		numWorkers: *invocationCleanupWorkers,
		deleteFn:   deleteExpiredInvocations,
	}
}

func lookupExpiredExecutionIDs(ctx context.Context, c *JanitorConfig) ([]interface{}, error) {
	dbh := c.env.GetDBHandle()
	cutoff := time.Now().Add(-1 * c.ttl)

	dbOpts := db.Opts().WithQueryName("lookup_expired_executions")
	stmt := `SELECT execution_id FROM "Executions" WHERE created_at_usec < ? LIMIT ?`
	rows, err := dbh.RawWithOptions(ctx, dbOpts, stmt, cutoff.UnixMicro(), c.batchSize).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	executionIDs := make([]interface{}, 0, c.batchSize)
	for rows.Next() {
		var executionID *string
		if err := rows.Scan(&executionID); err != nil {
			return nil, err
		}
		executionIDs = append(executionIDs, *executionID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return executionIDs, nil
}

func deleteExpiredExecutions(c *JanitorConfig) {
	ctx := c.env.GetServerContext()
	dbh := c.env.GetDBHandle()

	executionIDs, err := lookupExpiredExecutionIDs(ctx, c)
	if err != nil && c.errorLoggingEnabled {
		log.Warningf("Error finding expired deletions: %s", err)
		return
	}

	if len(executionIDs) == 0 {
		return
	}

	err = dbh.TransactionWithOptions(ctx, db.Opts().WithQueryName("delete_expired_executions"), func(tx *db.DB) error {
		if txError := tx.Exec(`DELETE FROM "Executions" WHERE execution_id IN (?`+strings.Repeat(",?", len(executionIDs)-1)+`)`, executionIDs...).Error; txError != nil {
			return txError
		}
		return tx.Exec(`DELETE FROM "InvocationExecutions" WHERE execution_id IN (?`+strings.Repeat(",?", len(executionIDs)-1)+`)`, executionIDs...).Error
	})
	if err != nil && c.errorLoggingEnabled {
		log.Warningf("Error deleting expired executions: %s", err)
	}

}

func NewExecutionJanitor(env environment.Env) *Janitor {
	c := &JanitorConfig{
		env:                 env,
		ttl:                 *executionTTL,
		batchSize:           *executionCleanupBatchSize,
		errorLoggingEnabled: *logDeletionErrors,
	}
	return &Janitor{
		name:       "execution janitor",
		config:     c,
		interval:   *executionCleanupInterval,
		numWorkers: *executionCleanupWorkers,
		deleteFn:   deleteExpiredExecutions,
	}
}

func (j *Janitor) Start() {
	j.ticker = time.NewTicker(j.interval)
	j.quit = make(chan struct{})

	if j.config.ttl == 0 {
		log.Infof("Configured TTL was 0; disabling %s", j.name)
		return
	}

	for i := 0; i < j.numWorkers; i++ {
		go func() {
			for {
				select {
				case <-j.ticker.C:
					j.deleteFn(j.config)
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
