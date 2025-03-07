package janitor

import (
	"context"
	"database/sql"
	"flag"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
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
	env       environment.Env
	ttl       time.Duration
	batchSize int
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
	if err := c.env.GetBlobstore().DeleteBlob(ctx, invocation.BlobID); err != nil {
		log.Warningf("Error deleting blob (%s): %s", invocation.BlobID, err)
	}

	// Try to delete the row too, even if blob deletion failed.
	if err := c.env.GetInvocationDB().DeleteInvocation(ctx, invocation.InvocationID); err != nil {
		log.Warningf("Error deleting invocation (%s): %s", invocation.InvocationID, err)
	}
}

func deleteExpiredInvocations(c *JanitorConfig) {
	ctx := c.env.GetServerContext()
	cutoff := time.Now().Add(-1 * c.ttl)
	expired, err := c.env.GetInvocationDB().LookupExpiredInvocations(ctx, cutoff, c.batchSize)
	if err != nil {
		log.Warningf("Error finding expired deletions: %s", err)
		return
	}

	for _, exp := range expired {
		deleteInvocation(c, exp)
	}
}

func NewInvocationJanitor(env environment.Env) *Janitor {
	c := &JanitorConfig{
		env:       env,
		ttl:       time.Duration(*invocationTTLSeconds) * time.Second,
		batchSize: *invocationCleanupBatchSize,
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

	stmt := `SELECT execution_id FROM "Executions" WHERE created_at_usec < ? LIMIT ?`
	rq := dbh.NewQuery(ctx, "janitor_lookup_expired_executions").Raw(stmt, cutoff.UnixMicro(), c.batchSize)
	executionIDs := make([]interface{}, 0, c.batchSize)
	err := rq.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
		var executionID *string
		if err := row.Scan(&executionID); err != nil {
			return err
		}
		executionIDs = append(executionIDs, *executionID)
		return nil
	})
	return executionIDs, err
}

func deleteExpiredExecutions(c *JanitorConfig) {
	ctx := c.env.GetServerContext()
	dbh := c.env.GetDBHandle()

	executionIDs, err := lookupExpiredExecutionIDs(ctx, c)
	if err != nil {
		log.Warningf("Error finding expired deletions: %s", err)
		return
	}

	if len(executionIDs) == 0 {
		return
	}

	err = dbh.Transaction(ctx, func(tx interfaces.DB) error {
		if txError := tx.NewQuery(ctx, "janitor_delete_executions").Raw(
			`DELETE FROM "Executions" WHERE execution_id IN (?`+strings.Repeat(",?", len(executionIDs)-1)+`)`, executionIDs...).Exec().Error; txError != nil {
			return txError
		}
		return tx.NewQuery(ctx, "janitor_delete_execution_links").Raw(
			`DELETE FROM "InvocationExecutions" WHERE execution_id IN (?`+strings.Repeat(",?", len(executionIDs)-1)+`)`, executionIDs...).Exec().Error
	})
	if err != nil {
		log.Warningf("Error deleting expired executions: %s", err)
	}

}

func NewExecutionJanitor(env environment.Env) *Janitor {
	c := &JanitorConfig{
		env:       env,
		ttl:       *executionTTL,
		batchSize: *executionCleanupBatchSize,
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
