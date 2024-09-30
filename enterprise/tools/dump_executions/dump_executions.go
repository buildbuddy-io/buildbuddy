package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
)

var (
	limit   = flag.Int("n", 10_000_000, "Number of executions to fetch.")
	out     = flag.String("out", "/tmp/executions.jsonl.zst", "Output file")
	groupID = flag.String("group_id", "", "Filter to this group ID")
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	flag.Parse()
	for k, v := range map[string]any{
		"auto_migrate_db":               false,
		"database.slow_query_threshold": 60 * time.Second,
	} {
		if err := flagutil.SetValueForFlagName(k, v, nil, false); err != nil {
			return fmt.Errorf("set flag --%s=%v", k, v)
		}
	}
	_ = log.Configure()

	ctx := context.Background()

	env := real_environment.NewBatchEnv()

	dbh, err := db.GetConfiguredDatabase(ctx, env)
	if err != nil {
		return fmt.Errorf("configure DB: %w", err)
	}

	iids := make(chan string)
	executions := make(chan *tables.Execution, 4096)

	var eg errgroup.Group
	defer eg.Wait()

	// Cancel DB fetch ctx once we've hit the execution limit.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	eg.Go(func() (err error) {
		defer func() {
			if err != nil {
				cancel(err)
			}
		}()
		t := time.Now()
		for {
			log.Infof("Fetching most recent invocations up to %s", t)
			q := dbh.NewQuery(ctx, "get_invocations").Raw(`
				SELECT group_id, invocation_id, updated_at_usec
				FROM "Invocations"
				WHERE
					group_id = ?
					AND updated_at_usec <= ?
					AND updated_at_usec != 0
					AND invocation_status = 1
				ORDER BY updated_at_usec DESC
				LIMIT 100
			`, *groupID, t.UnixMicro())
			rows, err := db.ScanAll(q, &tables.Invocation{})
			if err != nil {
				return fmt.Errorf("scan invocations: %w", err)
			}
			if len(rows) == 0 {
				log.Infof("Scanned all invocations.")
				return io.EOF
			}
			t = time.UnixMicro(rows[len(rows)-1].UpdatedAtUsec)
			for _, inv := range rows {
				iids <- inv.InvocationID
			}
		}
	})

	// Fetch invocation IDs in batches
	// Get executions for each invocation
	for range 64 {
		eg.Go(func() (err error) {
			defer func() {
				if err != nil {
					cancel(err)
				}
			}()
			for {
				select {
				case <-ctx.Done():
					return nil
				case iid := <-iids:
					// Get executions for invocation
					log.Infof("Getting executions for invocation %s", iid)
					q := dbh.NewQuery(ctx, "get_invocation_executions").Raw(`
						SELECT *
						FROM "Executions" E, "InvocationExecutions" IE
						WHERE
							E.execution_id = IE.execution_id
							AND IE.invocation_id = ? 
					`, iid)
					rows, err := db.ScanAll(q, &tables.Execution{})
					if err != nil {
						return fmt.Errorf("scan executions: %w", err)
					}
					for _, row := range rows {
						executions <- row
					}
				}
			}
		})
	}

	// Dump executions to a compressed file, one JSON object per line
	f, err := os.Create(*out)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()
	w, err := zstd.NewWriter(f)
	if err != nil {
		return fmt.Errorf("create zstd writer: %w", err)
	}
	defer w.Close()

	for i := 0; i < *limit; i++ {
		var ex *tables.Execution
		select {
		case <-ctx.Done():
			if context.Cause(ctx) == io.EOF {
				break
			}
			return context.Cause(ctx)
		case ex = <-executions:
		}
		if ex == nil {
			break
		}
		b, err := json.Marshal(ex)
		if err != nil {
			return fmt.Errorf("marshal execution: %w", err)
		}
		b = append(b, '\n')
		// os.Stderr.WriteString("Writing execution: " + string(b))
		if _, err := w.Write(b); err != nil {
			return fmt.Errorf("write compressed execution: %w", err)
		}

		if (i+1)%10_000 == 0 {
			log.Infof("Wrote %d executions so far", i+1)
		}
	}

	// Flush zstd output
	if err := w.Close(); err != nil {
		return fmt.Errorf("flush and close zstd writer: %w", err)
	}

	return nil
}
