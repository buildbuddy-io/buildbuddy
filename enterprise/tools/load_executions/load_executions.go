package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/qps"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	json "github.com/goccy/go-json"
)

var (
	in         = flag.String("in", "/tmp/executions.jsonl.zst", "Input file. Should be zstd-compressed, JSON-encoded Execution rows, one per line")
	olapTarget = flag.String("olap_target", "", "ClickHouse target where rows should be loaded.")
	sqlTarget  = flag.String("sql_target", "", "MySQL target where rows should be loaded.")
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	flag.Parse()
	for k, v := range map[string]any{
		// "auto_migrate_db":               false,
		"database.slow_query_threshold":      60 * time.Second,
		"database.data_source":               *sqlTarget,
		"database.max_open_conns":            100,
		"database.max_idle_conns":            25,
		"database.conn_max_lifetime_seconds": 300,
		"olap_database.data_source":          *olapTarget,
	} {
		if err := flagutil.SetValueForFlagName(k, v, nil, false); err != nil {
			return fmt.Errorf("set flag --%s=%v", k, v)
		}
	}
	_ = log.Configure()

	f, err := os.Open(*in)
	if err != nil {
		return fmt.Errorf("open input file: %w", err)
	}
	defer f.Close()
	r, err := zstd.NewReader(f)
	if err != nil {
		return fmt.Errorf("create zstd reader: %w", err)
	}
	defer r.Close()
	// Using goccy/go-json since we don't want JSON parsing to be a bottleneck.
	dec := json.NewDecoder(r)

	// Start row decoder in the background.
	rows := make(chan *tables.Execution, 4096)
	decoderErr := make(chan error)
	go func() {
		defer close(decoderErr)
		seen := map[string]bool{}
		for {
			row := &tables.Execution{}
			if err := dec.Decode(row); err != nil {
				if err == io.EOF {
					return
				}
				decoderErr <- err
				return
			}
			// TODO: dump script should not store dupes.
			if seen[row.ExecutionID] {
				continue
			}
			seen[row.ExecutionID] = true

			rows <- row
		}
	}()

	ctx := context.Background()

	env := real_environment.NewBatchEnv()

	monitoring.StartMonitoringHandler(env, "0.0.0.0:9090")

	if *sqlTarget != "" {
		if *olapTarget != "" {
			return fmt.Errorf("cannot set both -sql_target and -olap_target")
		}
		dbh, err := db.GetConfiguredDatabase(ctx, env)
		if err != nil {
			return fmt.Errorf("configure DB: %w", err)
		}
		env.SetDBHandle(dbh)
	} else if *olapTarget != "" {
		if err := clickhouse.Register(env); err != nil {
			return fmt.Errorf("configure OLAP DB: %w", err)
		}
	} else {
		return fmt.Errorf("must set -sql_target or -olap_target")
	}

	// Insert rows into MySQL / ClickHouse.
	insert := func(ctx context.Context, row *tables.Execution) error {
		if *sqlTarget != "" {
			return env.GetDBHandle().NewQuery(ctx, "insert_execution").Create(row)
		} else {
			executionProto := execution.TableExecToProto(row, &sipb.StoredInvocationLink{})
			return env.GetOLAPDBHandle().FlushExecutionStats(ctx, &sipb.StoredInvocation{}, []*repb.StoredExecution{executionProto})
		}
	}

	var numRows int64

	q := qps.NewCounter(2 * time.Second)
	go func() {
		for {
			time.Sleep(2 * time.Second)
			log.Infof("Inserted %d rows, current rate %.2f rows/s", atomic.LoadInt64(&numRows), q.Get())
		}
	}()

	start := time.Now()
	defer func() {
		log.Infof("Processed %d rows in %s", numRows, time.Since(start))
	}()

	eg, ctx := errgroup.WithContext(ctx)
	for range 32 {
		eg.Go(func() error {
			for {
				select {
				case row := <-rows:
					if err := insert(ctx, row); err != nil {
						return err
					}
					atomic.AddInt64(&numRows, 1)
					q.Inc()
				case err := <-decoderErr:
					if err == nil {
						return nil
					}
					return fmt.Errorf("decoder error: %w", err)
				}
			}
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("")
	}

	return nil
}
