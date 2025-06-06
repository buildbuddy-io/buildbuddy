// Loads a mysqldump from a mysql Usages table into a local ClickHouse instance
// using the RawUsage table.
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

var (
	usageMysqlDumpPath = flag.String("usage_mysqldump_path", "", "Path to a .sql file (from mysqldump) containing usage data to import into clickhouse.")
)

func main() {
	flag.CommandLine.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), `
Usage: usage_clickhouse_prototype [OPTIONS]

Loads a mysqldump sql file from a mysql Usages table into a local ClickHouse
instance using the RawUsage table.

Examples:
	# Create/wipe persistent data directory.
	export BB_CLICKHOUSE_DATA_DIR=/tmp/clickhouse_tmp
	rm -rf "$BB_CLICKHOUSE_DATA_DIR"
	# Configure ClickHouse port.
	export BB_CLICKHOUSE_PORT=9003
	# Run the tool, initializing ClickHouse and flags via tools/clickhouse.
	bazel run -- tools/usage_clickhouse_prototype $(tools/clickhouse)

Options:
`)
		flag.CommandLine.PrintDefaults()
	}
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal("Error: " + err.Error())
	}
}

func run() error {
	if err := log.Configure(); err != nil {
		return fmt.Errorf("configure logging: %w", err)
	}

	ctx := context.Background()
	env := real_environment.NewBatchEnv()

	// Register clickhouse to env and auto-migrate the DB.
	if err := clickhouse.Register(env); err != nil {
		return fmt.Errorf("register clickhouse: %w", err)
	}
	if env.GetOLAPDBHandle() == nil {
		return fmt.Errorf("no OLAP DB configured (append $(tools/clickhouse) to command line)")
	}

	return importRawUsage(ctx, env)
}

func importRawUsage(ctx context.Context, env environment.Env) error {
	// Import usage data from mysql into the local clickhouse instance.
	if *usageMysqlDumpPath == "" {
		return fmt.Errorf("usage_mysqldump_path is required")
	}

	// Parse the SQL dump file.
	f, err := os.Open(*usageMysqlDumpPath)
	if err != nil {
		return fmt.Errorf("open usage TSV file: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	startMarker := []byte("INSERT INTO `Usages` VALUES")
	foundStartMarker := false
	bytesProcessed := 0
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		defer func() {
			bytesProcessed += advance
		}()
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if !foundStartMarker {
			idx := bytes.Index(data, startMarker)
			if idx >= 0 {
				foundStartMarker = true
				return idx + len(startMarker), nil, nil
			}
			return 0, nil, nil
		}
		i := bytes.IndexByte(data, '(')
		if i < 0 {
			if atEOF {
				return len(data), nil, nil
			}
			return 0, nil, nil
		}
		j := bytes.IndexByte(data, ')')
		if j < 0 {
			return 0, nil, nil
		}
		return j + 1, data[i+1 : j], nil
	})

	var numClickhouseRowsFlushed atomic.Int64
	var wg sync.WaitGroup
	workerCh := make(chan []tables.Usage, 16)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range workerCh {
				clickhouseRows := make([]olaptables.RawUsage, 0, len(batch))
				for _, row := range batch {
					clickhouseRows = appendRawUsageRows(clickhouseRows, row)
				}
				numClickhouseRowsFlushed.Add(int64(len(clickhouseRows)))
				if err := env.GetOLAPDBHandle().InsertUsages(ctx, clickhouseRows); err != nil {
					log.Errorf("insert usages: %s", err)
				}
			}
		}()
	}

	const batchSize = 100_000
	currentBatch := make([]tables.Usage, 0, batchSize)
	flushBatch := func() {
		workerCh <- currentBatch
		currentBatch = make([]tables.Usage, 0, batchSize)
	}
	addToBatch := func(row tables.Usage) {
		currentBatch = append(currentBatch, row)
		if len(currentBatch) >= batchSize {
			flushBatch()
		}
	}

	rowsScanned := 0

	showProgress := func() {
		fmt.Printf("Scanned %dM mysql rows (%d MB); flushed %dM clickhouse rows\n", rowsScanned/1e6, bytesProcessed/1e6, numClickhouseRowsFlushed.Load()/1e6)
	}

	for scanner.Scan() {
		rowsScanned++
		entry := scanner.Text()

		// Parse entries from schema:
		/*
			CREATE TABLE `Usages` (
			0 `created_at_usec` bigint DEFAULT NULL,
			1 `updated_at_usec` bigint DEFAULT NULL,
			2 `group_id` varchar(255) NOT NULL,
			3 `period_start_usec` bigint NOT NULL,
			4 `final_before_usec` bigint DEFAULT '0',
			5 `invocations` bigint DEFAULT NULL,
			6 `cas_cache_hits` bigint DEFAULT NULL,
			7 `action_cache_hits` bigint DEFAULT NULL,
			8 `total_download_size_bytes` bigint DEFAULT NULL,
			9 `region` varchar(255) NOT NULL,
			10 `linux_execution_duration_usec` bigint NOT NULL DEFAULT '0',
			11 `mac_execution_duration_usec` bigint NOT NULL DEFAULT '0',
			12 `total_upload_size_bytes` bigint NOT NULL DEFAULT '0',
			13 `total_cached_action_exec_usec` bigint NOT NULL DEFAULT '0',
			14 `origin` varchar(255) NOT NULL DEFAULT '',
			15 `client` varchar(255) NOT NULL DEFAULT '',
			16 `cpu_nanos` bigint NOT NULL DEFAULT '0',
			17 `usage_id` varchar(255) DEFAULT NULL,
			18 `memory_gb_usec` bigint NOT NULL DEFAULT '0',
			19 `self_hosted_linux_execution_duration_usec` bigint NOT NULL DEFAULT '0',
			20 `self_hosted_mac_execution_duration_usec` bigint NOT NULL DEFAULT '0',
		*/
		var mysqlRow tables.Usage
		n := 0
		for field := range strings.SplitSeq(entry, ",") {
			n++
			if field == "NULL" || field == "" {
				continue
			}
			// Parse string
			if field[0] == '\'' {
				field = field[1 : len(field)-1]
			}
			i := n - 1
			switch i {
			// case 0:
			// 	mysqlRow.CreatedAtUsec, err = strconv.ParseInt(field, 10, 64)
			// 	if err != nil {
			// 		return fmt.Errorf("parse created_at_usec (index %d, value '%s'): %w", n, field, err)
			// 	}
			// case 1:
			// 	mysqlRow.UpdatedAtUsec, err = strconv.ParseInt(field, 10, 64)
			// 	if err != nil {
			// 		return fmt.Errorf("parse updated_at_usec (index %d, value '%s'): %w", n, field, err)
			// 	}
			case 2:
				mysqlRow.GroupID = field
			case 3:
				mysqlRow.PeriodStartUsec, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse period_start_usec (index %d, value '%s'): %w", n, field, err)
				}
			// case 4:
			// 	mysqlRow.FinalBeforeUsec, err = strconv.ParseInt(field, 10, 64)
			// 	if err != nil {
			// 		return fmt.Errorf("parse final_before_usec (index %d, value '%s'): %w", n, field, err)
			// 	}
			case 5:
				mysqlRow.Invocations, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse invocations (index %d, value '%s'): %w", n, field, err)
				}
			case 6:
				mysqlRow.CASCacheHits, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse cas_cache_hits (index %d, value '%s'): %w", n, field, err)
				}
			case 7:
				mysqlRow.ActionCacheHits, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse action_cache_hits (index %d, value '%s'): %w", n, field, err)
				}
			case 8:
				mysqlRow.TotalDownloadSizeBytes, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse total_download_size_bytes (index %d, value '%s'): %w", n, field, err)
				}
			case 9:
				mysqlRow.Region = field
			case 10:
				mysqlRow.LinuxExecutionDurationUsec, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse linux_execution_duration_usec (index %d, value '%s'): %w", n, field, err)
				}
			case 11:
				mysqlRow.MacExecutionDurationUsec, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse mac_execution_duration_usec (index %d, value '%s'): %w", n, field, err)
				}
			case 12:
				mysqlRow.TotalUploadSizeBytes, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse total_upload_size_bytes (index %d, value '%s'): %w", n, field, err)
				}
			case 13:
				mysqlRow.TotalCachedActionExecUsec, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse total_cached_action_exec_usec (index %d, value '%s'): %w", n, field, err)
				}
			case 14:
				mysqlRow.Origin = field
			case 15:
				mysqlRow.Client = field
			case 16:
				mysqlRow.CPUNanos, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse cpu_nanos (index %d, value '%s'): %w", n, field, err)
				}
			// case 17:
			// 	mysqlRow.UsageID = field
			case 18:
				mysqlRow.MemoryGBUsec, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse memory_gb_usec (index %d, value '%s'): %w", n, field, err)
				}
			case 19:
				mysqlRow.SelfHostedLinuxExecutionDurationUsec, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse self_hosted_linux_execution_duration_usec (index %d, value '%s'): %w", n, field, err)
				}
			case 20:
				mysqlRow.SelfHostedMacExecutionDurationUsec, err = strconv.ParseInt(field, 10, 64)
				if err != nil {
					return fmt.Errorf("parse self_hosted_mac_execution_duration_usec (index %d, value '%s'): %w", n, field, err)
				}
			}
		}
		if n != 21 {
			return fmt.Errorf("unexpected number of fields: %d", n)
		}
		addToBatch(mysqlRow)
		if rowsScanned%1e6 == 0 {
			showProgress()
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan usage data: %w", err)
	}

	flushBatch()
	close(workerCh)
	wg.Wait()

	log.Infof("Done")
	showProgress()

	log.Infof("Press Ctrl+C to quit...")
	select {}

	// return nil
}

func appendRawUsageRows(clickhouseRows []olaptables.RawUsage, mysqlRow tables.Usage) []olaptables.RawUsage {
	// var labels []string
	// if mysqlRow.Origin != "" {
	// 	labels = append(labels, mysqlRow.Origin)
	// }
	// if mysqlRow.Client != "" {
	// 	labels = append(labels, mysqlRow.Client)
	// }
	labels := map[string]string{}
	if mysqlRow.Origin != "" {
		labels["origin"] = mysqlRow.Origin
	}
	if mysqlRow.Client != "" {
		labels["client"] = mysqlRow.Client
	}
	// For this prototype, create one SKU for each (origin, client) pair. In
	// practice, not all of these SKUs will make sense - for example, with linux
	// execution duration, the client should always be "executor" (?)
	// skuPrefix := strings.ToUpper(strings.Join(labels, "_"))
	// if skuPrefix != "" {
	// 	skuPrefix += "_"
	// }
	skuPrefix := ""

	baseRow := &olaptables.RawUsage{
		GroupID:       mysqlRow.GroupID,
		PeriodStart:   time.UnixMicro(mysqlRow.PeriodStartUsec),
		CollectionKey: mysqlRow.Region,
		Labels:        labels,
	}
	// BES
	if mysqlRow.Invocations > 0 {
		chRow := *baseRow
		// Assume all invocations are bazel invocations for this prototype,
		// since we don't currently store those in the usage table. For the real
		// thing, we'd track workflow and remote bazel invocations as well.
		chRow.SKU = skuPrefix + "BAZEL_INVOCATIONS"
		chRow.Count = mysqlRow.Invocations
		clickhouseRows = append(clickhouseRows, chRow)
	}
	// RBE
	if mysqlRow.LinuxExecutionDurationUsec > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "LINUX_EXECUTION_DURATION"
		chRow.Count = mysqlRow.LinuxExecutionDurationUsec
		clickhouseRows = append(clickhouseRows, chRow)
	}
	if mysqlRow.MacExecutionDurationUsec > 0 {
		// Ignore origin and client labels; they aren't relevant for mac
		// execution duration.
		chRow := *baseRow
		chRow.SKU = skuPrefix + "MAC_EXECUTION_DURATION_USEC"
		chRow.Count = mysqlRow.MacExecutionDurationUsec
		clickhouseRows = append(clickhouseRows, chRow)
	}
	if mysqlRow.SelfHostedLinuxExecutionDurationUsec > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "SELF_HOSTED_LINUX_EXECUTION_DURATION_USEC"
		chRow.Count = mysqlRow.SelfHostedLinuxExecutionDurationUsec
		clickhouseRows = append(clickhouseRows, chRow)
	}
	if mysqlRow.SelfHostedMacExecutionDurationUsec > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "SELF_HOSTED_MAC_EXECUTION_DURATION_USEC"
		chRow.Count = mysqlRow.SelfHostedMacExecutionDurationUsec
		clickhouseRows = append(clickhouseRows, chRow)
	}
	// Execution
	if mysqlRow.CPUNanos > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "EXECUTION_CPU_NANOS"
		chRow.Count = mysqlRow.CPUNanos
		clickhouseRows = append(clickhouseRows, chRow)
	}
	if mysqlRow.MemoryGBUsec > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "EXECUTION_MEMORY_GB_USEC"
		chRow.Count = mysqlRow.MemoryGBUsec
		clickhouseRows = append(clickhouseRows, chRow)
	}
	// Cache
	if mysqlRow.TotalCachedActionExecUsec > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "CACHED_ACTION_EXEC_DURATION_USEC"
		chRow.Count = mysqlRow.TotalCachedActionExecUsec
		clickhouseRows = append(clickhouseRows, chRow)
	}
	if mysqlRow.TotalDownloadSizeBytes > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "CACHE_DOWNLOAD_SIZE_BYTES"
		chRow.Count = mysqlRow.TotalDownloadSizeBytes
		clickhouseRows = append(clickhouseRows, chRow)
	}
	if mysqlRow.TotalUploadSizeBytes > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "CACHE_UPLOAD_SIZE_BYTES"
		chRow.Count = mysqlRow.TotalUploadSizeBytes
		clickhouseRows = append(clickhouseRows, chRow)
	}
	if mysqlRow.CASCacheHits > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "CAS_CACHE_HITS"
		chRow.Count = mysqlRow.CASCacheHits
		clickhouseRows = append(clickhouseRows, chRow)
	}
	if mysqlRow.ActionCacheHits > 0 {
		chRow := *baseRow
		chRow.SKU = skuPrefix + "ACTION_CACHE_HITS"
		chRow.Count = mysqlRow.ActionCacheHits
		clickhouseRows = append(clickhouseRows, chRow)
	}
	return clickhouseRows
}
