// fsbench measures concurrent small-file creation across a configurable
// number of directory shards.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

var (
	benchmarkDir = flag.String("dir", "", "Empty directory where benchmark files will be created")
	shardCount   = flag.Int("dir_shards", 512, "Number of directory shards to use")
	fileCount    = flag.Int("file_count", 1_000_000, "Number of files to create")
	concurrency  = flag.Int("concurrency", 100, "Maximum number of concurrent file creations")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	if *benchmarkDir == "" {
		return errors.New("dir is required")
	}
	if *shardCount <= 0 {
		return errors.New("dir_shards must be positive")
	}
	if *fileCount <= 0 {
		return errors.New("file_count must be positive")
	}
	if *concurrency <= 0 {
		return errors.New("concurrency must be positive")
	}

	dir, err := filepath.Abs(*benchmarkDir)
	if err != nil {
		return fmt.Errorf("resolve benchmark dir: %w", err)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create benchmark dir: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read benchmark dir: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("benchmark dir %q is not empty", dir)
	}
	freeBytesBefore, err := freeBytes(dir)
	if err != nil {
		return err
	}

	log.Infof("Starting benchmark with n=%d, shards=%d, concurrency=%d, dir=%s", *fileCount, *shardCount, *concurrency, dir)
	durations := make([]time.Duration, *fileCount)
	eg, ctx := errgroup.WithContext(context.Background())
	eg.SetLimit(*concurrency)
	start := time.Now()
	for i := range *fileCount {
		if ctx.Err() != nil {
			break
		}
		eg.Go(func() error {
			operationStart := time.Now()
			defer func() {
				durations[i] = time.Since(operationStart)
			}()

			filePath := getFilePath(dir, i)
			if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
				return fmt.Errorf("create shard dir: %w", err)
			}
			if err := os.WriteFile(filePath, fmt.Appendf(nil, "file%d", i), 0644); err != nil {
				return fmt.Errorf("write file: %w", err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	wallTime := time.Since(start)

	unix.Sync()
	freeBytesAfter, err := freeBytes(dir)
	if err != nil {
		return err
	}

	slices.Sort(durations)
	fmt.Println("---")
	fmt.Printf("Shards:\t%d\n", *shardCount)
	fmt.Printf("Files:\t%d\n", *fileCount)
	fmt.Printf("Concurrency:\t%d\n", *concurrency)
	fmt.Printf("Wall:\t%s\n", wallTime)
	fmt.Printf("p25:\t%s\n", percentile(durations, 0.25))
	fmt.Printf("p50:\t%s\n", percentile(durations, 0.50))
	fmt.Printf("p75:\t%s\n", percentile(durations, 0.75))
	fmt.Printf("p90:\t%s\n", percentile(durations, 0.90))
	fmt.Printf("p95:\t%s\n", percentile(durations, 0.95))
	fmt.Printf("p99:\t%s\n", percentile(durations, 0.99))
	fmt.Printf("p99.9:\t%s\n", percentile(durations, 0.999))
	fmt.Printf("p99.99:\t%s\n", percentile(durations, 0.9999))
	fmt.Printf("Avg:\t%s\n", time.Duration(int64(float64(sum(durations))/float64(len(durations)))))
	fmt.Printf("Max:\t%s\n", durations[len(durations)-1])
	fmt.Printf("DiskUsageDelta:\t%d\n", int64(freeBytesBefore)-int64(freeBytesAfter))
	return nil
}

func freeBytes(path string) (uint64, error) {
	stats := &unix.Statfs_t{}
	if err := unix.Statfs(path, stats); err != nil {
		return 0, fmt.Errorf("statfs benchmark dir: %w", err)
	}
	return stats.Bavail * uint64(stats.Bsize), nil
}

func getFilePath(root string, i int) string {
	hash := xxhash.Sum64String(strconv.Itoa(i))
	shardNumber := int(hash % uint64(*shardCount))
	return filepath.Join(root, fmt.Sprintf("%d/file%d", shardNumber, i))
}

func percentile(durations []time.Duration, p float64) time.Duration {
	return durations[int(float64(len(durations))*p)]
}

func sum[T ~int | ~int64 | ~float64](values []T) T {
	var total T
	for _, value := range values {
		total += value
	}
	return total
}
