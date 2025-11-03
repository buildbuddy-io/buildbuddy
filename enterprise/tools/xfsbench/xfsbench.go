// xfs benchmarking tool.
//
// To set up a temporary loopback XFS and run the tool using the temp FS,
// run ./enterprise/tools/xfsbench/xfsbench.sh
package main

import (
	"context"
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
	shardCount = flag.Int("dir_shards", 512, "Number of directory shards to use")
	fileCount  = flag.Int("file_count", 1_000_000, "Number of files to create")
)

const (
	filecachePath = "/mnt/tempxfs/cache"
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	log.Infof("Starting benchmark: n=%d, shards=%d, dir=%s", *fileCount, *shardCount, filecachePath)

	if err := os.MkdirAll(filecachePath, 0755); err != nil {
		return err
	}

	durations := make([]time.Duration, *fileCount)
	// Concurrently write tons of small files to filecache
	eg, ctx := errgroup.WithContext(context.Background())
	eg.SetLimit(100)
	start := time.Now()
	for i := range *fileCount {
		if ctx.Err() != nil {
			break
		}
		// if i%10_000 == 0 {
		// 	fmt.Printf("Starting write %d", i)
		// }
		eg.Go(func() error {
			start := time.Now()
			defer func() {
				dur := time.Since(start)
				durations[i] = dur
				if dur > 1*time.Second {
					log.Printf("[%d] slow! (%s)", i, time.Since(start))
				}
			}()

			// Create shard directory if needed
			filePath := getFilePath(i)
			if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
				return err
			}

			// Once we get past 100K files, start deleting the oldest file.
			// if i >= 100_000 {
			// 	oldestFilePath := getFilePath(i - 100_000)
			// 	if err := os.Remove(oldestFilePath); err != nil {
			// 		return err
			// 	}
			// }

			return os.WriteFile(filePath, fmt.Appendf(nil, "file%d", i), 0644)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	wallTime := time.Since(start)

	// Sync to ensure files are written to disk, then measure disk usage.
	unix.Sync()
	stats := &unix.Statfs_t{}
	if err := unix.Statfs(filecachePath, stats); err != nil {
		return err
	}
	allBytes := stats.Blocks * uint64(stats.Bsize)
	freeBytes := stats.Bfree * uint64(stats.Bsize)
	usedBytes := allBytes - freeBytes

	slices.Sort(durations)
	fmt.Println("---")
	fmt.Printf("Shards:\t%d\n", *shardCount)
	fmt.Printf("Files:\t%d\n", *fileCount)
	fmt.Printf("Wall:\t%s\n", wallTime)
	fmt.Printf("p25:\t%s\n", durations[int(float64(len(durations))*0.25)])
	fmt.Printf("p50:\t%s\n", durations[int(float64(len(durations))*0.50)])
	fmt.Printf("p75:\t%s\n", durations[int(float64(len(durations))*0.75)])
	fmt.Printf("p90:\t%s\n", durations[int(float64(len(durations))*0.90)])
	fmt.Printf("p95:\t%s\n", durations[int(float64(len(durations))*0.95)])
	fmt.Printf("p99:\t%s\n", durations[int(float64(len(durations))*0.99)])
	fmt.Printf("p99.9:\t%s\n", durations[int(float64(len(durations))*0.999)])
	fmt.Printf("p99.99:\t%s\n", durations[int(float64(len(durations))*0.9999)])
	fmt.Printf("Avg:\t%s\n", time.Duration(int64(float64(sum(durations))/float64(len(durations)))))
	fmt.Printf("Max:\t%s\n", durations[len(durations)-1])
	fmt.Printf("DiskUsage:\t%d\n", usedBytes)
	return nil
}

func getFilePath(i int) string {
	hash := xxhash.Sum64String(strconv.Itoa(i))
	shardNumber := int(hash % uint64(*shardCount))
	return filepath.Join(filecachePath, fmt.Sprintf("%d/file%d", shardNumber, i))
}

func sum[T ~int | ~int64 | ~float64](s []T) T {
	var sum T
	for _, v := range s {
		sum += v
	}
	return sum
}
