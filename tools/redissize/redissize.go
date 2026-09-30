// Tool to show statistics on the size of redis keys categorized by prefix.
//
// First, port-forward to the redis instance you want to analyze:
// $ kubectl port-forward -n redis-prod redis-sharded-3 6380:6379
//
// Then run the tool:
// $ bazel run tools/redissize
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/docker/go-units"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/mattn/go-isatty"
)

var (
	target              = flag.String("target", "localhost:6380", "")
	printThresholdBytes = flag.Int("print_larger_than", 0, "Print keys larger than this size in bytes")
	outputFormat        = flag.String("output_format", "human_readable", "Output format: human_readable or csv")
	maxRemainingTTL     = flag.Duration("max_ttl", 0*time.Second, "Only includes keys if the remaining TTL is less than this value, for measuring data expiring soon")
	percentiles         = flag.String("percentiles", "50,99,99.9,100", "Comma-separated size percentiles in [0,100]; estimates use 1.1x buckets from 1 byte to 100 GB, and p100 is exact")
	topN                = flag.Int("top_n", 10, "Number of largest keys to print after each table; 0 disables tracking, and CSV output ignores this flag")

	isTTY = isatty.IsTerminal(os.Stdout.Fd())

	sizeBucketBounds = func() []int64 {
		bounds := []int64{0}
		for size := 1.0; size < 100e9; size *= 1.1 {
			if bound := int64(size); bound > bounds[len(bounds)-1] {
				bounds = append(bounds, bound)
			}
		}
		return append(bounds, 100e9)
	}()
)

type keySize struct {
	key  string
	size int64
}

type topKeys struct {
	limit   int
	entries []keySize
}

func (t *topKeys) add(key string, size int64) {
	if t.limit <= 0 {
		return
	}
	// SCAN may return a key more than once. Keep its largest observed size.
	for i, entry := range t.entries {
		if entry.key == key {
			if entry.size >= size {
				return
			}
			t.entries = slices.Delete(t.entries, i, i+1)
			break
		}
	}
	i := 0
	for i < len(t.entries) && t.entries[i].size >= size {
		i++
	}
	if i >= t.limit {
		return
	}
	if len(t.entries) == t.limit {
		t.entries = t.entries[:t.limit-1]
	}
	t.entries = slices.Insert(t.entries, i, keySize{key: key, size: size})
}

type sizeStats struct {
	totalSize int64
	count     int64
	maxSize   int64
	buckets   []int64
}

func (s *sizeStats) add(size int64) {
	if s.buckets == nil {
		// The extra bucket includes sizes above the largest finite bound.
		s.buckets = make([]int64, len(sizeBucketBounds)+1)
	}
	i, _ := slices.BinarySearch(sizeBucketBounds, size)
	s.buckets[i]++
	s.totalSize += size
	s.count++
	s.maxSize = max(s.maxSize, size)
}

func (s *sizeStats) percentile(p float64) int64 {
	if s.count == 0 || p == 100 {
		return s.maxSize
	}
	rank := max(int64(1), int64(math.Ceil(p*float64(s.count)/100)))
	var count int64
	for i, n := range s.buckets {
		count += n
		if count >= rank {
			// Use the bucket's upper bound, capped at the observed maximum.
			// For the overflow bucket, the maximum is the only upper bound.
			if i == len(sizeBucketBounds) {
				return s.maxSize
			}
			return min(sizeBucketBounds[i], s.maxSize)
		}
	}
	return s.maxSize
}

func parsePercentiles(value string) ([]float64, error) {
	var result []float64
	for field := range strings.SplitSeq(value, ",") {
		p, err := strconv.ParseFloat(strings.TrimSpace(field), 64)
		if err != nil || math.IsNaN(p) || p < 0 || p > 100 {
			return nil, fmt.Errorf("invalid percentile %q; expected a number in [0,100]", field)
		}
		result = append(result, p)
	}
	return result, nil
}

func main() {
	flag.Parse()

	ps, err := parsePercentiles(*percentiles)
	if err != nil {
		log.Fatalf("parse -percentiles: %s", err)
	}

	c := redis.NewClient(&redis.Options{Addr: *target})

	stats := make(map[string]*sizeStats)
	largestKeys := topKeys{}
	if *outputFormat == "human_readable" {
		largestKeys.limit = *topN
	}

	ctx := context.Background()
	cursor := uint64(0)
	count := 0
	totalSize := int64(0)
	for {
		keys, newCursor, err := c.Scan(ctx, cursor, "*", 4096).Result()
		if err != nil {
			log.Fatalf("scan err: %s", err)
		}
		cursor = newCursor

		cmds, err := c.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, k := range keys {
				if *maxRemainingTTL > 0 {
					pipe.TTL(ctx, k)
				}
				pipe.MemoryUsage(ctx, k)
			}
			return nil
		})
		if err != nil && err != redis.Nil {
			log.Fatalf("Pipeline error: %s", err)
		}

		for i, k := range keys {
			usageCmdIdx := i
			if *maxRemainingTTL > 0 {
				usageCmdIdx = 2*i + 1
				ttl, err := cmds[2*i].(*redis.DurationCmd).Result()
				if err != nil && err != redis.Nil {
					log.Warningf("ttl err: %s", err)
				}
				if ttl > *maxRemainingTTL {
					continue
				}
			}

			usage, err := cmds[usageCmdIdx].(*redis.IntCmd).Result()
			if err != nil {
				if err != redis.Nil {
					log.Warningf("memusage err: %s", err)
				}
				continue
			}
			if *printThresholdBytes > 0 && usage > int64(*printThresholdBytes) {
				fmt.Println("Large entry:", k, "; size="+colorSize(float64(usage), units.BytesSize(float64(usage))))
			}
			// SCAN may return a key more than once, and unlike the top keys
			// list, the counts, totals, and percentiles do not dedupe keys.
			// Deduping would require remembering every scanned key, so these
			// stats are approximate instead.
			p := keyType(k)
			totalSize += usage
			if stats[p] == nil {
				stats[p] = &sizeStats{}
			}
			stats[p].add(usage)
			largestKeys.add(k, usage)
			count++
			if count%1000 == 0 {
				printOutput(os.Stdout, count, totalSize, stats, ps, largestKeys.entries)
			}
		}
		if cursor == 0 {
			break
		}
	}
	// Skip the final print if the periodic print already showed the final
	// count, so that the last output block is not duplicated.
	if count == 0 || count%1000 != 0 {
		printOutput(os.Stdout, count, totalSize, stats, ps, largestKeys.entries)
	}
}

func keyType(key string) string {
	firstPiece, _, _ := strings.Cut(key, "/")
	if _, err := uuid.Parse(firstPiece); err == nil {
		return "invocation_logs"
	}
	if strings.HasPrefix(firstPiece, "warning-") {
		return "warning-*"
	}
	if firstPiece == "hit_tracker" && strings.HasSuffix(key, "/results") {
		return "hit_tracker/*/results"
	} else if firstPiece == "hit_tracker" {
		return "hit_tracker_counts"
	}
	return firstPiece
}

func esc(codes ...int) string {
	if !isTTY {
		return ""
	}
	parts := make([]string, len(codes))
	for i, code := range codes {
		parts[i] = strconv.Itoa(code)
	}
	return "\x1b[" + strings.Join(parts, ";") + "m"
}

func colorSize(size float64, text string) string {
	if *outputFormat != "human_readable" {
		return text
	}
	switch {
	case size > units.GiB:
		return esc(31) + text + esc(0)
	case size > 100*units.MiB:
		return esc(33) + text + esc(0)
	default:
		return text
	}
}

func printOutput(w io.Writer, count int, totalSize int64, stats map[string]*sizeStats, percentiles []float64, largestKeys []keySize) {

	type prefixSize struct {
		prefix string
		size   int64
	}

	fmt.Fprintln(w, "===")
	fmt.Fprintf(w, "Keys scanned: %d\n", count)
	averageSize := float64(totalSize) / float64(max(count, 1))
	fmt.Fprintf(w, "Average size: %s bytes\n", colorSize(averageSize, fmt.Sprintf("%.2f", averageSize)))
	var s []prefixSize
	for k, v := range stats {
		s = append(s, prefixSize{k, v.totalSize})
	}
	slices.SortFunc(s, func(a, b prefixSize) int {
		if a.size > b.size {
			return -1
		} else if a.size == b.size {
			return 0
		} else {
			return 1
		}
	})
	if *outputFormat == "human_readable" {
		table := [][]string{{"KEY TYPE", "N", "TOTAL", "AVG"}}
		for _, p := range percentiles {
			if p == 100 {
				table[0] = append(table[0], "MAX")
			} else {
				table[0] = append(table[0], fmt.Sprintf("P%g", p))
			}
		}
		for _, v := range s {
			stats := stats[v.prefix]
			row := []string{
				v.prefix,
				strconv.FormatInt(stats.count, 10),
				units.BytesSize(float64(v.size)),
				units.BytesSize(float64(v.size / stats.count)),
			}
			for _, p := range percentiles {
				row = append(row, units.BytesSize(float64(stats.percentile(p))))
			}
			table = append(table, row)
		}
		widths := make([]int, len(table[0]))
		for _, row := range table {
			for i, cell := range row {
				widths[i] = max(widths[i], len(cell))
			}
		}
		for rowIndex, row := range table {
			for i, cell := range row {
				// Measure the visible text before adding ANSI color sequences.
				padding := widths[i] - len(cell) + 2
				if rowIndex > 0 {
					stats := stats[row[0]]
					switch {
					case i == 2:
						cell = colorSize(float64(stats.totalSize), cell)
					case i == 3:
						cell = colorSize(float64(stats.totalSize)/float64(stats.count), cell)
					case i >= 4:
						cell = colorSize(float64(stats.percentile(percentiles[i-4])), cell)
					}
				}
				if i == len(row)-1 {
					fmt.Fprintln(w, cell)
				} else {
					fmt.Fprintf(w, "%s%s", cell, strings.Repeat(" ", padding))
				}
			}
		}
		if len(largestKeys) > 0 {
			fmt.Fprintf(w, "\nTop %d keys by size:\n", len(largestKeys))
			for _, entry := range largestKeys {
				size := colorSize(float64(entry.size), fmt.Sprintf("%9s", units.BytesSize(float64(entry.size))))
				fmt.Fprintf(w, "  %s  %s\n", size, entry.key)
			}
		}
		return
	}
	if *outputFormat == "csv" {
		fmt.Fprint(w, "key_type,total_size_bytes,average_size_bytes,key_count")
		for _, p := range percentiles {
			fmt.Fprintf(w, ",p%g_size_bytes", p)
		}
		fmt.Fprintln(w)
	}
	for _, v := range s {
		stats := stats[v.prefix]
		fmt.Fprintf(w, "%s,%d,%d,%d", v.prefix, v.size, v.size/stats.count, stats.count)
		for _, p := range percentiles {
			fmt.Fprintf(w, ",%d", stats.percentile(p))
		}
		fmt.Fprintln(w)
	}
}
