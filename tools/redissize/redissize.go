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
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/docker/go-units"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

var (
	target              = flag.String("target", "localhost:6380", "")
	printThresholdBytes = flag.Int("print_larger_than", 0, "Print keys larger than this size in bytes")
	outputFormat        = flag.String("output_format", "human_readable", "Output format: human_readable or csv")
)

func main() {
	flag.Parse()

	c := redis.NewClient(&redis.Options{Addr: *target})

	sizes := make(map[string]int64)
	counts := make(map[string]int64)

	type prefixSize struct {
		prefix string
		size   int64
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
		if newCursor == 0 {
			break
		}
		cursor = newCursor

		cmds, err := c.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, k := range keys {
				pipe.MemoryUsage(ctx, k)
			}
			return nil
		})
		if err != nil && err != redis.Nil {
			log.Fatalf("Pipeline error: %s", err)
		}

		for i, k := range keys {
			p := strings.Split(k, "/")[0]
			v, err := cmds[i].(*redis.IntCmd).Result()
			if err != nil && err != redis.Nil {
				log.Warningf("memusage err: %s", err)
			}
			if *printThresholdBytes > 0 && v > int64(*printThresholdBytes) {
				fmt.Println("Large entry:", k, "; size="+units.BytesSize(float64(v)))
			}
			if _, err := uuid.Parse(p); err == nil {
				p = "invocation_logs"
			}
			if strings.HasPrefix(p, "warning-") {
				p = "warning-*"
			}
			if p == "hit_tracker" && strings.HasSuffix(k, "/results") {
				p = "hit_tracker/*/results"
			} else if p == "hit_tracker" {
				p = "hit_tracker_counts"
			}
			totalSize += v
			sizes[p] += v
			counts[p] += 1
			count++
			if count%1000 == 0 {
				fmt.Println("===")
				fmt.Printf("Keys scanned: %d\n", count)
				fmt.Printf("Average size: %.2f bytes\n", float64(totalSize)/float64(count))
				var s []prefixSize
				for k, v := range sizes {
					s = append(s, prefixSize{k, v})
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
				if *outputFormat == "csv" {
					fmt.Println("key_type,total_size_bytes,average_size_bytes,key_count")
				}
				for _, v := range s {
					if *outputFormat == "human_readable" {
						fmt.Printf("%s  %s\tavg=%s\tn=%d\n", padRight(v.prefix, 24), padRight(units.BytesSize(float64(v.size)), 10), units.BytesSize(float64(v.size/counts[v.prefix])), counts[v.prefix])
					} else {
						fmt.Printf("%s,%d,%d,%d\n", v.prefix, v.size, v.size/counts[v.prefix], counts[v.prefix])
					}
				}
			}
		}
	}
}

func padRight(s string, length int) string {
	if len(s) >= length {
		return s
	}
	return s + strings.Repeat(" ", length-len(s))
}
