// Tool to run bulk redis operations.
//
// List keys matching a pattern:
// $ bazel run -- tools/bulkredis -match='taskSize/*'
//
// Confirm the keys match what you expect, then delete all matching keys with
// $ bazel run -- tools/bulkredis -match='taskSize/*' -op=delete

package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/go-redis/redis/v8"
)

var (
	target = flag.String("target", "localhost:6380", "Redis target")
	match  = flag.String("match", "", "Redis keys to match")
	op     = flag.String("op", "print", "Bulk operation to perform. Just prints all keys by default.")
)

func main() {
	flag.Parse()

	if *match == "" {
		log.Fatalf("Missing -match arg")
	}

	c := redis.NewClient(&redis.Options{Addr: *target})

	ctx := context.Background()
	cursor := uint64(0)
	for {
		keys, newCursor, err := c.Scan(ctx, cursor, *match, 100_000).Result()
		if err != nil {
			log.Fatalf("Scan failed: %s", err)
		}
		if newCursor == 0 {
			break
		}
		cursor = newCursor

		_, err = c.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, k := range keys {
				switch *op {
				case "del", "delete":
					pipe.Del(ctx, k)
				default:
					fmt.Println(k)
				}
			}
			return nil
		})
		if err != nil {
			log.Fatalf("Pipeline failed: %s", err)
		}
		if *op != "print" {
			log.Infof("Processed %d keys", len(keys))
		}
	}
}
