package main

import (
	"context"
	"flag"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/go-redis/redis/v8"
)

var (
	target = flag.String("target", "localhost:6380", "")
)

func main() {
	flag.Parse()

	c := redis.NewClient(&redis.Options{Addr: *target})

	ctx := context.Background()
	cursor := uint64(0)
	for {
		keys, newCursor, err := c.Scan(ctx, cursor, "hit_tracker/*/results", 5000).Result()
		if err != nil {
			log.Fatalf("scan err: %s", err)
		}
		if newCursor == 0 {
			break
		}
		cursor = newCursor

		var ttls []*redis.DurationCmd
		_, err = c.Pipelined(ctx, func(c redis.Pipeliner) error {
			for _, k := range keys {
				ttls = append(ttls, c.TTL(ctx, k))
			}
			return nil
		})
		if err != nil {
			log.Fatalf("TTL pipeline failed: %s", err)
		}

		var del []string
		for i, k := range keys {
			ttl, err := ttls[i].Result()
			if err != nil {
				log.Fatalf("ttl err: %s", err)
			}
			if ttl < 90*time.Minute {
				log.Infof("Deleting key %s with ttl %s", k, ttl)
				del = append(del, k)
			}
		}
		if len(del) == 0 {
			continue
		}
		if err := c.Del(ctx, del...).Err(); err != nil {
			log.Fatalf("delete err: %s", err)
		}
	}
}
