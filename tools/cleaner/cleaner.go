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
		keys, newCursor, err := c.Scan(ctx, cursor, "monitoredPubSub/*", 100).Result()
		if err != nil {
			log.Fatalf("scan err: %s", err)
		}
		if newCursor == 0 {
			break
		}
		cursor = newCursor
		p := c.Pipeline()
		for _, k := range keys {
			ttl, err := p.TTL(ctx, k).Result()
			if err != nil {
				log.Fatalf("ttl err: %s", err)
			}
			if ttl < 4*time.Hour {
				log.Infof("deleting key %s with ttl %s", k, ttl)
				if err := c.Del(ctx, k).Err(); err != nil {
					log.Fatalf("delete err: %s", err)
				}
			}
		}
	}
}
