package redisutil

import (
	"context"
	"log"
	"strings"

	"github.com/go-redis/redis/v8"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
)

func isRedisURI(redisTarget string) bool {
	return strings.HasPrefix(redisTarget, "redis://") ||
		strings.HasPrefix(redisTarget, "rediss://") ||
		strings.HasPrefix(redisTarget, "unix://")
}

func TargetToOptions(redisTarget string) *redis.Options {
	if !isRedisURI(redisTarget) {
		return &redis.Options{
			Addr:     redisTarget,
			Password: "", // no password set
			DB:       0,  // use default DB
		}
	} else if opt, err := redis.ParseURL(redisTarget); err == nil {
		return opt
	} else {
		log.Println(err)
		log.Println(
			"The supported redis URI formats are:\n" +
				"redis[s]://[[USER][:PASSWORD]@][HOST][:PORT]" +
				"[/DATABASE]\nor:\nunix://[[USER][:PASSWORD]@]" +
				"SOCKET_PATH[?db=DATABASE]\nIf using a socket path, " +
				"path must be an absolute unix path.")
		return &redis.Options{}
	}
}

type healthChecker struct {
	rdb *redis.Client
}

func (c *healthChecker) Check(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func NewClient(redisTarget string, checker interfaces.HealthChecker, healthCheckName string) *redis.Client {
	rdb := redis.NewClient(TargetToOptions(redisTarget))
	checker.AddHealthCheck(healthCheckName, &healthChecker{rdb})
	return rdb
}
