package redisutil

import (
	"context"
	"log"
	"strings"

	"github.com/go-redis/redis/extra/redisotel/v8"
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

type HealthChecker struct {
	Rdb *redis.Client
}

func (c *HealthChecker) Check(ctx context.Context) error {
	return c.Rdb.Ping(ctx).Err()
}

func NewClient(redisTarget string, checker interfaces.HealthChecker, healthCheckName string) *redis.Client {
	return NewClientWithOpts(TargetToOptions(redisTarget), checker, healthCheckName)
}

func NewClientWithOpts(opts *redis.Options, checker interfaces.HealthChecker, healthCheckName string) *redis.Client {
	redisClient := redis.NewClient(opts)
	redisClient.AddHook(redisotel.NewTracingHook())
	checker.AddHealthCheck(healthCheckName, &HealthChecker{Rdb: redisClient})
	return redisClient
}
