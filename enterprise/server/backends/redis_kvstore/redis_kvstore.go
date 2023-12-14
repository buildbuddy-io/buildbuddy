package redis_kvstore

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
)

const (
	ttl = 3 * 24 * time.Hour
)

type store struct {
	rdb redis.UniversalClient
}

func Register(env *real_environment.RealEnv) error {
	rdb := env.GetDefaultRedisClient()
	if rdb == nil {
		return nil
	}
	env.SetKeyValStore(New(rdb))
	return nil
}

func New(rdb redis.UniversalClient) *store {
	return &store{rdb}
}

func (s *store) Set(ctx context.Context, key string, val []byte) error {
	if val == nil {
		return s.rdb.Del(ctx, key).Err()
	}
	return s.rdb.Set(ctx, key, val, ttl).Err()
}

func (s *store) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := s.rdb.Get(ctx, key).Bytes()
	if err == nil {
		return b, nil
	}
	if err == redis.Nil {
		return nil, status.NotFoundErrorf("Key %q not found in key value store", key)
	}
	return nil, err
}
