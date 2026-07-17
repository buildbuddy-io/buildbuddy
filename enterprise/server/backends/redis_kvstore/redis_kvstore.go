package redis_kvstore

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/go-redis/redis/v8"
)

const (
	ttl = 1 * 24 * time.Hour
)

type store struct {
	rdb redis.UniversalClient
}

var _ interfaces.KeyValStore = (*store)(nil)

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

// replaceSuffixScript replaces the value at `key` from `offset` onward with
// `data`, truncating the value to `offset`+len(`data`), if the value
// currently has length `expectedLength`. On success it refreshes the key's
// TTL to `ttl` seconds and returns -1; on a length mismatch it writes nothing
// and returns the value's current length.
//
// KEYS[1]: key
// ARGV[1]: expectedLength
// ARGV[2]: offset
// ARGV[3]: data
// ARGV[4]: ttl (seconds)
var replaceSuffixScript = redis.NewScript(`
	local len = redis.call('STRLEN', KEYS[1])
	if len ~= tonumber(ARGV[1]) then
		return len
	end
	local offset = tonumber(ARGV[2])
	local data = ARGV[3]
	if offset + #data >= len then
		redis.call('SETRANGE', KEYS[1], offset, data)
	else
		local prefix = ''
		if offset > 0 then
			prefix = redis.call('GETRANGE', KEYS[1], 0, offset - 1)
		end
		redis.call('SET', KEYS[1], prefix .. data)
	end
	redis.call('EXPIRE', KEYS[1], ARGV[4])
	return -1
`)

func (s *store) ReplaceSuffix(ctx context.Context, key string, expectedLength, offset int64, data []byte) error {
	if offset < 0 || offset > expectedLength {
		return status.InvalidArgumentErrorf("offset %d out of range for value of length %d", offset, expectedLength)
	}
	n, err := replaceSuffixScript.Run(ctx, s.rdb, []string{key}, expectedLength, offset, data, int64(ttl.Seconds())).Int64()
	if err != nil {
		return err
	}
	if n >= 0 {
		return status.FailedPreconditionErrorf("value at key %q has length %d, expected %d", key, n, expectedLength)
	}
	return nil
}
