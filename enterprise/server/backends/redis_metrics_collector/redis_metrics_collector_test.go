package redis_metrics_collector_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_metrics_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

const (
	// noExpiration is a special expiration value indicating that no expiration
	// should be set.
	noExpiration time.Duration = 0
)

func TestSetAndGetAll(t *testing.T) {
	target := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(target))
	rbuf := redisutil.NewCommandBuffer(rdb)
	mc := redis_metrics_collector.New(rdb, rbuf)
	ctx := context.Background()
	const nKeys = 100_000

	// Do a bunch of Set() operations
	keys := make([]string, 0, nKeys)
	expectedValues := make([]string, 0, nKeys)
	for i := 0; i < nKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		keys = append(keys, key)
		val := fmt.Sprintf("val_%d", i)
		expectedValues = append(expectedValues, val)
		err := mc.Set(ctx, key, val, noExpiration)
		require.NoError(t, err)
	}
	err := rbuf.Flush(ctx)
	require.NoError(t, err)

	// Read back the values written via Set()
	values, err := mc.GetAll(ctx, keys...)
	require.NoError(t, err)
	require.Equal(t, expectedValues, values)
}
