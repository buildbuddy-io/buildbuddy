package testredis

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

type deterministicHash struct{}

func (deterministicHash) Get(key string) string {
	switch key {
	case "direct", "pipeline", "transaction":
		return "shard0"
	default:
		// Ring's fallback routing hashes a random integer. Sending every
		// unexpected key to shard1 makes missing command metadata fail
		// deterministically rather than relying on random distribution.
		return "shard1"
	}
}

func TestCommandInfoCompatibility(t *testing.T) {
	ctx := context.Background()
	client := Start(t).Client()
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	info, err := client.Command(ctx).Result()
	require.NoError(t, err)
	for _, name := range []string{"get", "hset", "lpush", "sadd", "set", "xadd", "zadd"} {
		command := info[name]
		require.NotNil(t, command, "missing COMMAND metadata for %q", name)
		require.Equal(t, int8(1), command.FirstKeyPos, name)
		require.Equal(t, int8(1), command.LastKeyPos, name)
		require.Equal(t, int8(1), command.StepCount, name)
	}
}

func TestRingRoutesCommandsByKey(t *testing.T) {
	ctx := context.Background()
	shards := StartShardedTCP(t, 2)
	client := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"shard0": shards.Shards[0].Target,
			"shard1": shards.Shards[1].Target,
		},
		NewConsistentHash: func([]string) redis.ConsistentHash {
			return deterministicHash{}
		},
	})
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	tests := []struct {
		name  string
		write func() error
	}{
		{
			name: "direct",
			write: func() error {
				return client.HSet(ctx, "direct", "field", "value").Err()
			},
		},
		{
			name: "pipeline",
			write: func() error {
				pipe := client.Pipeline()
				pipe.HSet(ctx, "pipeline", "field", "value")
				_, err := pipe.Exec(ctx)
				return err
			},
		},
		{
			name: "transaction",
			write: func() error {
				pipe := client.TxPipeline()
				pipe.HSet(ctx, "transaction", "field", "value")
				_, err := pipe.Exec(ctx)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, test.write())
		})
	}

	shard0 := shards.Shards[0].Client()
	shard1 := shards.Shards[1].Client()
	t.Cleanup(func() {
		require.NoError(t, shard0.Close())
		require.NoError(t, shard1.Close())
	})
	for _, test := range tests {
		value, err := shard0.HGet(ctx, test.name, "field").Result()
		require.NoError(t, err)
		require.Equal(t, "value", value)

		_, err = shard1.HGet(ctx, test.name, "field").Result()
		require.ErrorIs(t, err, redis.Nil)
	}
}
