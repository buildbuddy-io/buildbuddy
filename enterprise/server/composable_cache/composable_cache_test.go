package composable_cache_test

import (
	"context"
	"flag"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/composable_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

func testEnvAndContext(t *testing.T) (environment.Env, context.Context) {
	target := testredis.Start(t).Target
	te := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{RedisTarget: target})
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te)
	require.NoError(t, err)
	return te, ctx
}

func writeDigest(ctx context.Context, t *testing.T, c interfaces.Cache, sizeBytes int64) *rspb.ResourceName {
	r, buf := testdigest.RandomCASResourceBuf(t, sizeBytes)
	w, err := c.Writer(ctx, r)
	require.NoError(t, err)
	_, err = w.Write(buf)
	require.NoError(t, err)
	err = w.Commit()
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)
	return r
}

func readAndVerifyDigest(ctx context.Context, t *testing.T, c interfaces.Cache, d *rspb.ResourceName) {
	r, err := c.Reader(ctx, d, 0, 0)
	require.NoError(t, err)
	rd, err := digest.Compute(r, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	err = r.Close()
	require.NoError(t, err)
	require.Equal(t, digest.NewKey(d.GetDigest()), digest.NewKey(rd))
}

func TestReadThrough(t *testing.T) {
	env1, _ := testEnvAndContext(t)
	flag.Set("cache.redis.max_value_size_bytes", "100")
	outer := redis_cache.NewCache(env1.GetDefaultRedisClient())

	env2, ctx := testEnvAndContext(t)
	flag.Set("cache.redis.max_value_size_bytes", "1000")
	inner := redis_cache.NewCache(env2.GetDefaultRedisClient())

	c := composable_cache.NewComposableCache(outer, inner, composable_cache.ModeReadThrough|composable_cache.ModeWriteThrough)

	// Write a digest into the inner cache and check that it gets written to
	// outer cache on read.
	{
		r := writeDigest(ctx, t, inner, 99)

		// Check that we can read the digest through the composable cache.
		readAndVerifyDigest(ctx, t, c, r)

		// Verify that the outer cache has the digest data.
		readAndVerifyDigest(ctx, t, outer, r)
	}

	// Write a digest that doesn't fit into the outer cache and test that we can
	// still read it from the composable cache.
	{
		r := writeDigest(ctx, t, inner, 100)

		// Check that we can read the digest through the composable cache.
		readAndVerifyDigest(ctx, t, c, r)
	}

	// Perform a partial read and check that outer has the correct (full) blob.
	{
		rn := writeDigest(ctx, t, inner, 99)

		r, err := c.Reader(ctx, rn, 0, 0)
		require.NoError(t, err)

		buf := make([]byte, 50)
		_, err = r.Read(buf)
		require.NoError(t, err)
		r.Close()

		readAndVerifyDigest(ctx, t, outer, rn)
	}
}
