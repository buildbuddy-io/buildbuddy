package composable_cache_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/composable_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

func testEnvAndContext(t *testing.T) (environment.Env, context.Context) {
	target := testredis.Start(t).Target
	te := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{RedisTarget: target})
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
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
	rd, err := digest.Compute(r, repb.DigestFunction_SHA256, d.GetDigest().GetSizeBytes())
	require.NoError(t, err)
	err = r.Close()
	require.NoError(t, err)
	require.Equal(t, digest.NewKey(d.GetDigest()), digest.NewKey(rd))
}

func TestReadThrough(t *testing.T) {
	env1, _ := testEnvAndContext(t)
	flags.Set(t, "cache.redis.max_value_size_bytes", 100)
	outer := redis_cache.NewCache(env1.GetDefaultRedisClient())

	env2, ctx := testEnvAndContext(t)
	flags.Set(t, "cache.redis.max_value_size_bytes", 1000)
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

func TestReadsGITSHA1(t *testing.T) {
	te := testenv.GetTestEnv(t)
	ctx, err := prefix.AttachUserPrefixToContext(context.Background(), te.GetAuthenticator())
	require.NoError(t, err)
	inner := te.GetCache()
	outer, err := memory_cache.NewMemoryCache(100_000)
	require.NoError(t, err)
	c := composable_cache.NewComposableCache(outer, inner, composable_cache.ModeReadThrough)
	var resources []*rspb.ResourceName
	for _, s := range []string{"outer", "inner", "missing"} {
		d, err := digest.Compute(strings.NewReader(s), repb.DigestFunction_GITSHA1, int64(len(s)))
		require.NoError(t, err)
		resources = append(resources, digest.NewCASResourceName(d, "test", repb.DigestFunction_GITSHA1).ToProto())
	}
	require.NoError(t, outer.Set(ctx, resources[0], []byte("outer")))
	require.NoError(t, inner.Set(ctx, resources[1], []byte("inner")))
	missing, err := c.FindMissing(ctx, resources)
	require.NoError(t, err)
	require.Equal(t, []*repb.Digest{resources[2].GetDigest()}, missing)
	found, err := c.GetMulti(ctx, resources)
	require.NoError(t, err)
	require.Equal(t, map[*repb.Digest][]byte{
		resources[0].GetDigest(): []byte("outer"),
		resources[1].GetDigest(): []byte("inner"),
	}, found)

	r, err := c.Reader(ctx, resources[1], 0, 2)
	require.NoError(t, err)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, []byte("in"), data)

	r, err = c.Reader(ctx, resources[1], 0, 0)
	require.NoError(t, err)
	data, err = io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, []byte("inner"), data)

	data, err = outer.Get(ctx, resources[1])
	require.NoError(t, err)
	require.Equal(t, []byte("inner"), data)
}
