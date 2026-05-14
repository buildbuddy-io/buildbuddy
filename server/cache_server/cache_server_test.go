package cache_server_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/require"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
)

func TestGetMetadata(t *testing.T) {
	env := testenv.GetTestEnv(t)
	service := cache_server.New(env)

	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)

	rn, buf := testdigest.RandomCASResourceBuf(t, 100)
	err = env.GetCache().Set(ctx, rn, buf)
	require.NoError(t, err)

	resp, err := service.GetMetadata(ctx, &capb.GetCacheMetadataRequest{
		ResourceName: rn,
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(buf)), resp.GetStoredSizeBytes())
	require.Equal(t, int64(len(buf)), resp.GetDigestSizeBytes())
}
