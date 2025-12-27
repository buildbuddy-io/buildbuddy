package cache_service

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func TestGetMetadataReturnsStoredValues(t *testing.T) {
	env := testenv.GetTestEnv(t)
	service := New(env)

	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)

	data := []byte("cached-value")
	resourceName := &rspb.ResourceName{
		Digest: &repb.Digest{
			Hash:      strings.Repeat("a", 64),
			SizeBytes: int64(len(data)),
		},
		CacheType: rspb.CacheType_CAS,
	}
	require.NoError(t, env.GetCache().Set(ctx, resourceName, data))

	resp, err := service.GetMetadata(ctx, &capb.GetCacheMetadataRequest{
		ResourceName: resourceName,
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), resp.GetStoredSizeBytes())
	require.Equal(t, int64(len(data)), resp.GetDigestSizeBytes())
}

func TestGetMetadataMissingResource(t *testing.T) {
	env := testenv.GetTestEnv(t)
	service := New(env)

	ctx := context.Background()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)

	resourceName := &rspb.ResourceName{
		Digest: &repb.Digest{
			Hash:      strings.Repeat("b", 64),
			SizeBytes: 1,
		},
		CacheType: rspb.CacheType_CAS,
	}

	_, err = service.GetMetadata(ctx, &capb.GetCacheMetadataRequest{
		ResourceName: resourceName,
	})
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}
