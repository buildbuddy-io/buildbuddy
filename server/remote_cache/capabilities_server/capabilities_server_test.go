package capabilities_server_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type cdcDisabledFlagProvider struct {
	interfaces.ExperimentFlagProvider
}

func (cdcDisabledFlagProvider) Boolean(ctx context.Context, flagName string, defaultValue bool, opts ...any) bool {
	return false
}

func (cdcDisabledFlagProvider) Int64(ctx context.Context, flagName string, defaultValue int64, opts ...any) int64 {
	return 512 * 1024
}

func TestGetCapabilities_CDCDoesNotVaryWithExperiments(t *testing.T) {
	flags.Set(t, "cache.avg_chunk_size_bytes", 1024*1024)
	env := testenv.GetTestEnv(t)
	env.SetExperimentFlagProvider(cdcDisabledFlagProvider{})
	s := capabilities_server.NewCapabilitiesServer(env, true, false, false)

	rsp, err := s.GetCapabilities(t.Context(), &repb.GetCapabilitiesRequest{})
	require.NoError(t, err)
	assert.True(t, rsp.GetCacheCapabilities().GetSplitBlobSupport())
	assert.True(t, rsp.GetCacheCapabilities().GetSpliceBlobSupport())
	assert.Equal(t, uint64(1024*1024), rsp.GetCacheCapabilities().GetFastCdc_2020Params().GetAvgChunkSizeBytes())
}
