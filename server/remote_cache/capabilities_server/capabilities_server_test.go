package capabilities_server_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
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

func TestGetCapabilities_RBEKey(t *testing.T) {
	env := testenv.GetTestEnv(t)
	u := testauth.User("user", "group")
	u.Capabilities = []cappb.Capability{cappb.Capability_CAS_WRITE, cappb.Capability_EXECUTOR_CACHE_WRITE}
	executor := testauth.User("executor", "group")
	executor.Capabilities = []cappb.Capability{cappb.Capability_REGISTER_EXECUTOR}
	auth := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{"user": u, "executor": executor})
	env.SetAuthenticator(auth)
	ctx, err := auth.WithAuthenticatedUser(t.Context(), "user")
	require.NoError(t, err)
	ctx = bazel_request.OverrideRequestMetadata(ctx, &repb.RequestMetadata{
		ToolDetails: &repb.ToolDetails{ToolName: "bazel", ToolVersion: "8.0.0"},
	})
	s := capabilities_server.NewCapabilitiesServer(env, true, true, false)
	rsp, err := s.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	require.NoError(t, err)
	require.True(t, rsp.GetExecutionCapabilities().GetExecEnabled())
	require.False(t, rsp.GetCacheCapabilities().GetActionCacheUpdateCapabilities().GetUpdateEnabled())
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(authutil.ExecutorAPIKeyHeader, "executor"))
	rsp, err = s.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	require.NoError(t, err)
	require.True(t, rsp.GetCacheCapabilities().GetActionCacheUpdateCapabilities().GetUpdateEnabled())
}
