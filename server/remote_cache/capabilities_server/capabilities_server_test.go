package capabilities_server_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestGetCapabilities_ImageCacheWrite(t *testing.T) {
	for _, test := range []struct {
		name          string
		capabilities  []cappb.Capability
		instanceName  string
		updateEnabled bool
	}{
		{
			name:          "image capability enables image instance updates",
			capabilities:  []cappb.Capability{cappb.Capability_IMAGE_CACHE_WRITE},
			instanceName:  interfaces.OCIImageInstanceNamePrefix,
			updateEnabled: true,
		},
		{
			name:          "image capability disables regular instance updates",
			capabilities:  []cappb.Capability{cappb.Capability_IMAGE_CACHE_WRITE},
			instanceName:  "regular-instance",
			updateEnabled: false,
		},
		{
			name:          "cache write enables regular instance updates",
			capabilities:  []cappb.Capability{cappb.Capability_CACHE_WRITE},
			instanceName:  "regular-instance",
			updateEnabled: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			user := &testauth.TestUser{
				UserID:       "US1",
				GroupID:      "GR1",
				Capabilities: test.capabilities,
			}
			env := testenv.GetTestEnv(t)
			env.SetAuthenticator(testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{user.UserID: user}))
			s := capabilities_server.NewCapabilitiesServer(env, true, false, false)
			ctx := testauth.WithAuthenticatedUserInfo(t.Context(), user)
			ctx = bazel_request.OverrideRequestMetadata(ctx, &repb.RequestMetadata{
				ToolDetails: &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.0.0"},
			})

			rsp, err := s.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{InstanceName: test.instanceName})

			require.NoError(t, err)
			assert.Equal(t, test.updateEnabled, rsp.GetCacheCapabilities().GetActionCacheUpdateCapabilities().GetUpdateEnabled())
		})
	}
}
