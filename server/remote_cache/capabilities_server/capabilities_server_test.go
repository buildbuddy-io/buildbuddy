package capabilities_server

import (
	"context"
	"testing"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
)

type testFlagProvider struct{}

func (testFlagProvider) Boolean(ctx context.Context, flagName string, defaultValue bool, opts ...any) bool {
	if flagName == "cache.chunking_enabled" {
		return true
	}
	return defaultValue
}
func (testFlagProvider) String(ctx context.Context, flagName string, defaultValue string, opts ...any) string {
	return defaultValue
}
func (testFlagProvider) Float64(ctx context.Context, flagName string, defaultValue float64, opts ...any) float64 {
	return defaultValue
}
func (testFlagProvider) Int64(ctx context.Context, flagName string, defaultValue int64, opts ...any) int64 {
	if flagName == "cache.chunking_max_write_size_bytes" {
		return 123456789
	}
	return defaultValue
}
func (testFlagProvider) Object(ctx context.Context, flagName string, defaultValue map[string]any, opts ...any) map[string]any {
	return defaultValue
}
func (testFlagProvider) BooleanDetails(ctx context.Context, flagName string, defaultValue bool, opts ...any) (bool, interfaces.ExperimentFlagDetails) {
	return testFlagProvider{}.Boolean(ctx, flagName, defaultValue, opts...), nil
}
func (testFlagProvider) StringDetails(ctx context.Context, flagName string, defaultValue string, opts ...any) (string, interfaces.ExperimentFlagDetails) {
	return defaultValue, nil
}
func (testFlagProvider) Float64Details(ctx context.Context, flagName string, defaultValue float64, opts ...any) (float64, interfaces.ExperimentFlagDetails) {
	return defaultValue, nil
}
func (testFlagProvider) Int64Details(ctx context.Context, flagName string, defaultValue int64, opts ...any) (int64, interfaces.ExperimentFlagDetails) {
	return testFlagProvider{}.Int64(ctx, flagName, defaultValue, opts...), nil
}
func (testFlagProvider) ObjectDetails(ctx context.Context, flagName string, defaultValue map[string]any, opts ...any) (map[string]any, interfaces.ExperimentFlagDetails) {
	return defaultValue, nil
}
func (testFlagProvider) Subscribe(ch chan<- struct{}) func() { return func() {} }

func TestGetCapabilitiesIncludesFastCDCMaxWriteSize(t *testing.T) {
	env := testenv.GetTestEnv(t)
	env.SetExperimentFlagProvider(testFlagProvider{})

	s := NewCapabilitiesServer(env, true, false, true)
	rsp, err := s.GetCapabilities(context.Background(), &repb.GetCapabilitiesRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(123456789), rsp.GetCacheCapabilities().GetFastCdc_2020Params().GetBuildbuddyMaxChunkedWriteSizeBytes())
}
