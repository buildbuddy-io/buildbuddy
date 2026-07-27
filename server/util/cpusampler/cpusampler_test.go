package cpusampler

import (
	"context"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestCPUNanosByRPCLabels(t *testing.T) {
	p := &profile.Profile{
		SampleType: []*profile.ValueType{
			{Type: "samples", Unit: "count"},
			{Type: "cpu", Unit: "nanoseconds"},
		},
		Sample: []*profile.Sample{
			{Value: []int64{1, 100}, Label: map[string][]string{RPCMethodLabelKey: {"/foo.Service/Bar"}, GroupIDLabelKey: {"GR1"}}},
			{Value: []int64{2, 250}, Label: map[string][]string{RPCMethodLabelKey: {"/foo.Service/Bar"}, GroupIDLabelKey: {"GR1"}}},
			{Value: []int64{1, 30}, Label: map[string][]string{RPCMethodLabelKey: {"/foo.Service/Bar"}, GroupIDLabelKey: {"GR2"}}},
			{Value: []int64{1, 40}, Label: map[string][]string{RPCMethodLabelKey: {"/foo.Service/Baz"}}},
			{Value: []int64{1, 50}, Label: map[string][]string{"unrelated_label": {"x"}}},
			{Value: []int64{1, 25}},
			{Value: []int64{1, 0}},
		},
	}
	got, err := cpuNanosByRPCLabels(p)
	require.NoError(t, err)
	require.Equal(t, map[sampleKey]int64{
		{method: "/foo.Service/Bar", groupID: "GR1"}:                      350,
		{method: "/foo.Service/Bar", groupID: "GR2"}:                      30,
		{method: "/foo.Service/Baz", groupID: UnattributedLabelValue}:     40,
		{method: UnattributedLabelValue, groupID: UnattributedLabelValue}: 75,
	}, got)
}

func TestSplitFullMethodName(t *testing.T) {
	for _, tc := range []struct {
		full, service, method string
	}{
		{"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs", "build.bazel.remote.execution.v2.ContentAddressableStorage", "BatchUpdateBlobs"},
		{"/foo.Service/Bar", "foo.Service", "Bar"},
		{UnattributedLabelValue, UnattributedLabelValue, UnattributedLabelValue},
	} {
		service, method := splitFullMethodName(tc.full)
		require.Equal(t, tc.service, service, "service for %q", tc.full)
		require.Equal(t, tc.method, method, "method for %q", tc.full)
	}
}

func TestCPUNanosByRPCLabelsMissingCPUSampleType(t *testing.T) {
	p := &profile.Profile{
		SampleType: []*profile.ValueType{{Type: "samples", Unit: "count"}},
	}
	_, err := cpuNanosByRPCLabels(p)
	require.Error(t, err)
}

func TestProfileWindowAttributesCPUToLabeledGoroutines(t *testing.T) {
	const method = "/test.Service/Burn"
	const groupID = "GR123"
	stop := make(chan struct{})
	done := make(chan struct{})
	go pprof.Do(context.Background(), pprof.Labels(RPCMethodLabelKey, method, GroupIDLabelKey, groupID), func(ctx context.Context) {
		defer close(done)
		x := 0
		for {
			select {
			case <-stop:
				return
			default:
				x++
			}
		}
	})
	defer func() {
		close(stop)
		<-done
	}()

	byKey, profiledDur, err := profileWindow(context.Background(), 500*time.Millisecond)
	require.NoError(t, err)
	require.GreaterOrEqual(t, profiledDur, 500*time.Millisecond)
	key := sampleKey{method: method, groupID: groupID}
	require.Greater(t, byKey[key], int64(0), "expected CPU samples attributed to %+v, got: %v", key, byKey)
}

func TestUnaryServerInterceptorSetsLabels(t *testing.T) {
	flags.Set(t, "cpu_sampler.enabled", true)
	interceptor := UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/foo.Service/Bar"}

	ctx := claims.AuthContext(context.Background(), &claims.Claims{GroupID: "GR123"})
	var gotMethod, gotGroup string
	_, err := interceptor(ctx, nil, info, func(ctx context.Context, req any) (any, error) {
		gotMethod, _ = pprof.Label(ctx, RPCMethodLabelKey)
		gotGroup, _ = pprof.Label(ctx, GroupIDLabelKey)
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, "/foo.Service/Bar", gotMethod)
	require.Equal(t, "GR123", gotGroup)
}

func TestUnaryServerInterceptorAnonymous(t *testing.T) {
	flags.Set(t, "cpu_sampler.enabled", true)
	interceptor := UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/foo.Service/Bar"}

	var gotGroup string
	_, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		gotGroup, _ = pprof.Label(ctx, GroupIDLabelKey)
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, interfaces.AuthAnonymousUser, gotGroup)
}

func TestUnaryServerInterceptorDisabled(t *testing.T) {
	interceptor := UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/foo.Service/Bar"}
	var labelSet bool
	_, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
		_, labelSet = pprof.Label(ctx, RPCMethodLabelKey)
		return nil, nil
	})
	require.NoError(t, err)
	require.False(t, labelSet)
}
