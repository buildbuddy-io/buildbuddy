package profile

import (
	"bytes"
	"context"
	"io"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pfpb "github.com/buildbuddy-io/buildbuddy/proto/profile"
)

var gzipMagic = []byte{0x1f, 0x8b}

func TestCollect(t *testing.T) {
	for _, tc := range []struct {
		name        string
		profileType pfpb.ProfileType
		wantGzip    bool
	}{
		{"cpu", pfpb.ProfileType_CPU, true},
		{"heap", pfpb.ProfileType_HEAP, true},
		{"goroutine", pfpb.ProfileType_GOROUTINE, true},
		{"goroutine_dump", pfpb.ProfileType_GOROUTINE_DUMP, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := Collect(t.Context(), tc.profileType, 100*time.Millisecond)
			require.NoError(t, err)
			require.NotEmpty(t, data)
			assert.Equal(t, tc.wantGzip, bytes.HasPrefix(data, gzipMagic))
		})
	}
}

func TestCollect_GoroutineDumpContainsThisTest(t *testing.T) {
	data, err := Collect(t.Context(), pfpb.ProfileType_GOROUTINE_DUMP, 0)
	require.NoError(t, err)
	assert.Contains(t, string(data), "TestCollect_GoroutineDumpContainsThisTest")
}

func TestCollect_UnknownType(t *testing.T) {
	_, err := Collect(t.Context(), pfpb.ProfileType_UNKNOWN_TYPE, 0)
	assert.True(t, status.IsUnimplementedError(err), "got %v", err)
}

func TestCollect_CPUProfileAlreadyRunning(t *testing.T) {
	require.NoError(t, pprof.StartCPUProfile(io.Discard))
	defer pprof.StopCPUProfile()
	_, err := Collect(t.Context(), pfpb.ProfileType_CPU, time.Millisecond)
	assert.True(t, status.IsFailedPreconditionError(err), "got %v", err)
}

func TestCollect_OneAtATime(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		Collect(ctx, pfpb.ProfileType_CPU, time.Minute)
		close(done)
	}()
	require.Eventually(t, func() bool {
		if !collectMu.TryLock() {
			return true
		}
		collectMu.Unlock()
		return false
	}, 10*time.Second, time.Millisecond)

	_, err := Collect(t.Context(), pfpb.ProfileType_HEAP, 0)
	assert.True(t, status.IsResourceExhaustedError(err), "got %v", err)

	cancel()
	<-done
	_, err = Collect(t.Context(), pfpb.ProfileType_HEAP, 0)
	require.NoError(t, err)
}

func TestCollect_CPUProfileCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := Collect(ctx, pfpb.ProfileType_CPU, time.Minute)
	assert.True(t, status.IsUnavailableError(err), "got %v", err)
}
