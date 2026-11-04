package profile

import (
	"bytes"
	"context"
	"io"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"

	pfpb "github.com/buildbuddy-io/buildbuddy/proto/profile"
)

var gzipMagic = []byte{0x1f, 0x8b}

func TestCollect(t *testing.T) {
	for _, tc := range []struct {
		name     string
		req      *pfpb.GetProfileRequest
		wantGzip bool
	}{
		{"cpu", &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_CPU, Duration: durationpb.New(100 * time.Millisecond)}, true},
		{"heap", &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_HEAP}, true},
		{"goroutine", &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_GOROUTINE}, true},
		{"goroutine_dump", &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_GOROUTINE_DUMP}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := Collect(t.Context(), tc.req)
			require.Zero(t, p.GetStatus().GetCode(), p.GetStatus().GetMessage())
			assert.Equal(t, tc.req.GetType(), p.GetType())
			require.NotEmpty(t, p.GetData())
			assert.Equal(t, tc.wantGzip, bytes.HasPrefix(p.GetData(), gzipMagic))
		})
	}
}

func TestCollect_GoroutineDumpContainsThisTest(t *testing.T) {
	p := Collect(t.Context(), &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_GOROUTINE_DUMP})
	assert.Contains(t, string(p.GetData()), "TestCollect_GoroutineDumpContainsThisTest")
}

func TestCollect_UnknownType(t *testing.T) {
	p := Collect(t.Context(), &pfpb.GetProfileRequest{})
	assert.Equal(t, int32(codes.Unimplemented), p.GetStatus().GetCode())
	assert.Empty(t, p.GetData())
}

func TestCollect_CPUProfileAlreadyRunning(t *testing.T) {
	require.NoError(t, pprof.StartCPUProfile(io.Discard))
	defer pprof.StopCPUProfile()
	p := Collect(t.Context(), &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_CPU, Duration: durationpb.New(time.Millisecond)})
	assert.Equal(t, int32(codes.FailedPrecondition), p.GetStatus().GetCode())
}

func TestCollect_OneAtATime(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan *pfpb.Profile)
	go func() {
		done <- Collect(ctx, &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_CPU, Duration: durationpb.New(time.Minute)})
	}()
	require.Eventually(t, func() bool {
		if !collectMu.TryLock() {
			return true
		}
		collectMu.Unlock()
		return false
	}, 10*time.Second, time.Millisecond)

	p := Collect(t.Context(), &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_HEAP})
	assert.Equal(t, int32(codes.ResourceExhausted), p.GetStatus().GetCode())
	assert.Empty(t, p.GetData())

	cancel()
	<-done
	p = Collect(t.Context(), &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_HEAP})
	require.Zero(t, p.GetStatus().GetCode(), p.GetStatus().GetMessage())
}

func TestCollect_CPUProfileCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	p := Collect(ctx, &pfpb.GetProfileRequest{Type: pfpb.GetProfileRequest_CPU, Duration: durationpb.New(time.Minute)})
	assert.Equal(t, int32(codes.Unavailable), p.GetStatus().GetCode())
	assert.Empty(t, p.GetData())
}
