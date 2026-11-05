package profile

import (
	"bytes"
	"context"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	pfpb "github.com/buildbuddy-io/buildbuddy/proto/profile"
)

const (
	// The default and maximum CPU profile durations.
	defaultCPUProfileDuration = 30 * time.Second
	maxCPUProfileDuration     = 30 * time.Second

	// Profiles larger than this are rejected so they fit within the default
	// gRPC maximum message size.
	maxProfileSizeBytes = 40_000_000
)

// Guards profile collection to prevent concurrent profile requests.
var collectMu sync.Mutex

// Collect collects a profile of the given type. The duration only applies to
// CPU profiles. Only one profile is collected at a time; concurrent requests
// fail with ResourceExhausted.
func Collect(ctx context.Context, profileType pfpb.GetProfileRequest_Type, duration time.Duration) ([]byte, error) {
	if !collectMu.TryLock() {
		return nil, status.ResourceExhaustedError("another profile is already being collected")
	}
	defer collectMu.Unlock()
	data, err := collect(ctx, profileType, duration)
	if err != nil {
		return nil, err
	}
	if len(data) > maxProfileSizeBytes {
		return nil, status.ResourceExhaustedErrorf("profile is too large: %d bytes (max %d)", len(data), maxProfileSizeBytes)
	}
	return data, nil
}

func collect(ctx context.Context, profileType pfpb.GetProfileRequest_Type, duration time.Duration) ([]byte, error) {
	switch profileType {
	case pfpb.GetProfileRequest_CPU:
		return collectCPUProfile(ctx, duration)
	case pfpb.GetProfileRequest_HEAP:
		return lookup("heap", 0)
	case pfpb.GetProfileRequest_GOROUTINE:
		return lookup("goroutine", 0)
	case pfpb.GetProfileRequest_GOROUTINE_DUMP:
		return lookup("goroutine", 2)
	default:
		return nil, status.UnimplementedErrorf("unsupported profile type %s", profileType)
	}
}

func collectCPUProfile(ctx context.Context, duration time.Duration) ([]byte, error) {
	if duration <= 0 {
		duration = defaultCPUProfileDuration
	}
	duration = min(duration, maxCPUProfileDuration)
	var buf bytes.Buffer
	// Only one CPU profile can run at a time per process, including ones
	// started via the monitoring port's /debug/pprof/profile handler.
	if err := pprof.StartCPUProfile(&buf); err != nil {
		return nil, status.FailedPreconditionErrorf("could not start CPU profile: %s", err)
	}
	select {
	case <-time.After(duration):
		pprof.StopCPUProfile()
		return buf.Bytes(), nil
	case <-ctx.Done():
		pprof.StopCPUProfile()
		return nil, status.UnavailableErrorf("CPU profile cancelled: %s", ctx.Err())
	}
}

func lookup(name string, debug int) ([]byte, error) {
	p := pprof.Lookup(name)
	if p == nil {
		return nil, status.InternalErrorf("profile %q not found", name)
	}
	var buf bytes.Buffer
	if err := p.WriteTo(&buf, debug); err != nil {
		return nil, status.InternalErrorf("could not write %s profile: %s", name, err)
	}
	return buf.Bytes(), nil
}
