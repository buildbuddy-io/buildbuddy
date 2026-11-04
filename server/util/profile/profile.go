package profile

import (
	"bytes"
	"context"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	pfpb "github.com/buildbuddy-io/buildbuddy/proto/profile"
	gstatus "google.golang.org/grpc/status"
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

// Collect collects the requested profile. Errors are reported in the
// returned profile's status rather than returned. Only one profile is
// collected at a time; concurrent requests fail with ResourceExhausted.
func Collect(ctx context.Context, req *pfpb.GetProfileRequest) *pfpb.Profile {
	if !collectMu.TryLock() {
		err := status.ResourceExhaustedError("another profile is already being collected")
		return &pfpb.Profile{Type: req.GetType(), Status: gstatus.Convert(err).Proto()}
	}
	defer collectMu.Unlock()
	data, err := collect(ctx, req)
	if err == nil && len(data) > maxProfileSizeBytes {
		err = status.ResourceExhaustedErrorf("profile is too large: %d bytes (max %d)", len(data), maxProfileSizeBytes)
	}
	if err != nil {
		return &pfpb.Profile{Type: req.GetType(), Status: gstatus.Convert(err).Proto()}
	}
	return &pfpb.Profile{Type: req.GetType(), Data: data}
}

func collect(ctx context.Context, req *pfpb.GetProfileRequest) ([]byte, error) {
	switch req.GetType() {
	case pfpb.GetProfileRequest_CPU:
		return collectCPUProfile(ctx, req.GetDuration().AsDuration())
	case pfpb.GetProfileRequest_HEAP:
		return lookup("heap", 0)
	case pfpb.GetProfileRequest_GOROUTINE:
		return lookup("goroutine", 0)
	case pfpb.GetProfileRequest_GOROUTINE_DUMP:
		return lookup("goroutine", 2)
	default:
		return nil, status.UnimplementedErrorf("unsupported profile type %s", req.GetType())
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
