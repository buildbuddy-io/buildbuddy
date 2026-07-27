// Package cpusampler attributes app CPU usage to individual gRPC methods and
// customer groups.
//
// Every RPC handler runs with pprof labels identifying its method and
// authenticated group ID (the interceptor runs at the end of the interceptor
// chain, after auth, so pre-auth interceptor CPU stays unattributed); labels
// are inherited by all goroutines spawned (transitively) by the handler. A
// background sampler duty-cycles the runtime CPU profiler (a short window each
// period, at a random offset so windows aren't aligned across instances),
// aggregates the profile's CPU samples by label, and exports the totals as
// Prometheus counters.
//
// Only one CPU profiler can run per process, so a sampling window can conflict
// with an ad-hoc /debug/pprof/profile scrape. Whichever side starts second
// loses: the sampler treats a failed StartCPUProfile as "skip this cycle", and
// with the default 10s-per-60s duty cycle, ad-hoc scrapes usually win.
package cpusampler

import (
	"bytes"
	"context"
	"math/rand/v2"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/pprof/profile"
	"google.golang.org/grpc"
)

var (
	enabled = flag.Bool("cpu_sampler.enabled", false, "If true, periodically run CPU profiling windows and export CPU usage per gRPC method as metrics.")
	period  = flag.Duration("cpu_sampler.period", 60*time.Second, "How often to run a CPU profiling window.")
	window  = flag.Duration("cpu_sampler.window", 10*time.Second, "How long each CPU profiling window lasts. Must be shorter than cpu_sampler.period.")
)

const (
	// RPCMethodLabelKey is the pprof label key holding the full gRPC method
	// name (e.g. "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs").
	RPCMethodLabelKey = "rpc_method"

	// GroupIDLabelKey is the pprof label key holding the authenticated group
	// ID on whose behalf the RPC is running.
	GroupIDLabelKey = "group_id"

	// UnattributedLabelValue is the metric label value used for CPU samples
	// that don't carry the corresponding pprof label (background work, GC,
	// pre-label interceptor work, RPCs from servers not using the
	// interceptor, etc.).
	UnattributedLabelValue = "[unattributed]"
)

// Start launches the background sampling loop if enabled. The loop stops when
// the server shuts down.
func Start(env environment.Env) {
	if !*enabled {
		return
	}
	if *window <= 0 || *period <= *window {
		log.Errorf("Invalid cpu_sampler configuration: window (%s) must be positive and shorter than period (%s); not sampling.", *window, *period)
		return
	}
	ctx, cancel := context.WithCancel(env.GetServerContext())
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		cancel()
		return nil
	})
	go run(ctx)
}

func run(ctx context.Context) {
	for {
		// Profile at a random offset within each period so that windows
		// aren't aligned across app instances and cover all phase offsets of
		// periodic work over time.
		offset := rand.N(*period - *window)
		if !sleepCtx(ctx, offset) {
			return
		}
		cpuNanos, profiledDur, err := profileWindow(ctx, *window)
		if err != nil {
			// Most likely the profiler is already in use (e.g. an ad-hoc
			// /debug/pprof/profile scrape); skip this cycle.
			log.Debugf("CPU sampler: skipping cycle: %s", err)
		} else {
			metrics.CPUSamplerProfiledWallTimeSeconds.Add(profiledDur.Seconds())
			for k, nanos := range cpuNanos {
				service, method := splitFullMethodName(k.method)
				metrics.CPUSamplerSampledCPUNanos.WithLabelValues(service, method).Add(float64(nanos))
				metrics.CPUSamplerSampledCPUNanosPerGroup.WithLabelValues(k.groupID).Add(float64(nanos))
			}
		}
		if !sleepCtx(ctx, *period-offset-*window) {
			return
		}
	}
}

// profileWindow runs the CPU profiler for the given duration and returns
// on-CPU nanoseconds grouped by RPC method and group ID labels, along with the
// actual profiled wall time.
func profileWindow(ctx context.Context, dur time.Duration) (map[sampleKey]int64, time.Duration, error) {
	buf := &bytes.Buffer{}
	if err := pprof.StartCPUProfile(buf); err != nil {
		return nil, 0, err
	}
	start := time.Now()
	sleepCtx(ctx, dur)
	pprof.StopCPUProfile()
	profiledDur := time.Since(start)

	p, err := profile.ParseData(buf.Bytes())
	if err != nil {
		return nil, profiledDur, status.InternalErrorf("parse CPU profile: %s", err)
	}
	byKey, err := cpuNanosByRPCLabels(p)
	if err != nil {
		return nil, profiledDur, err
	}
	return byKey, profiledDur, nil
}

// sampleKey identifies the RPC method and group on whose behalf a CPU sample
// was taken.
type sampleKey struct {
	method  string
	groupID string
}

// cpuNanosByRPCLabels sums the CPU-nanoseconds sample values in the profile,
// grouped by RPC method and group ID pprof labels. Samples missing a label
// are summed under UnattributedLabelValue for that label.
func cpuNanosByRPCLabels(p *profile.Profile) (map[sampleKey]int64, error) {
	cpuIndex := -1
	for i, st := range p.SampleType {
		if st.Type == "cpu" && st.Unit == "nanoseconds" {
			cpuIndex = i
		}
	}
	if cpuIndex == -1 {
		return nil, status.InternalError("no cpu/nanoseconds sample type in CPU profile")
	}
	out := make(map[sampleKey]int64)
	for _, s := range p.Sample {
		v := s.Value[cpuIndex]
		if v == 0 {
			continue
		}
		out[sampleKey{
			method:  labelValue(s, RPCMethodLabelKey),
			groupID: labelValue(s, GroupIDLabelKey),
		}] += v
	}
	return out, nil
}

// splitFullMethodName splits a full gRPC method name ("/pkg.Service/Method")
// into service ("pkg.Service") and bare method ("Method") parts, matching the
// grpc_service/grpc_method label convention used by the grpc_server_* metrics.
// Values with no "/" separator (e.g. UnattributedLabelValue) are returned as
// both parts.
func splitFullMethodName(fullMethod string) (service, method string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/")
	if before, after, ok := strings.Cut(fullMethod, "/"); ok {
		return before, after
	}
	return fullMethod, fullMethod
}

func labelValue(s *profile.Sample, key string) string {
	if vals := s.Label[key]; len(vals) > 0 {
		return vals[0]
	}
	return UnattributedLabelValue
}

// sleepCtx sleeps for the given duration, returning early (false) if the
// context is canceled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// rpcLabels returns the pprof labels to apply to an RPC handler goroutine.
// It must be called after the auth interceptor has run so that claims are
// available on the context.
func rpcLabels(ctx context.Context, fullMethod string) pprof.LabelSet {
	groupID := interfaces.AuthAnonymousUser
	if c, err := claims.ClaimsFromContext(ctx); err == nil && c.GetGroupID() != "" {
		groupID = c.GetGroupID()
	}
	return pprof.Labels(RPCMethodLabelKey, fullMethod, GroupIDLabelKey, groupID)
}

// UnaryServerInterceptor labels the handler goroutine (and any goroutines it
// spawns, transitively) with the RPC method and authenticated group ID so
// that CPU profile samples can be attributed to them. It must be installed
// after the auth interceptor.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !*enabled {
			return handler(ctx, req)
		}
		var rsp any
		var err error
		pprof.Do(ctx, rpcLabels(ctx, info.FullMethod), func(ctx context.Context) {
			rsp, err = handler(ctx, req)
		})
		return rsp, err
	}
}

type labeledStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *labeledStream) Context() context.Context { return s.ctx }

// StreamServerInterceptor is the streaming equivalent of
// UnaryServerInterceptor.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !*enabled {
			return handler(srv, stream)
		}
		var err error
		pprof.Do(stream.Context(), rpcLabels(stream.Context(), info.FullMethod), func(ctx context.Context) {
			err = handler(srv, &labeledStream{stream, ctx})
		})
		return err
	}
}
