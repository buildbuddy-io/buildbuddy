// Package cache_proxy_registration runs inside the cache proxy binary and
// maintains a registration heartbeat to the main BuildBuddy app's
// CacheProxyRegistry service.
//
// On startup the cache proxy dials the app target, authenticates with its
// configured API key (which must have the REGISTER_CACHE_PROXY capability),
// and opens a client-streaming RegisterAndStreamHeartbeat RPC. It then
// re-sends its registration on a fixed interval. If the stream or
// connection breaks for any reason — including the app revoking the API key
// — the goroutine retries with a fixed backoff. Cache traffic continues to
// flow regardless of registration state; this package only affects whether
// the proxy shows up on the app's /cache-proxies admin page.
package cache_proxy_registration

import (
	"context"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/hostid"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	cppb "github.com/buildbuddy-io/buildbuddy/proto/cache_proxy"
	dto "github.com/prometheus/client_model/go"
)

var (
	apiKey            = flag.String("cache_proxy.api_key", "", "API Key used to authorize the cache proxy with the BuildBuddy app server.", flag.Secret)
	appTarget         = flag.String("cache_proxy.app_target", "", "Optional BuildBuddy app gRPC target the cache proxy registers itself with for the deployment view (e.g. grpcs://app.buildbuddy.io). Requires --cache_proxy.api_key. If unset, no registration is attempted.")
	metadataDirectory = flag.String("cache_proxy.metadata_directory", "", "Directory where the cache proxy persists its stable host ID. If unset, it will attempt to persist the ID under the OS user config dir; if persistence fails, a new ID will be generated on each restart.")
	labels            = flag.Map[string, string]("cache_proxy.labels", map[string]string{}, "Optional key-value labels identifying this cache proxy, similar to Kubernetes labels (e.g. 'region=us-east1,environment=prod'). Reported at registration and surfaced on the cache proxy admin page.")
)

var (
	// Upper bound on how long we'll wait for the very first Send to land
	// after opening the stream. grpc.Dial is non-blocking, so a black-hole
	// target lets the conn come up "fine" and only the actual Send notices
	// that the handshake never completed. Without this bound the retry
	// loop in run() would never get to run.
	initialSendTimeout = 10 * time.Second

	// How often we re-send registration. Must be well under the server's
	// maxRegistrationStaleness (10m) so a single dropped heartbeat doesn't
	// make us disappear from the deployment view.
	heartbeatInterval = 30 * time.Second

	// How long to wait before retrying after a failed connection or stream.
	retryInterval = 5 * time.Second

	// Upper bound on how long the health checker's shutdown function will
	// block waiting for the goodbye message to flush. We don't want to
	// hold up app shutdown if the stream is wedged.
	shutdownGoodbyeTimeout = time.Second

	// Limits on the labels reported at registration. Registration fails if a
	// key or value is longer than maxLabelLen, or if there are more than
	// maxLabels labels.
	maxLabels   = 20
	maxLabelLen = 50
)

func Register(env *real_environment.RealEnv) error {
	if *appTarget == "" || *apiKey == "" {
		if *appTarget == "" && *apiKey == "" {
			log.Debug("Skipping Cache Proxy registration because both --cache_proxy.app_target and --cache_proxy.api_key are unset")
		} else if *appTarget == "" {
			log.Debug("Skipping Cache Proxy registration because --cache_proxy.app_target is unset")
		} else {
			log.Debug("Skipping Cache Proxy registration because --cache_proxy.api_key is unset")
		}
		return nil
	}

	hostname, err := os.Hostname()
	if err != nil {
		// Hostname is purely cosmetic in the deployment view.
		log.Warningf("Skipping Cache Proxy hostname registration: could not get hostname: %s", err)
		hostname = "unknown"
	}
	proxyID := uuid.NewString()
	if len(*labels) > maxLabels {
		return status.InvalidArgumentErrorf("too many cache proxy labels: %d (max %d)", len(*labels), maxLabels)
	}
	trimmedLabels := make(map[string]string, len(*labels))
	for k, v := range *labels {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(v)
		if len(key) > maxLabelLen {
			return status.InvalidArgumentErrorf("cache proxy label key %q is too long: %d chars (max %d)", key, len(key), maxLabelLen)
		}
		if len(val) > maxLabelLen {
			return status.InvalidArgumentErrorf("cache proxy label value %q is too long: %d chars (max %d)", val, len(val), maxLabelLen)
		}
		trimmedLabels[key] = val
	}
	log.Infof("Registering Cache Proxy %s on host %q", proxyID, hostname)
	node := &cppb.CacheProxyNode{
		Host:                 hostname,
		ProxyId:              proxyID,
		OsFamily:             runtime.GOOS,
		Arch:                 runtime.GOARCH,
		Version:              version.Tag(),
		StartTime:            timestamppb.Now(),
		Labels:               trimmedLabels,
		AllocatedCpuMillis:   resources.GetAllocatedCPUMillis(),
		AllocatedMemoryBytes: resources.GetAllocatedRAMBytes(),
		ConfiguredFlags:      flag.ConfiguredFlags(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	// shutdownCh signals the run goroutine to send a final
	// shutting_down=true message before closing the stream, so the app can
	// drop us from its registry immediately rather than waiting for the
	// staleness TTL. doneCh lets the shutdown function wait for that
	// goodbye to actually flush before we hard-cancel ctx.
	shutdownCh := make(chan struct{})
	doneCh := make(chan struct{})
	env.GetHealthChecker().RegisterShutdownFunction(func(shutdownCtx context.Context) error {
		close(shutdownCh)
		select {
		case <-doneCh:
		case <-shutdownCtx.Done():
		case <-time.After(shutdownGoodbyeTimeout):
		}
		cancel()
		return nil
	})
	go func() {
		defer close(doneCh)
		run(ctx, shutdownCh, env, *appTarget, *apiKey, node)
	}()
	return nil
}

func run(ctx context.Context, shutdownCh <-chan struct{}, env environment.Env, target, apiKey string, node *cppb.CacheProxyNode) {
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey)
	for {
		conn, err := grpc_client.DialInternalWithPoolSize(env, target, 1)
		if err == nil {
			client := cppb.NewCacheProxyRegistryClient(conn)
			if err := streamHeartbeats(ctx, shutdownCh, client, node); err != nil && ctx.Err() == nil {
				log.Warningf("Cache Proxy registration stream failed, will retry in %s: %s", retryInterval, err)
			}
			_ = conn.Close()
		} else if ctx.Err() == nil {
			log.Warningf("Cache Proxy registration: dial %q failed, will retry in %s: %s", target, retryInterval, err)
		}
		if sleepUntilShutdown(ctx, shutdownCh, retryInterval) {
			return
		}
	}
}

// streamHeartbeats opens the registration stream, sends an initial heartbeat
// plus one every heartbeatInterval, and returns when the stream breaks,
// ctx is cancelled, or shutdownCh is closed. On a clean shutdown signal it
// also sends a final shutting_down=true message so the server can drop us
// from its registry immediately instead of waiting for the staleness TTL.
func streamHeartbeats(ctx context.Context, shutdownCh <-chan struct{}, client cppb.CacheProxyRegistryClient, node *cppb.CacheProxyNode) error {
	stream, cleanup, err := openStream(ctx, client, node)
	if err != nil {
		return err
	}
	defer cleanup()
	log.Infof("Successfully registered Cache Proxy %q with the app", node.GetProxyId())

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Best-effort close — the server returns
			// RegisterCacheProxyResponse on EOF, but we don't care about
			// its contents.
			stream.CloseAndRecv()
			return nil
		case <-shutdownCh:
			// Best-effort goodbye. If the send fails the server will
			// eventually drop us via the staleness TTL anyway.
			if err := sendHeartbeat(stream, &cppb.RegisterCacheProxyRequest{Node: node, ShuttingDown: true}); err == nil {
				// Send succeeded; close the stream cleanly. On send
				// failure (io.EOF) sendHeartbeat has already drained
				// the trailer via its own CloseAndRecv, so we skip it
				// here to avoid the redundant call.
				stream.CloseAndRecv()
			}
			return nil
		case <-ticker.C:
			req := &cppb.RegisterCacheProxyRequest{Node: node, Statistics: collectStatistics()}
			if err := sendHeartbeat(stream, req); err != nil {
				return err
			}
		}
	}
}

// collectStatistics reads the proxy's cumulative AC/CAS read and write
// counters out of the in-process Prometheus registry. Reads are partitioned
// by the hit/miss label; writes are reported as totals since there's no
// meaningful hit/miss distinction on the write side.
func collectStatistics() *cppb.Statistics {
	stats := &cppb.Statistics{}
	stats.AcReadHits, stats.AcReadMisses, stats.AcReadUncacheable = sumByStatus(metrics.ActionCacheProxiedReadRequests)
	stats.AcReadHitBytes, stats.AcReadMissBytes, stats.AcReadUncacheableBytes = sumByStatus(metrics.ActionCacheProxiedReadBytes)
	stats.CasReadHits, stats.CasReadMisses, stats.CasReadUncacheable = sumByStatus(metrics.ByteStreamProxiedReadRequests)
	stats.CasReadHitBytes, stats.CasReadMissBytes, stats.CasReadUncacheableBytes = sumByStatus(metrics.ByteStreamProxiedReadBytes)
	stats.AcWrites = sumAll(metrics.ActionCacheProxiedWriteRequests)
	stats.AcWriteBytes = sumAll(metrics.ActionCacheProxiedWriteBytes)
	stats.CasWrites = sumAll(metrics.ByteStreamProxiedWriteRequests)
	stats.CasWriteBytes = sumAll(metrics.ByteStreamProxiedWriteBytes)
	return stats
}

// sumAll collects every series from a CounterVec and totals their values
// without partitioning by any label.
func sumAll(cv *prometheus.CounterVec) int64 {
	ch := make(chan prometheus.Metric, 64)
	go func() {
		defer close(ch)
		cv.Collect(ch)
	}()
	var total float64
	for m := range ch {
		dm := &dto.Metric{}
		if err := m.Write(dm); err != nil {
			continue
		}
		total += dm.GetCounter().GetValue()
	}
	return int64(total)
}

// sumByStatus collects every series from a CounterVec and totals their values
// into (hits, misses, uncacheable) based on the CacheHitMissStatus label.
// Series with any other status value are ignored.
func sumByStatus(cv *prometheus.CounterVec) (hits int64, misses int64, uncacheable int64) {
	ch := make(chan prometheus.Metric, 64)
	go func() {
		defer close(ch)
		cv.Collect(ch)
	}()
	var h, m, u float64
	for metric := range ch {
		dm := &dto.Metric{}
		if err := metric.Write(dm); err != nil {
			continue
		}
		var status string
		for _, lp := range dm.GetLabel() {
			if lp.GetName() == metrics.CacheHitMissStatus {
				status = lp.GetValue()
				break
			}
		}
		v := dm.GetCounter().GetValue()
		switch status {
		case metrics.HitStatusLabel:
			h += v
		case metrics.MissStatusLabel:
			m += v
		case metrics.UncacheableStatusLabel:
			u += v
		}
	}
	return int64(h), int64(m), int64(u)
}

func openStream(ctx context.Context, client cppb.CacheProxyRegistryClient, node *cppb.CacheProxyNode) (cppb.CacheProxyRegistry_RegisterAndStreamHeartbeatClient, func(), error) {
	// The stream is opened on a child ctx. If setup hasn't completed by
	// initialSendTimeout the AfterFunc cancels that child, failing the
	// in-flight call so the caller's retry loop can back off. Once setup
	// succeeds we stop the timer, leaving the child ctx alive for the
	// lifetime of the stream — the caller's deferred cleanup eventually
	// cancels it on exit.
	setupCtx, cancelSetup := context.WithCancel(ctx)
	setupTimer := time.AfterFunc(initialSendTimeout, cancelSetup)
	fail := func(err error) error {
		setupTimer.Stop()
		cancelSetup()
		// Distinguish our own timeout-induced cancellation from a real
		// parent-ctx cancellation, so callers and logs see something
		// meaningful.
		if setupCtx.Err() != nil && ctx.Err() == nil {
			return status.DeadlineExceededErrorf("Cache Proxy registration did not complete within %s", initialSendTimeout)
		}
		return err
	}

	stream, err := client.RegisterAndStreamHeartbeat(setupCtx)
	if err != nil {
		return nil, nil, fail(err)
	}
	if err := sendHeartbeat(stream, &cppb.RegisterCacheProxyRequest{Node: node, Statistics: collectStatistics()}); err != nil {
		return nil, nil, fail(err)
	}
	setupTimer.Stop()
	return stream, cancelSetup, nil
}

// sendHeartbeat writes one heartbeat to the stream. If Send sees the server
// has already terminated (io.EOF), it drains the trailer via CloseAndRecv to
// surface the real status (PermissionDenied, etc.) instead of a bare EOF.
func sendHeartbeat(stream cppb.CacheProxyRegistry_RegisterAndStreamHeartbeatClient, req *cppb.RegisterCacheProxyRequest) error {
	err := stream.Send(req)
	if err == io.EOF {
		if _, recvErr := stream.CloseAndRecv(); recvErr != nil {
			return recvErr
		}
	}
	return err
}

// getProxyHostID returns an ID that identifies the host this cache proxy
// process is running on. If --cache_proxy.metadata_directory is set and
// writable, the ID is persisted there and survives process restarts.
// If it's unset, we attempt to persist the ID under the OS user config dir
// (via hostid.GetHostID("")). If the ID can't be persisted, we fall back to
// a process-wide random UUID, so the proxy may appear as a new host after
// each restart.
func getProxyHostID() string {
	dir := *metadataDirectory
	if dir != "" {
		if err := disk.EnsureDirectoryExists(dir); err == nil {
			if id, err := hostid.GetHostID(dir); err == nil {
				return id
			} else {
				log.Warningf("Cache Proxy: could not read/create stable host ID in %q: %s", dir, err)
			}
		} else {
			log.Warningf("Cache Proxy: metadata directory %q is unusable: %s", dir, err)
		}
	}
	return hostid.GetFailsafeHostID(dir)
}

func sleepUntilShutdown(ctx context.Context, shutdownCh <-chan struct{}, d time.Duration) (cancelled bool) {
	select {
	case <-ctx.Done():
		return true
	case <-shutdownCh:
		return true
	case <-time.After(d):
		return false
	}
}
