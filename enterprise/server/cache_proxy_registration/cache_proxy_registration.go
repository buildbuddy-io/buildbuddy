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
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	cppb "github.com/buildbuddy-io/buildbuddy/proto/cache_proxy"
)

var (
	apiKey    = flag.String("cache_proxy.api_key", "", "API Key used to authorize the cache proxy with the BuildBuddy app server.", flag.Secret)
	appTarget = flag.String("cache_proxy.app_target", "", "Optional BuildBuddy app gRPC target the cache proxy registers itself with for the deployment view (e.g. grpcs://app.buildbuddy.io). Requires --cache_proxy.api_key. If unset, no registration is attempted.")
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
)

func Register(env *real_environment.RealEnv) {
	if *appTarget == "" || *apiKey == "" {
		if *appTarget == "" && *apiKey == "" {
			log.Debug("Skipping Cache Proxy registration because both --cache_proxy.app_target and --cache_proxy.api_key are unset")
		} else if *appTarget == "" {
			log.Debug("Skipping Cache Proxy registration because --cache_proxy.app_target is unset")
		} else {
			log.Debug("Skipping Cache Proxy registration because --cache_proxy.api_key is unset")
		}
		return
	}

	hostname, err := os.Hostname()
	if err != nil {
		// Hostname is purely cosmetic in the deployment view.
		log.Warningf("Skipping Cache Proxy hostname registration: could not get hostname: %s", err)
		hostname = "unknown"
	}
	proxyID := uuid.NewString()
	log.Infof("Registering Cache Proxy %s on host %q", proxyID, hostname)
	node := &cppb.CacheProxyNode{
		Host:      hostname,
		ProxyId:   proxyID,
		OsFamily:  runtime.GOOS,
		Arch:      runtime.GOARCH,
		Version:   version.Tag(),
		StartTime: timestamppb.Now(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	env.GetHealthChecker().RegisterShutdownFunction(func(context.Context) error {
		cancel()
		return nil
	})
	go run(ctx, env, *appTarget, *apiKey, node)
}

func run(ctx context.Context, env environment.Env, target, apiKey string, node *cppb.CacheProxyNode) {
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey)
	for {
		conn, err := grpc_client.DialInternalWithPoolSize(env, target, 1)
		if err == nil {
			client := cppb.NewCacheProxyRegistryClient(conn)
			if err := streamHeartbeats(ctx, client, node); err != nil && ctx.Err() == nil {
				log.Warningf("Cache Proxy registration stream failed, will retry in %s: %s", retryInterval, err)
			}
			_ = conn.Close()
		} else if ctx.Err() == nil {
			log.Warningf("Cache Proxy registration: dial %q failed, will retry in %s: %s", target, retryInterval, err)
		}
		if sleepWithContext(ctx, retryInterval) {
			return
		}
	}
}

// streamHeartbeats opens the registration stream, sends an initial heartbeat
// plus one every heartbeatInterval, and returns when the stream breaks or
// ctx is cancelled.
func streamHeartbeats(ctx context.Context, client cppb.CacheProxyRegistryClient, node *cppb.CacheProxyNode) error {
	stream, cleanup, err := openStream(ctx, client, node)
	if err != nil {
		return err
	}
	defer cleanup()
	log.Infof("Successfully registered Cache Proxy %q with the app", node.GetProxyId())

	req := &cppb.RegisterCacheProxyRequest{Node: node}
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
		case <-ticker.C:
			if err := sendHeartbeat(stream, req); err != nil {
				return err
			}
		}
	}
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
	fail := func(err error) (cppb.CacheProxyRegistry_RegisterAndStreamHeartbeatClient, func(), error) {
		setupTimer.Stop()
		cancelSetup()
		// Distinguish our own timeout-induced cancellation from a real
		// parent-ctx cancellation, so callers and logs see something
		// meaningful.
		if setupCtx.Err() != nil && ctx.Err() == nil {
			return nil, nil, status.DeadlineExceededErrorf("Cache Proxy registration did not complete within %s", initialSendTimeout)
		}
		return nil, nil, err
	}

	stream, err := client.RegisterAndStreamHeartbeat(setupCtx)
	if err != nil {
		return fail(err)
	}
	if err := sendHeartbeat(stream, &cppb.RegisterCacheProxyRequest{Node: node}); err != nil {
		return fail(err)
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

func sleepWithContext(ctx context.Context, d time.Duration) (cancelled bool) {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(d):
		return false
	}
}
