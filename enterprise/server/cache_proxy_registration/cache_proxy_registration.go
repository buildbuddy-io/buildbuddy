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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/cache_proxy_auth"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	cppb "github.com/buildbuddy-io/buildbuddy/proto/cache_proxy"
)

var (
	appTarget = flag.String("cache_proxy.app_target", "", "Optional BuildBuddy app gRPC target the cache proxy registers itself with for the deployment view (e.g. grpcs://app.buildbuddy.io). Requires --cache_proxy.api_key. If unset, no registration is attempted.")
)

var (
	// How often we re-send registration. Must be well under the server's
	// maxRegistrationStaleness (10m) so a single dropped heartbeat doesn't
	// make us disappear from the deployment view.
	heartbeatInterval = 30 * time.Second

	// How long to wait before retrying after a failed connection or stream.
	retryInterval = 5 * time.Second
)

func Register(env *real_environment.RealEnv) {
	target := *appTarget
	apiKey := cache_proxy_auth.APIKey()
	if target == "" || apiKey == "" {
		if target == "" && apiKey == "" {
			log.Debug("Skipping Cache Proxy registration because both --cache_proxy.app_target and --cache_proxy.api_key are unset")
		} else if target == "" {
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
	go run(ctx, env, target, apiKey, node)
}

func run(ctx context.Context, env environment.Env, target, apiKey string, node *cppb.CacheProxyNode) {
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey)
	for {
		conn, err := grpc_client.DialInternal(env, target)
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
	stream, err := client.RegisterAndStreamHeartbeat(ctx)
	if err != nil {
		return err
	}
	req := &cppb.RegisterCacheProxyRequest{Node: node}
	if err := sendHeartbeat(stream, req); err != nil {
		return err
	}
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
		case <-ticker.C:
			if err := sendHeartbeat(stream, req); err != nil {
				return err
			}
		}
	}
}

// sendHeartbeat writes one heartbeat to the stream. If Send sees the server
// has already terminated (io.EOF), it drains the trailer via CloseAndRecv to
// surface the real status (PermissionDenied, etc.) instead of a bare EOF.
func sendHeartbeat(stream cppb.CacheProxyRegistry_RegisterAndStreamHeartbeatClient, req *cppb.RegisterCacheProxyRequest) error {
	if err := stream.Send(req); err != nil {
		if err == io.EOF {
			if _, recvErr := stream.CloseAndRecv(); recvErr != nil {
				return recvErr
			}
		}
		return err
	}
	return nil
}

func sleepWithContext(ctx context.Context, d time.Duration) (cancelled bool) {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(d):
		return false
	}
}
