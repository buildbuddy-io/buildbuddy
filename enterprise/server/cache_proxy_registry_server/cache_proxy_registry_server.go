// This package implements a gRPC server that tracks the set of live cache
// proxies connected to the app, scoped by group.
//
// Cache proxies authenticate using an API key (which must have the
// REGISTER_CACHE_PROXY capability) and open a client-streaming RPC
// (RegisterAndStreamHeartbeat). The server persists each heartbeat as a
// RegisteredCacheProxy entry in a per-group Redis hash, along with an ACL
// scoped to that group. While the stream is open, credentials are
// periodically re-validated so a revoked or downgraded API key terminates
// the stream rather than continuing to write registrations.
//
// The companion GetCacheProxies RPC reads that hash, drops entries that
// haven't checked in for a while, applies ACL filtering, and returns the
// survivors.
package cache_proxy_registry_server

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/upgrade"
	"github.com/go-redis/redis/v8"
	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/types/known/timestamppb"

	cppb "github.com/buildbuddy-io/buildbuddy/proto/cache_proxy"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	uppb "github.com/buildbuddy-io/buildbuddy/proto/upgrade"
)

const (
	// A cache proxy is removed from the registry if it does not refresh its
	// registration within this amount of time.
	maxRegistrationStaleness = 10 * time.Minute

	// How often we revalidate credentials for an open registration stream. We
	// need to regularly revlidate credentials to handle revoked API keys.
	checkRegistrationCredentialsInterval = 5 * time.Minute

	// How long a computed newest-registered-version value is served from the
	// in-process cache before the Redis registry is scanned again.
	newestVersionCacheTTL = 5 * time.Minute

	// Message attached to upgrade prompts returned by GetCacheProxies.
	upgradePromptMessage = "One or more of your cache proxies are running an outdated version. The newest available version is %s."
)

var (
	upgradePromptMaxLags     = flag.Map("cache_proxy.upgrade_prompt_max_lags", map[string]string{}, "Map from upgrade prompt urgency (LOW, MEDIUM, HIGH, or CRITICAL) to the maximum version lag (a semver-shaped diff, e.g. \"0.10.0\" tolerates at most 10 minor versions) a cache proxy may fall behind the newest registered version before GetCacheProxies prompts an upgrade at that urgency.")
	upgradePromptMinVersions = flag.Map("cache_proxy.upgrade_prompt_min_versions", map[string]string{}, "Map from upgrade prompt urgency (LOW, MEDIUM, HIGH, or CRITICAL) to the minimum version (semver) below which GetCacheProxies prompts an upgrade at that urgency.")
	sharedPoolGroupID        = flag.String("cache_proxy.shared_pool_group_id", "", "Group ID that owns the shared proxies.")
)

type CacheProxyRegistryServer struct {
	authenticator interfaces.Authenticator
	clock         clockwork.Clock
	rdb           redis.UniversalClient
	quit          chan struct{}
	detector      *upgrade.Detector

	mu                  sync.Mutex
	newestVersion       *semver.Version
	newestVersionExpiry time.Time
}

func Register(env *real_environment.RealEnv) error {
	if env.GetDefaultRedisClient() == nil {
		return nil
	}
	triggers, err := upgrade.ParseTriggers(*upgradePromptMaxLags, *upgradePromptMinVersions)
	if err != nil {
		return status.InvalidArgumentErrorf("Invalid cache proxy upgrade prompt configuration: %s", err)
	}
	s, err := NewCacheProxyRegistryServer(env, upgrade.NewDetector(triggers))
	if err != nil {
		return status.InternalErrorf("Error configuring cache proxy registry server: %v", err)
	}
	env.SetCacheProxyRegistryService(s)
	return nil
}

func upgradeTriggersFromFlags() (map[uppb.Prompt_Urgency]upgrade.Trigger, error) {
	return upgrade.ParseTriggers(*upgradePromptMaxLags, *upgradePromptMinVersions)
}

func NewCacheProxyRegistryServer(env environment.Env, detector *upgrade.Detector) (*CacheProxyRegistryServer, error) {
	rdb := env.GetDefaultRedisClient()
	if rdb == nil {
		return nil, status.FailedPreconditionError("Redis is required for cache proxy registration")
	}
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, status.FailedPreconditionError("Authenticator is required for cache proxy registration")
	}
	quit := make(chan struct{})
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		close(quit)
		return nil
	})
	return &CacheProxyRegistryServer{
		authenticator: authenticator,
		clock:         env.GetClock(),
		rdb:           rdb,
		quit:          quit,
		detector:      detector,
	}, nil
}

func redisKeyForCacheProxies(groupID string) string {
	return "cacheProxies/" + groupID
}

func (s *CacheProxyRegistryServer) authorize(ctx context.Context) (string, error) {
	// AuthenticateGRPCRequest re-reads credentials so we catch API key
	// deletions / capability changes while the stream is open.
	user, err := s.authenticator.AuthenticateGRPCRequest(ctx)
	if err != nil {
		return "", err
	}
	if !user.HasCapability(cappb.Capability_REGISTER_CACHE_PROXY) {
		return "", status.PermissionDeniedError("API key is missing REGISTER_CACHE_PROXY capability")
	}
	return user.GetGroupID(), nil
}

func (s *CacheProxyRegistryServer) RegisterAndStreamHeartbeat(stream cppb.CacheProxyRegistry_RegisterAndStreamHeartbeatServer) error {
	ctx := stream.Context()
	groupID, err := s.authorize(ctx)
	if err != nil {
		log.CtxInfof(ctx, "Rejecting cache proxy registration stream: %s", err)
		return err
	}

	requestChan := make(chan *cppb.RegisterCacheProxyRequest, 1)
	errChan := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				close(requestChan)
				return
			}
			if err != nil {
				// Bail out instead of blocking on errChan if the main
				// loop has already returned (which cancels ctx).
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}
			// Likewise, don't block forever on a buffered requestChan
			// that the main loop will never drain.
			select {
			case requestChan <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	checkCredentialsTicker := s.clock.NewTicker(checkRegistrationCredentialsInterval)
	defer checkCredentialsTicker.Stop()

	var proxyID string
	for {
		select {
		case <-s.quit:
			log.CtxInfof(ctx, "Closing cache proxy registration stream for proxy %q (group %q): server is shutting down", proxyID, groupID)
			return status.CanceledError("server is shutting down")
		case err := <-errChan:
			log.CtxWarningf(ctx, "Closing cache proxy registration stream for proxy %q (group %q): receive failed: %s", proxyID, groupID, err)
			return err
		case req, ok := <-requestChan:
			if !ok {
				log.CtxInfof(ctx, "Cache proxy %q (group %q) closed its registration stream", proxyID, groupID)
				return stream.SendAndClose(&cppb.RegisterCacheProxyResponse{})
			}
			node := req.GetNode()
			if node == nil {
				log.CtxInfof(ctx, "Rejecting cache proxy heartbeat from group %q: missing node info", groupID)
				return status.InvalidArgumentError("registration request missing node info")
			}
			if node.GetProxyId() == "" {
				log.CtxInfof(ctx, "Rejecting cache proxy heartbeat from group %q: missing proxy_id", groupID)
				return status.InvalidArgumentError("registration request missing proxy_id")
			}
			if req.GetShuttingDown() {
				log.CtxInfof(ctx, "Cache proxy %q (group %q) signalled shutdown; removing from registry", node.GetProxyId(), groupID)
				if err := s.removeProxy(ctx, groupID, node.GetProxyId()); err != nil {
					log.CtxWarningf(ctx, "Could not remove shutting-down cache proxy %q (group %q): %s", node.GetProxyId(), groupID, err)
					return err
				}
				return stream.SendAndClose(&cppb.RegisterCacheProxyResponse{})
			}
			if err := s.insertOrUpdateProxy(ctx, groupID, node, req.GetStatistics()); err != nil {
				log.CtxInfof(ctx, "Closing cache proxy registration stream for proxy %q (group %q): could not store registration: %s", node.GetProxyId(), groupID, err)
				return err
			}
			proxyID = node.GetProxyId()
			log.CtxDebugf(ctx, "Cache proxy %q (host ID %q, host %q) checked in", proxyID, node.GetProxyHostId(), node.GetHost())
		case <-checkCredentialsTicker.Chan():
			if _, err := s.authorize(ctx); err != nil {
				if status.IsPermissionDeniedError(err) || status.IsUnauthenticatedError(err) {
					log.CtxInfof(ctx, "Closing cache proxy registration stream for proxy %q (group %q): credentials revoked: %s", proxyID, groupID, err)
					return err
				}
				log.CtxWarningf(ctx, "could not revalidate cache proxy registration: %s", err)
			}
		}
	}
}

func (s *CacheProxyRegistryServer) insertOrUpdateProxy(ctx context.Context, groupID string, node *cppb.CacheProxyNode, stats *cppb.Statistics) error {
	acl := perms.ToACLProto(nil /*=userID*/, groupID, perms.GROUP_WRITE|perms.GROUP_READ)

	r := &cppb.RegisteredCacheProxy{
		Registration: node,
		GroupId:      groupID,
		Acl:          acl,
		LastPingTime: timestamppb.Now(),
		Statistics:   stats,
	}
	b, err := proto.Marshal(r)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, redisKeyForCacheProxies(groupID), node.GetProxyId(), b).Err()
}

func (s *CacheProxyRegistryServer) removeProxy(ctx context.Context, groupID, proxyID string) error {
	return s.rdb.HDel(ctx, redisKeyForCacheProxies(groupID), proxyID).Err()
}

// getNewestVersion returns the maximum semantic version among live
// registrations in the shared pool group.
func (s *CacheProxyRegistryServer) getNewestVersion(ctx context.Context) *semver.Version {
	if *sharedPoolGroupID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clock.Now().Before(s.newestVersionExpiry) {
		return s.newestVersion
	}

	entries, err := s.rdb.HGetAll(ctx, redisKeyForCacheProxies(*sharedPoolGroupID)).Result()
	if err != nil {
		log.CtxWarningf(ctx, "could not read cache proxy registrations for newest version: %s", err)
		// Don't cache the result of a failed read.
		return nil
	}
	var newest *semver.Version
	for _, data := range entries {
		reg := &cppb.RegisteredCacheProxy{}
		if err := proto.Unmarshal([]byte(data), reg); err != nil {
			continue
		}
		if s.clock.Since(reg.GetLastPingTime().AsTime()) > maxRegistrationStaleness {
			continue
		}
		// Skip "unknown" and other unparseable versions.
		v, err := semver.NewVersion(reg.GetRegistration().GetVersion())
		if err != nil {
			continue
		}
		if newest == nil || v.GreaterThan(newest) {
			newest = v
		}
	}

	s.newestVersion = newest
	s.newestVersionExpiry = s.clock.Now().Add(newestVersionCacheTTL)
	return newest
}

// upgradePrompt returns a Prompt carrying the newest registered proxy version
// if any of the given proxies meets one of the configured upgrade triggers
// (see the --cache_proxy.upgrade_prompt_* flags), and nil otherwise. The
// urgency reflects the most-outdated proxy in the list.
func (s *CacheProxyRegistryServer) upgradePrompt(ctx context.Context, proxies []*cppb.GetCacheProxiesResponse_CacheProxy) *uppb.Prompt {
	if s.detector == nil || len(proxies) == 0 {
		return nil
	}
	newestVersion := s.getNewestVersion(ctx)
	newestVersionString := "unknown"
	if newestVersion != nil {
		newestVersionString = newestVersion.String()
	}
	versions := make([]string, 0, len(proxies))
	for _, p := range proxies {
		versions = append(versions, p.GetNode().GetVersion())
	}
	return s.detector.Detect(newestVersion, versions, fmt.Sprintf(upgradePromptMessage, newestVersionString))
}

func (s *CacheProxyRegistryServer) GetCacheProxies(ctx context.Context, req *cppb.GetCacheProxiesRequest) (*cppb.GetCacheProxiesResponse, error) {
	// The group ID comes from the request context (the UI's "selected
	// group") rather than the authenticated user, because a user can
	// belong to several groups. perms.AuthorizeRead below still verifies
	// that the caller actually has read access to entries owned by this
	// group, so passing an arbitrary group ID here just yields an empty
	// response, not a leak.
	groupID := req.GetRequestContext().GetGroupId()
	if groupID == "" {
		return nil, status.InvalidArgumentError("group not specified")
	}

	user, err := s.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	redisKey := redisKeyForCacheProxies(groupID)
	entries, err := s.rdb.HGetAll(ctx, redisKey).Result()
	if err != nil {
		return nil, err
	}

	proxies := make([]*cppb.GetCacheProxiesResponse_CacheProxy, 0, len(entries))
	for id, data := range entries {
		reg := &cppb.RegisteredCacheProxy{}
		if err := proto.Unmarshal([]byte(data), reg); err != nil {
			return nil, err
		}
		if time.Since(reg.GetLastPingTime().AsTime()) > maxRegistrationStaleness {
			// Racey: a fresh heartbeat could land in this hash field
			// between our HGetAll above and the HDel below, and we'd
			// drop a live registration. That's tolerable here because
			// the next heartbeat from that proxy will reinsert it.
			log.CtxInfof(ctx, "Removing stale cache proxy %q (group %q)", id, groupID)
			if err := s.rdb.HDel(ctx, redisKey, id).Err(); err != nil {
				log.CtxWarningf(ctx, "could not remove stale cache proxy: %s", err)
			}
			continue
		}
		if err := perms.AuthorizeRead(user, reg.GetAcl()); err != nil {
			continue
		}
		proxies = append(proxies, &cppb.GetCacheProxiesResponse_CacheProxy{
			Node:            reg.GetRegistration(),
			LastCheckInTime: reg.GetLastPingTime(),
			Statistics:      reg.GetStatistics(),
		})
	}

	slices.SortFunc(proxies, func(a, b *cppb.GetCacheProxiesResponse_CacheProxy) int {
		if c := strings.Compare(a.GetNode().GetHost(), b.GetNode().GetHost()); c != 0 {
			return c
		}
		return strings.Compare(a.GetNode().GetProxyId(), b.GetNode().GetProxyId())
	})

	return &cppb.GetCacheProxiesResponse{
		CacheProxy:    proxies,
		UpgradePrompt: s.upgradePrompt(ctx, proxies),
	}, nil
}
