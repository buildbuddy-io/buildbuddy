// This package implements a gRPC server that tracks the set of live cache
// proxies connected to the app, scoped by group.
//
// Cache proxies authenticate using an API key (which must have the
// REGISTER_CACHE_PROXY capability) and open a bidirectional streaming RPC
// (RegisterAndStreamHeartbeat). The server persists each heartbeat as a
// RegisteredCacheProxy entry in a per-group Redis hash, along with an ACL
// scoped to that group. Registrations live in the remote execution Redis
// (the same one executor registrations use) so that, in multi-cluster
// deployments where each cluster has its own default Redis, proxies
// registering with any cluster are visible from the apps serving the UI.
// While the stream is open, credentials are periodically re-validated so a
// revoked or downgraded API key terminates the stream rather than
// continuing to write registrations.
//
// The companion GetCacheProxies RPC reads that hash, drops entries that
// haven't checked in for a while, applies ACL filtering, and returns the
// survivors.
//
// GetCacheProxy fetches details from a single cache proxy by sending a details
// request on its registration stream with the requested cache proxy and waiting
// for the response. Only the app instance holding the stream can do this, so
// each instance tracks the streams it holds in memory and the mapping between
// app and registration stream is stored in Redis so other apps can forward
// requests to the correct peer for that cache proxy.
package cache_proxy_registry_server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/upgrade"
	"github.com/go-redis/redis/v8"
	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/types/known/timestamppb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
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

	// How long GetCacheProxy waits for a cache proxy to respond.
	getDetailsTimeout = 15 * time.Second

	// Connections to peer app instances that haven't been used for this long
	// are closed. Peers come and go during app rollouts.
	unusedPeerConnExpiration    = 10 * time.Minute
	unusedPeerConnCheckInterval = 1 * time.Minute

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
	// host:port at which this app instance can be reached. Stored with each
	// registration so other app instances can route requests for a proxy to
	// the instance holding its registration stream.
	ownHostPort string

	mu                  sync.Mutex
	newestVersion       *semver.Version
	newestVersionExpiry time.Time

	// The registration streams held by this app instance.
	streamsMu sync.Mutex
	streams   map[proxyKey]*registrationStream

	// Connections to peer app instances, keyed by host:port, used to forward
	// GetCacheProxy requests to the instance holding a proxy's stream.
	peerConns *grpc_client.ConnCache
}

type proxyKey struct {
	groupID string
	proxyID string
}

// registrationStream is a handle on a registration stream held by this app
// instance. Only the stream's RegisterAndStreamHeartbeat loop sends on the
// stream, so other goroutines pass details requests to it over a channel.
type registrationStream struct {
	detailsRequests chan *detailsRequest
	// Closed when RegisterAndStreamHeartbeat returns.
	done chan struct{}
}

type detailsRequest struct {
	ctx context.Context
	req *cppb.GetCacheProxyRequest
	// Receives the proxy's answer. Buffered so the stream loop never blocks
	// on a caller that has given up.
	rsp chan *cppb.CacheProxyDetails
}

func Register(env *real_environment.RealEnv) error {
	if env.GetRemoteExecutionRedisClient() == nil {
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
	rdb := env.GetRemoteExecutionRedisClient()
	if rdb == nil {
		return nil, status.FailedPreconditionError("Redis is required for cache proxy registration")
	}
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, status.FailedPreconditionError("Authenticator is required for cache proxy registration")
	}
	ownHostname, err := resources.GetMyHostname()
	if err != nil {
		return nil, status.UnknownErrorf("Could not determine own hostname: %s", err)
	}
	ownPort, err := resources.GetMyPort()
	if err != nil {
		return nil, status.UnknownErrorf("Could not determine own port: %s", err)
	}
	peerConns, err := grpc_client.NewConnCache(env, grpc_client.ConnCacheOpts{
		PoolSize:      1,
		Expiration:    unusedPeerConnExpiration,
		CheckInterval: unusedPeerConnCheckInterval,
	})
	if err != nil {
		return nil, err
	}
	quit := make(chan struct{})
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		close(quit)
		peerConns.StopExpiring()
		return nil
	})
	return &CacheProxyRegistryServer{
		authenticator: authenticator,
		clock:         env.GetClock(),
		rdb:           rdb,
		quit:          quit,
		detector:      detector,
		ownHostPort:   net.JoinHostPort(ownHostname, strconv.Itoa(int(ownPort))),
		streams:       make(map[proxyKey]*registrationStream),
		peerConns:     peerConns,
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

	handle := &registrationStream{
		detailsRequests: make(chan *detailsRequest),
		done:            make(chan struct{}),
	}
	defer close(handle.done)

	// Details requests sent to the proxy and not yet answered, by request ID.
	pending := make(map[string]*detailsRequest)
	var lastRequestID int64

	var proxyID string
	defer func() {
		if proxyID != "" {
			s.untrackStream(groupID, proxyID, handle)
		}
	}()
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
				return nil
			}
			summary := req.GetSummary()
			if summary == nil {
				log.CtxInfof(ctx, "Rejecting cache proxy heartbeat from group %q: missing summary info", groupID)
				return status.InvalidArgumentError("registration request missing summary info")
			}
			if summary.GetProxyId() == "" {
				log.CtxInfof(ctx, "Rejecting cache proxy heartbeat from group %q: missing proxy_id", groupID)
				return status.InvalidArgumentError("registration request missing proxy_id")
			}
			if proxyID != "" && summary.GetProxyId() != proxyID {
				log.CtxInfof(ctx, "Rejecting cache proxy heartbeat from group %q: proxy_id changed mid-stream from %q to %q", groupID, proxyID, summary.GetProxyId())
				return status.InvalidArgumentError("proxy_id changed during registration stream")
			}
			if req.GetShuttingDown() {
				log.CtxInfof(ctx, "Cache proxy %q (group %q) signalled shutdown; removing from registry", summary.GetProxyId(), groupID)
				if err := s.removeProxy(ctx, groupID, summary.GetProxyId()); err != nil {
					log.CtxWarningf(ctx, "Could not remove shutting-down cache proxy %q (group %q): %s", summary.GetProxyId(), groupID, err)
					return err
				}
				return nil
			}
			if err := s.insertOrUpdateProxy(ctx, groupID, summary, req.GetStatistics()); err != nil {
				log.CtxInfof(ctx, "Closing cache proxy registration stream for proxy %q (group %q): could not store registration: %s", summary.GetProxyId(), groupID, err)
				return err
			}
			if proxyID == "" {
				proxyID = summary.GetProxyId()
				s.trackStream(groupID, proxyID, handle)
			}
			log.CtxDebugf(ctx, "Cache proxy %q (host ID %q, host %q) checked in", proxyID, summary.GetProxyHostId(), summary.GetHost())
			if id := req.GetRequestId(); id != "" {
				if p, ok := pending[id]; ok {
					delete(pending, id)
					p.rsp <- req.GetDetails()
				}
			}
		case p := <-handle.detailsRequests:
			// Drop requests whose callers have given up, so pending can't
			// grow without bound if the proxy never answers.
			for id, old := range pending {
				if old.ctx.Err() != nil {
					delete(pending, id)
				}
			}
			lastRequestID++
			id := strconv.FormatInt(lastRequestID, 10)
			rsp := &cppb.RegisterCacheProxyResponse{
				DetailsRequest: &cppb.GetCacheProxyRequest{
					IncludeConfiguredFlags: p.req.GetIncludeConfiguredFlags(),
					IncludeStatistics:      p.req.GetIncludeStatistics(),
				},
				RequestId: id,
			}
			if err := stream.Send(rsp); err != nil {
				log.CtxWarningf(ctx, "Closing cache proxy registration stream for proxy %q (group %q): send failed: %s", proxyID, groupID, err)
				return err
			}
			pending[id] = p
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

func (s *CacheProxyRegistryServer) trackStream(groupID, proxyID string, handle *registrationStream) {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	// If the proxy reconnected, replace the old stream with the new one.
	s.streams[proxyKey{groupID, proxyID}] = handle
}

func (s *CacheProxyRegistryServer) untrackStream(groupID, proxyID string, handle *registrationStream) {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	key := proxyKey{groupID, proxyID}
	// Leave the entry alone if a newer stream for the same proxy replaced it.
	if s.streams[key] == handle {
		delete(s.streams, key)
	}
}

func (s *CacheProxyRegistryServer) lookupStream(groupID, proxyID string) *registrationStream {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	return s.streams[proxyKey{groupID, proxyID}]
}

func (s *CacheProxyRegistryServer) insertOrUpdateProxy(ctx context.Context, groupID string, summary *cppb.CacheProxySummary, stats *cppb.Statistics) error {
	acl := perms.ToACLProto(nil /*=userID*/, groupID, perms.GROUP_WRITE|perms.GROUP_READ)

	r := &cppb.RegisteredCacheProxy{
		Summary:      summary,
		GroupId:      groupID,
		Acl:          acl,
		LastPingTime: timestamppb.Now(),
		Statistics:   stats,
		AppHostPort:  s.ownHostPort,
	}
	b, err := proto.Marshal(r)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, redisKeyForCacheProxies(groupID), summary.GetProxyId(), b).Err()
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
		v, err := semver.NewVersion(reg.GetSummary().GetVersion())
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
		versions = append(versions, p.GetSummary().GetVersion())
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
		summary := reg.GetSummary()
		if summary == nil {
			continue
		}
		summary.LastCheckInTime = reg.GetLastPingTime()
		proxies = append(proxies, &cppb.GetCacheProxiesResponse_CacheProxy{
			Summary:         summary,
			LastCheckInTime: reg.GetLastPingTime(),
			Statistics:      reg.GetStatistics(),
		})
	}

	slices.SortFunc(proxies, func(a, b *cppb.GetCacheProxiesResponse_CacheProxy) int {
		if c := strings.Compare(a.GetSummary().GetHost(), b.GetSummary().GetHost()); c != 0 {
			return c
		}
		return strings.Compare(a.GetSummary().GetProxyId(), b.GetSummary().GetProxyId())
	})

	return &cppb.GetCacheProxiesResponse{
		CacheProxy:    proxies,
		UpgradePrompt: s.upgradePrompt(ctx, proxies),
	}, nil
}

func (s *CacheProxyRegistryServer) ListCacheProxies(ctx context.Context, req *cppb.ListCacheProxiesRequest) (*cppb.ListCacheProxiesResponse, error) {
	getReq := cppb.GetCacheProxiesRequest{RequestContext: req.GetRequestContext()}
	getResp, err := s.GetCacheProxies(ctx, &getReq)
	if err != nil {
		return nil, err
	}
	resp := cppb.ListCacheProxiesResponse{
		UpgradePrompt: getResp.GetUpgradePrompt(),
	}
	summaries := []*cppb.CacheProxySummary{}
	for _, summary := range getResp.GetCacheProxy() {
		summaries = append(summaries, summary.GetSummary())
	}
	resp.Summary = summaries
	return &resp, nil
}

func (s *CacheProxyRegistryServer) GetCacheProxy(ctx context.Context, req *cppb.GetCacheProxyRequest) (*cppb.GetCacheProxyResponse, error) {
	// As in GetCacheProxies, the group comes from the request context and
	// the caller's access to it is checked below.
	groupID := req.GetRequestContext().GetGroupId()
	if groupID == "" {
		return nil, status.InvalidArgumentError("group not specified")
	}
	proxyID := req.GetSelector().GetProxyId()
	if proxyID == "" {
		return nil, status.InvalidArgumentError("proxy_id not specified")
	}
	user, err := s.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	// Registrations are stored with this ACL, so check against it.
	acl := perms.ToACLProto(nil /*=userID*/, groupID, perms.GROUP_WRITE|perms.GROUP_READ)
	if err := perms.AuthorizeRead(user, acl); err != nil {
		return nil, err
	}

	// This deadline covers the whole request. If the request is forwarded,
	// gRPC propagates it to the peer, which applies the same logic, so a
	// single deadline governs every hop.
	ctx, cancel := context.WithTimeout(ctx, getDetailsTimeout)
	defer cancel()

	handle := s.lookupStream(groupID, proxyID)
	if handle == nil {
		if req.GetDoNotForward() {
			return nil, status.NotFoundErrorf("cache proxy %q is not connected to this server", proxyID)
		}
		return s.forwardGetCacheProxy(ctx, groupID, req)
	}

	p := &detailsRequest{
		ctx: ctx,
		req: req,
		rsp: make(chan *cppb.CacheProxyDetails, 1),
	}
	select {
	case handle.detailsRequests <- p:
	case <-handle.done:
		return nil, status.UnavailableErrorf("registration stream for cache proxy %q closed", proxyID)
	case <-ctx.Done():
		return nil, detailsWaitError(ctx, proxyID)
	}
	var details *cppb.CacheProxyDetails
	select {
	case details = <-p.rsp:
	case <-handle.done:
		return nil, status.UnavailableErrorf("registration stream for cache proxy %q closed", proxyID)
	case <-ctx.Done():
		return nil, detailsWaitError(ctx, proxyID)
	}
	if details == nil {
		details = &cppb.CacheProxyDetails{}
	}
	if details.GetSummary() != nil {
		// The proxy just answered, so it has just checked in.
		details.GetSummary().LastCheckInTime = timestamppb.New(s.clock.Now())
	}
	return &cppb.GetCacheProxyResponse{
		Details: details,
		UpgradePrompt: s.upgradePrompt(ctx, []*cppb.GetCacheProxiesResponse_CacheProxy{
			{Summary: details.GetSummary()},
		}),
	}, nil
}

// detailsWaitError returns the error for a GetCacheProxy request whose context
// ended while waiting for the proxy, directly or via a peer.
func detailsWaitError(ctx context.Context, proxyID string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return status.DeadlineExceededErrorf("cache proxy %q did not respond in time", proxyID)
	}
	return status.FromContextError(ctx)
}

// forwardGetCacheProxy forwards a GetCacheProxy request to the app instance
// that, according to Redis, holds the requested proxy's registration stream.
func (s *CacheProxyRegistryServer) forwardGetCacheProxy(ctx context.Context, groupID string, req *cppb.GetCacheProxyRequest) (*cppb.GetCacheProxyResponse, error) {
	proxyID := req.GetSelector().GetProxyId()
	data, err := s.rdb.HGet(ctx, redisKeyForCacheProxies(groupID), proxyID).Result()
	if err == redis.Nil {
		return nil, status.NotFoundErrorf("cache proxy %q not found", proxyID)
	}
	if err != nil {
		return nil, status.UnavailableErrorf("could not look up cache proxy %q: %s", proxyID, err)
	}
	reg := &cppb.RegisteredCacheProxy{}
	if err := proto.Unmarshal([]byte(data), reg); err != nil {
		return nil, status.InternalErrorf("could not parse registration for cache proxy %q: %s", proxyID, err)
	}
	if s.clock.Since(reg.GetLastPingTime().AsTime()) > maxRegistrationStaleness {
		return nil, status.NotFoundErrorf("cache proxy %q not found", proxyID)
	}
	peer := reg.GetAppHostPort()
	if peer == "" {
		return nil, status.UnavailableErrorf("registration for cache proxy %q does not record which app holds its stream", proxyID)
	}
	if peer == s.ownHostPort {
		// The registration says this instance holds the stream, but it
		// doesn't (e.g. the stream just closed).
		return nil, status.NotFoundErrorf("cache proxy %q is not connected to this server", proxyID)
	}

	conn, err := s.peerConns.Get(peer)
	if err != nil {
		return nil, err
	}
	client := bbspb.NewBuildBuddyServiceClient(conn)
	fwdReq := proto.Clone(req).(*cppb.GetCacheProxyRequest)
	fwdReq.DoNotForward = true
	rsp, err := client.GetCacheProxy(ctx, fwdReq)
	if err != nil {
		// The peer shares this deadline but sees it slightly later, so when
		// the proxy doesn't answer, this call times out first.
		if ctx.Err() != nil {
			err = detailsWaitError(ctx, proxyID)
		}
		log.CtxInfof(ctx, "Forwarding GetCacheProxy for cache proxy %q (group %q) to %q failed: %s", proxyID, groupID, peer, err)
		return nil, err
	}
	return rsp, nil
}
