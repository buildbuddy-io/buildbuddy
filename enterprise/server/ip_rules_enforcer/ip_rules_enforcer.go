package ip_rules_enforcer

import (
	"context"
	"flag"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/metadata"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
	snpb "github.com/buildbuddy-io/buildbuddy/proto/server_notification"
)

var (
	enableIPRules           = flag.Bool("auth.ip_rules.enable", false, "If true, IP rules will be checked during auth.")
	cacheTTL                = flag.Duration("auth.ip_rules.cache_ttl", 5*time.Minute, "Duration of time IP rules will be cached in memory.")
	remoteIPRulesTarget     = flag.String("auth.ip_rules.remote.target", "", "The gRPC target of the backend storing IP rules.")
	remoteIPRulesRPCTimeout = flag.Duration("auth.ip_rules.remote.rpc_timeout", 15*time.Second, "Timeout for remote IP rules RPCs.")
)

const (
	// The number of IP rules (net.IPNet instances) that we will store in memory.
	cacheSize = 100_000

	// bypassIPRulesMetadataKey is a gRPC metadata key indicating that the peer
	// would not like IP rules to be enforced for this request. This may be
	// because the peer has enforced rules already, or because this is a system
	// request.
	//
	// IMPORTANT: this metadata should only be accepted from trusted peers. The
	// client identity framework can be used to verify the identity of the peer.
	bypassIPRulesMetadataKey = "x-buildbuddy-bypass-ip-rules"
)

type ipRule struct {
	id      string
	allowed *net.IPNet
}

type ipRuleCache interface {
	Add(groupID string, allowed []ipRule) bool
	Remove(groupID string) bool
	Get(groupID string) ([]ipRule, bool)
	Keys() []string
}

type noopIPRuleCache struct {
}

func (c *noopIPRuleCache) Add(groupID string, allowed []ipRule) bool {
	return false
}

func (c *noopIPRuleCache) Remove(groupID string) bool {
	return false
}

func (c *noopIPRuleCache) Get(groupID string) ([]ipRule, bool) {
	return nil, false
}

func (c *noopIPRuleCache) Keys() []string {
	return nil
}

func newIPRuleCache(clock clockwork.Clock) (ipRuleCache, error) {
	if *cacheTTL <= 0 {
		return &noopIPRuleCache{}, nil
	}
	return lru.New(&lru.Config[[]ipRule]{
		TTL:        *cacheTTL,
		MaxSize:    cacheSize,
		SizeFn:     func(v []ipRule) int64 { return int64(len(v)) },
		ThreadSafe: true,
		Clock:      clock,
	})
}

// An abstraction for retrieving IP rules from a source of truth.
type ipRulesProvider interface {
	get(ctx context.Context, groupID string) ([]ipRule, error)
	invalidate(ctx context.Context, groupID string)
	startRefresher(env environment.Env) error
}

// An implementation of ipRulesProvider that retrieves IP rules from a database.
type dbIPRulesProvider struct {
	db    interfaces.DBHandle
	cache ipRuleCache
}

func newDBIPRulesProvider(env environment.Env) (*dbIPRulesProvider, error) {
	cache, err := newIPRuleCache(env.GetClock())
	if err != nil {
		return nil, err
	}
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("DB-backed IP Rules Provider requires a DB")
	}
	return &dbIPRulesProvider{
		db:    env.GetDBHandle(),
		cache: cache,
	}, nil
}

func (p *dbIPRulesProvider) loadRulesFromDB(ctx context.Context, groupID string) ([]*tables.IPRule, error) {
	rq := p.db.NewQuery(ctx, "iprules_load_rules").Raw(
		`SELECT * FROM "IPRules" WHERE group_id = ? ORDER BY created_at_usec`, groupID)
	rules, err := db.ScanAll(rq, &tables.IPRule{})
	if err != nil {
		return nil, err
	}
	return rules, nil
}

func (p *dbIPRulesProvider) loadParsedRulesFromDB(ctx context.Context, groupID string) ([]ipRule, error) {
	rs, err := p.loadRulesFromDB(ctx, groupID)
	if err != nil {
		return nil, err
	}

	var allowed []ipRule
	for _, r := range rs {
		_, ipNet, err := net.ParseCIDR(r.CIDR)
		if err != nil {
			alert.UnexpectedEvent("unparsable CIDR rule", "rule %q", r.CIDR)
			continue
		}
		allowed = append(allowed, ipRule{
			id:      r.IPRuleID,
			allowed: ipNet,
		})
	}
	return allowed, nil
}

func (p *dbIPRulesProvider) refreshRules(ctx context.Context, groupID string) error {
	pr, err := p.loadParsedRulesFromDB(ctx, groupID)
	if err != nil {
		return err
	}
	p.cache.Add(groupID, pr)
	log.CtxInfof(ctx, "refreshed IP rules for group %s", groupID)
	return nil
}

func (p *dbIPRulesProvider) get(ctx context.Context, groupID string) ([]ipRule, error) {
	allowed, ok := p.cache.Get(groupID)
	if !ok {
		pr, err := p.loadParsedRulesFromDB(ctx, groupID)
		if err != nil {
			return nil, err
		}
		p.cache.Add(groupID, pr)
		allowed = pr
	}
	return allowed, nil
}

func (p *dbIPRulesProvider) invalidate(ctx context.Context, groupID string) {
	p.cache.Remove(groupID)
}

func (p *dbIPRulesProvider) startRefresher(env environment.Env) error {
	sns := env.GetServerNotificationService()
	if sns == nil {
		return nil
	}
	hc := env.GetHealthChecker()
	if hc == nil {
		return status.FailedPreconditionError("Missing health checker")
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	closeStop := sync.OnceFunc(func() { close(stop) })
	sub := sns.Subscribe(&snpb.InvalidateIPRulesCache{})
	hc.RegisterShutdownFunction(func(ctx context.Context) error {
		closeStop()
		return p.waitForShutdown(ctx, done)
	})
	go p.runRefresher(env.GetServerContext(), sub, stop, done)
	return nil
}

// runRefresher listens for cache invalidation messages and refreshes the IP
// rules cache accordingly. It closes the done channel when it exits, and can be
// stopped by another goroutine via the stop channel.
func (p *dbIPRulesProvider) runRefresher(ctx context.Context, sub <-chan proto.Message, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-stop:
			return
		case msg, ok := <-sub:
			if !ok {
				return
			}
			p.handleRefresherMessage(ctx, msg)
		}
	}
}

func (p *dbIPRulesProvider) handleRefresherMessage(ctx context.Context, msg proto.Message) {
	ic, ok := msg.(*snpb.InvalidateIPRulesCache)
	if !ok {
		alert.UnexpectedEvent("iprules_invalid_proto_type", "received proto type %T", msg)
		return
	}
	if err := p.refreshRules(ctx, ic.GetGroupId()); err != nil {
		log.Warningf("could not refresh IP rules for group %q: %s", ic.GetGroupId(), err)
	}
}

// The notification service does not expose an unsubscribe API, so the shutdown
// callback first signals the refresher to stop waiting on the subscription
// channel, then calls this helper to wait for the goroutine to confirm it has
// exited.
func (p *dbIPRulesProvider) waitForShutdown(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type remoteIPRulesProvider struct {
	client irpb.IPRulesServiceClient
	cache  ipRuleCache
	clock  clockwork.Clock
	sf     singleflight.Group[string, []ipRule]
}

func newRemoteIPRulesProvider(env environment.Env, target string) (*remoteIPRulesProvider, error) {
	conn, err := grpc_client.DialInternal(env, target)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to dial remote IP rules backend %q: %v", target, err)
	}
	if hc := env.GetHealthChecker(); hc != nil {
		hc.RegisterShutdownFunction(func(ctx context.Context) error {
			return conn.Close()
		})
	}
	cache, err := newIPRuleCache(env.GetClock())
	if err != nil {
		return nil, err
	}
	return &remoteIPRulesProvider{
		client: irpb.NewIPRulesServiceClient(conn),
		cache:  cache,
		clock:  env.GetClock(),
	}, nil
}

func (p *remoteIPRulesProvider) fetch(ctx context.Context, groupID string) ([]ipRule, error) {
	v, _, err := p.sf.Do(ctx, groupID, func(ctx context.Context) ([]ipRule, error) {
		ctx = SetBypassIPRules(ctx)
		ctx, cancel := context.WithTimeout(ctx, *remoteIPRulesRPCTimeout)
		defer cancel()
		rsp, err := p.client.GetIPRules(ctx, &irpb.GetRulesRequest{
			RequestContext: &ctxpb.RequestContext{
				GroupId: groupID,
			},
		})
		if err != nil {
			return nil, err
		}
		allowed := make([]ipRule, 0, len(rsp.GetIpRules()))
		for _, r := range rsp.GetIpRules() {
			_, ipNet, err := net.ParseCIDR(r.GetCidr())
			if err != nil {
				alert.CtxUnexpectedEvent(ctx, "unparsable CIDR rule", "rule %q", r.GetCidr())
				continue
			}
			allowed = append(allowed, ipRule{
				id:      r.GetIpRuleId(),
				allowed: ipNet,
			})
		}
		return allowed, nil
	})
	return v, err
}

func (p *remoteIPRulesProvider) get(ctx context.Context, groupID string) ([]ipRule, error) {
	allowed, ok := p.cache.Get(groupID)
	if ok {
		return allowed, nil
	}
	allowed, err := p.fetch(ctx, groupID)
	if err != nil {
		return nil, err
	}
	p.cache.Add(groupID, allowed)
	return allowed, nil
}

func (p *remoteIPRulesProvider) invalidate(ctx context.Context, groupID string) {
	p.cache.Remove(groupID)
}

func (p *remoteIPRulesProvider) startRefresher(env environment.Env) error {
	if *cacheTTL <= 0 {
		return nil
	}
	hc := env.GetHealthChecker()
	if hc == nil {
		return status.FailedPreconditionError("Missing health checker")
	}
	ctx, cancel := context.WithCancel(env.GetServerContext())
	done := make(chan struct{})
	hc.RegisterShutdownFunction(func(shutdownCtx context.Context) error {
		cancel()
		select {
		case <-done:
			return nil
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
	})
	go p.runRefresher(ctx, done)
	return nil
}

func (p *remoteIPRulesProvider) runRefresher(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := p.clock.NewTicker(*cacheTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			p.refreshAll(ctx)
		}
	}
}

func (p *remoteIPRulesProvider) refreshAll(ctx context.Context) {
	for _, groupID := range p.cache.Keys() {
		if ctx.Err() != nil {
			return
		}
		if err := p.refresh(ctx, groupID); err != nil {
			log.Warningf("could not refresh IP rules for group %q: %s", groupID, err)
		}
	}
}

func (p *remoteIPRulesProvider) refresh(ctx context.Context, groupID string) error {
	rules, err := p.fetch(ctx, groupID)
	if err != nil {
		return err
	}
	p.cache.Add(groupID, rules)
	log.CtxDebugf(ctx, "refreshed IP rules for group %s", groupID)
	return nil
}

func newIPRulesProvider(env environment.Env) (ipRulesProvider, error) {
	if *remoteIPRulesTarget == "" {
		return newDBIPRulesProvider(env)
	}
	return newRemoteIPRulesProvider(env, *remoteIPRulesTarget)

}

type Enforcer struct {
	env           environment.Env
	rulesProvider ipRulesProvider
}

type NoOpEnforcer struct{}

func (n *NoOpEnforcer) Authorize(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (n *NoOpEnforcer) AuthorizeGroup(ctx context.Context, groupID string) (context.Context, error) {
	return ctx, nil
}

func (n *NoOpEnforcer) AuthorizeHTTPRequest(ctx context.Context, r *http.Request) (context.Context, error) {
	return ctx, nil
}

func (n *NoOpEnforcer) InvalidateCache(ctx context.Context, groupID string) {
}

func (n *NoOpEnforcer) Check(ctx context.Context, groupID, skipRuleID string) error {
	return nil
}

func New(env environment.Env) (*Enforcer, error) {
	rulesProvider, err := newIPRulesProvider(env)
	if err != nil {
		return nil, err
	}

	if err := rulesProvider.startRefresher(env); err != nil {
		return nil, err
	}
	return &Enforcer{
		env:           env,
		rulesProvider: rulesProvider,
	}, nil
}

func Register(env *real_environment.RealEnv) error {
	var enforcer interfaces.IPRulesEnforcer = &NoOpEnforcer{}
	if *enableIPRules {
		realEnforcer, err := New(env)
		if err != nil {
			return err
		}
		enforcer = realEnforcer
	}
	env.SetIPRulesEnforcer(enforcer)
	return nil
}

func (s *Enforcer) Check(ctx context.Context, groupID, skipRuleID string) error {
	rawClientIP := clientip.Get(ctx)
	clientIP := net.ParseIP(rawClientIP)
	// Client IP is not parsable.
	if clientIP == nil {
		return status.FailedPreconditionErrorf("client IP %q is not valid", rawClientIP)
	}

	rules, err := s.rulesProvider.get(ctx, groupID)
	if err != nil {
		return err
	}

	for _, rule := range rules {
		if rule.id == skipRuleID {
			continue
		}
		if rule.allowed.Contains(clientIP) {
			return nil
		}
	}

	return status.PermissionDeniedErrorf("Client %q is not allowed by Organization IP rules", rawClientIP)
}

func (s *Enforcer) authorize(ctx context.Context, groupID string) (context.Context, error) {
	start := time.Now()
	err := s.Check(ctx, groupID, "" /*=skipRuleID*/)
	metrics.IPRulesCheckLatencyUsec.With(
		prometheus.Labels{metrics.StatusHumanReadableLabel: status.MetricsLabel(err)},
	).Observe(float64(time.Since(start).Microseconds()))
	if err != nil {
		return ctx, err
	}
	if *remoteIPRulesTarget != "" {
		ctx = SetBypassIPRules(ctx)
	}
	return ctx, nil
}

func (s *Enforcer) AuthorizeGroup(ctx context.Context, groupID string) (context.Context, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return ctx, err
	}
	// Server admins in impersonation mode can bypass IP rules.
	if u.IsImpersonating() {
		return ctx, nil
	}

	g, err := s.env.GetUserDB().GetGroupByID(ctx, groupID)
	if err != nil {
		return ctx, err
	}
	if !g.EnforceIPRules {
		return ctx, nil
	}

	return s.authorize(ctx, groupID)
}

func (s *Enforcer) Authorize(ctx context.Context) (context.Context, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		// If auth failed we don't need to (and can't) apply IP rules.
		return ctx, nil
	}

	// Server admins in impersonation mode can bypass IP rules.
	if u.IsImpersonating() {
		return ctx, nil
	}

	if !u.GetEnforceIPRules() {
		return ctx, nil
	}

	if cis := s.env.GetClientIdentityService(); cis != nil {
		si, err := cis.IdentityFromContext(ctx)
		// Trusted clients with signed identity.
		if err == nil && (si.Client == interfaces.ClientIdentityExecutor ||
			si.Client == interfaces.ClientIdentityApp ||
			si.Client == interfaces.ClientIdentityWorkflow) {
			return ctx, nil
		}

		// Allow trusted client to bypass local IP rule enforcement.
		if err == nil && bypassIPRules(ctx, si) {
			// Propagate the IP rule bypass to downstream requests, so
			// proxy-to-proxy isn't subject to IP rules at the destination.
			if *remoteIPRulesTarget != "" {
				ctx = SetBypassIPRules(ctx)
			}
			return ctx, nil
		}

		if err != nil && !status.IsNotFoundError(err) {
			return ctx, err
		}
	}

	groupID := u.GetGroupID()
	// For API keys, use the ACL list from the group that owns the API key
	// rather than the group that is the target of the API request.
	// For most API key users the OwnerGroupID is the same as the effective
	// GroupID, but for customers that use a parent/child hierarchy we want to
	// enforce the rules using the group to which the API key belongs to.
	if u.GetAPIKeyInfo().OwnerGroupID != "" {
		groupID = u.GetAPIKeyInfo().OwnerGroupID
	}
	return s.authorize(ctx, groupID)
}

func (s *Enforcer) AuthorizeHTTPRequest(ctx context.Context, r *http.Request) (context.Context, error) {
	// GetUser is used by the frontend to know what the user is allowed to
	// do, including whether or not they are allowed access by IP rules.
	if r.URL.Path == "/rpc/BuildBuddyService/GetUser" {
		return ctx, nil
	}

	// GetGroup is used to lookup group metadata for impersonation.
	if r.URL.Path == "/rpc/BuildBuddyService/GetGroup" {
		return ctx, nil
	}

	// All other APIs are subject to IP access checks.
	if strings.HasPrefix(r.URL.Path, "/rpc/") || strings.HasPrefix(r.URL.Path, "/api/") {
		return s.Authorize(ctx)
	}

	return ctx, nil
}

func (s *Enforcer) InvalidateCache(ctx context.Context, groupID string) {
	s.rulesProvider.invalidate(ctx, groupID)
}

func bypassIPRules(ctx context.Context, client *interfaces.ClientIdentity) bool {
	if client == nil || client.Client != interfaces.ClientIdentityCacheProxy {
		return false
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get(bypassIPRulesMetadataKey)
	return len(vals) > 0 && vals[0] == "true"
}

func SetBypassIPRules(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, bypassIPRulesMetadataKey, "true")
}
