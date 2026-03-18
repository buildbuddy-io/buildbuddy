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
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	snpb "github.com/buildbuddy-io/buildbuddy/proto/server_notification"
)

var (
	enableIPRules = flag.Bool("auth.ip_rules.enable", false, "If true, IP rules will be checked during auth.")
	cacheTTL      = flag.Duration("auth.ip_rules.cache_ttl", 5*time.Minute, "Duration of time IP rules will be cached in memory.")
)

const (
	// The number of IP rules (net.IPNet instances) that we will store in memory.
	cacheSize = 100_000
)

type ipRule struct {
	id      string
	allowed *net.IPNet
}

type ipRuleCache interface {
	Add(groupID string, allowed []ipRule) bool
	Remove(groupID string) bool
	Get(groupID string) ([]ipRule, bool)
}

type noopIpRuleCache struct {
}

func (c *noopIpRuleCache) Add(groupID string, allowed []ipRule) bool {
	return false
}

func (c *noopIpRuleCache) Remove(groupID string) bool {
	return false
}

func (c *noopIpRuleCache) Get(groupID string) ([]ipRule, bool) {
	return nil, false
}

func newIpRuleCache() (ipRuleCache, error) {
	if *cacheTTL == 0 {
		return &noopIpRuleCache{}, nil
	}
	return lru.New(&lru.Config[[]ipRule]{
		TTL:        *cacheTTL,
		MaxSize:    cacheSize,
		SizeFn:     func(v []ipRule) int64 { return int64(len(v)) },
		ThreadSafe: true,
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
	cache, err := newIpRuleCache()
	if err != nil {
		return nil, err
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

type Enforcer struct {
	env           environment.Env
	rulesProvider ipRulesProvider
}

type NoOpEnforcer struct{}

func (n *NoOpEnforcer) Authorize(ctx context.Context) error {
	return nil
}

func (n *NoOpEnforcer) AuthorizeGroup(ctx context.Context, groupID string) error {
	return nil
}

func (n *NoOpEnforcer) AuthorizeHTTPRequest(ctx context.Context, r *http.Request) error {
	return nil
}

func (n *NoOpEnforcer) InvalidateCache(ctx context.Context, groupID string) {
}

func (n *NoOpEnforcer) Check(ctx context.Context, groupID, skipRuleID string) error {
	return nil
}

func New(env environment.Env) (*Enforcer, error) {
	rulesProvider, err := newDBIPRulesProvider(env)
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

func (s *Enforcer) authorize(ctx context.Context, groupID string) error {
	start := time.Now()
	err := s.Check(ctx, groupID, "" /*=skipRuleID*/)
	metrics.IPRulesCheckLatencyUsec.With(
		prometheus.Labels{metrics.StatusHumanReadableLabel: status.MetricsLabel(err)},
	).Observe(float64(time.Since(start).Microseconds()))
	return err
}

func (s *Enforcer) AuthorizeGroup(ctx context.Context, groupID string) error {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	// Server admins in impersonation mode can bypass IP rules.
	if u.IsImpersonating() {
		return nil
	}

	g, err := s.env.GetUserDB().GetGroupByID(ctx, groupID)
	if err != nil {
		return err
	}
	if !g.EnforceIPRules {
		return nil
	}

	return s.authorize(ctx, groupID)
}

func (s *Enforcer) Authorize(ctx context.Context) error {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		// If auth failed we don't need to (and can't) apply IP rules.
		return nil
	}

	// Server admins in impersonation mode can bypass IP rules.
	if u.IsImpersonating() {
		return nil
	}

	if !u.GetEnforceIPRules() {
		return nil
	}

	if cis := s.env.GetClientIdentityService(); cis != nil {
		si, err := cis.IdentityFromContext(ctx)
		// Trusted clients with signed identity.
		if err == nil && (si.Client == interfaces.ClientIdentityExecutor ||
			si.Client == interfaces.ClientIdentityApp ||
			si.Client == interfaces.ClientIdentityWorkflow) {
			return nil
		}
		if err != nil && !status.IsNotFoundError(err) {
			return err
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

func (s *Enforcer) AuthorizeHTTPRequest(ctx context.Context, r *http.Request) error {
	// GetUser is used by the frontend to know what the user is allowed to
	// do, including whether or not they are allowed access by IP rules.
	if r.URL.Path == "/rpc/BuildBuddyService/GetUser" {
		return nil
	}

	// GetGroup is used to lookup group metadata for impersonation.
	if r.URL.Path == "/rpc/BuildBuddyService/GetGroup" {
		return nil
	}

	// All other APIs are subject to IP access checks.
	if strings.HasPrefix(r.URL.Path, "/rpc/") || strings.HasPrefix(r.URL.Path, "/api/") {
		err := s.Authorize(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Enforcer) InvalidateCache(ctx context.Context, groupID string) {
	s.rulesProvider.invalidate(ctx, groupID)
}
