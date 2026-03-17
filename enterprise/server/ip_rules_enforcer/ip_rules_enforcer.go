package ip_rules_enforcer

import (
	"context"
	"flag"
	"net"
	"net/http"
	"strings"
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

type ipRuleCache interface {
	Add(groupID string, allowed []*net.IPNet) bool
	Get(groupID string) ([]*net.IPNet, bool)
}

type noopIpRuleCache struct {
}

func (c *noopIpRuleCache) Add(groupID string, allowed []*net.IPNet) bool {
	return false
}

func (c *noopIpRuleCache) Get(groupID string) ([]*net.IPNet, bool) {
	return nil, false
}

func newIpRuleCache() (ipRuleCache, error) {
	if *cacheTTL == 0 {
		return &noopIpRuleCache{}, nil
	}
	return lru.New(&lru.Config[[]*net.IPNet]{
		TTL:        *cacheTTL,
		MaxSize:    cacheSize,
		SizeFn:     func(v []*net.IPNet) int64 { return int64(len(v)) },
		ThreadSafe: true,
	})
}

// An abstraction for retrieving IP rules from a source of truth.
type ipRulesProvider interface {
	// TODO(iain): get rid of skipCache and skipRuleID.
	get(ctx context.Context, groupID string, skipCache bool, skipRuleID string) ([]*net.IPNet, error)
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

func (p *dbIPRulesProvider) loadParsedRulesFromDB(ctx context.Context, groupID string, skipRuleID string) ([]*net.IPNet, error) {
	rs, err := p.loadRulesFromDB(ctx, groupID)
	if err != nil {
		return nil, err
	}

	var allowed []*net.IPNet
	for _, r := range rs {
		if r.IPRuleID == skipRuleID {
			continue
		}
		_, ipNet, err := net.ParseCIDR(r.CIDR)
		if err != nil {
			alert.UnexpectedEvent("unparsable CIDR rule", "rule %q", r.CIDR)
			continue
		}
		allowed = append(allowed, ipNet)
	}
	return allowed, nil
}

func (p *dbIPRulesProvider) refreshRules(ctx context.Context, groupID string) error {
	pr, err := p.loadParsedRulesFromDB(ctx, groupID, "" /*=skipRuleId*/)
	if err != nil {
		return err
	}
	p.cache.Add(groupID, pr)
	log.CtxInfof(ctx, "refreshed IP rules for group %s", groupID)
	return nil
}

func (p *dbIPRulesProvider) get(ctx context.Context, groupID string, skipCache bool, skipRuleID string) ([]*net.IPNet, error) {
	allowed, ok := p.cache.Get(groupID)
	if !ok || skipCache {
		pr, err := p.loadParsedRulesFromDB(ctx, groupID, skipRuleID)
		if err != nil {
			return nil, err
		}
		// if skipRuleID is set, the retrieved rule list may be incomplete.
		if skipRuleID == "" {
			p.cache.Add(groupID, pr)
		}
		allowed = pr
	}
	return allowed, nil
}

// TODO(iain): halt goroutine on server exit.
func (p *dbIPRulesProvider) startRefresher(env environment.Env) error {
	sns := env.GetServerNotificationService()
	if sns == nil {
		return nil
	}
	go func() {
		for msg := range sns.Subscribe(&snpb.InvalidateIPRulesCache{}) {
			ic, ok := msg.(*snpb.InvalidateIPRulesCache)
			if !ok {
				alert.UnexpectedEvent("iprules_invalid_proto_type", "received proto type %T", msg)
				continue
			}
			if err := p.refreshRules(env.GetServerContext(), ic.GetGroupId()); err != nil {
				log.Warningf("could not refresh IP rules for group %q: %s", ic.GetGroupId(), err)
			}
		}
	}()
	return nil
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

func (n *NoOpEnforcer) Check(ctx context.Context, groupID string, skipCache bool, skipRuleID string) error {
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

func (s *Enforcer) Check(ctx context.Context, groupID string, skipCache bool, skipRuleID string) error {
	rawClientIP := clientip.Get(ctx)
	clientIP := net.ParseIP(rawClientIP)
	// Client IP is not parsable.
	if clientIP == nil {
		return status.FailedPreconditionErrorf("client IP %q is not valid", rawClientIP)
	}

	allowed, err := s.rulesProvider.get(ctx, groupID, skipCache, skipRuleID)
	if err != nil {
		return err
	}

	for _, a := range allowed {
		if a.Contains(clientIP) {
			return nil
		}
	}

	return status.PermissionDeniedErrorf("Client %q is not allowed by Organization IP rules", rawClientIP)
}

func (s *Enforcer) authorize(ctx context.Context, groupID string) error {
	start := time.Now()
	err := s.Check(ctx, groupID, false /*=skipCache*/, "" /*skipRuleID*/)
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
