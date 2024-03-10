package iprules

import (
	"context"
	"flag"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
	snpb "github.com/buildbuddy-io/buildbuddy/proto/server_notification"
)

var (
	enableIPRules = flag.Bool("auth.ip_rules.enable", false, "If true, IP rules will be checked during auth.")
	cacheTTL      = flag.Duration("auth.ip_rules.cache_ttl", 5*time.Minute, "Duration of time IP rules will be cached in memory.")
	allowIPV6     = flag.Bool("auth.ip_rules.allow_ipv6", false, "If true, IPv6 rules will be allowed.")
)

const (
	// The number of IP rules (net.IPNet instances) that we will store in memory.
	cacheSize = 100_000
)

type ipRuleCacheEntry struct {
	allowed      []*net.IPNet
	expiresAfter time.Time
}

type ipRuleCache interface {
	Add(groupID string, allowed []*net.IPNet)
	Get(groupID string) ([]*net.IPNet, bool)
}

type memIpRuleCache struct {
	mu  sync.Mutex
	lru interfaces.LRU[*ipRuleCacheEntry]
}

func (c *memIpRuleCache) Get(groupID string) (allowed []*net.IPNet, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.lru.Get(groupID)
	if !ok {
		return nil, ok
	}
	if time.Now().After(entry.expiresAfter) {
		c.lru.Remove(groupID)
		return nil, false
	}
	return entry.allowed, true
}

func (c *memIpRuleCache) Add(groupID string, allowed []*net.IPNet) {
	c.mu.Lock()
	c.lru.Add(groupID, &ipRuleCacheEntry{allowed: allowed, expiresAfter: time.Now().Add(*cacheTTL)})
	c.mu.Unlock()
}

type noopIpRuleCache struct {
}

func (c *noopIpRuleCache) Add(groupID string, allowed []*net.IPNet) {
}

func (c *noopIpRuleCache) Get(groupID string) ([]*net.IPNet, bool) {
	return nil, false
}

func newIpRuleCache() (ipRuleCache, error) {
	if *cacheTTL == 0 {
		return &noopIpRuleCache{}, nil
	}
	config := &lru.Config[*ipRuleCacheEntry]{
		MaxSize: cacheSize,
		SizeFn:  func(v *ipRuleCacheEntry) int64 { return int64(len(v.allowed)) },
	}
	l, err := lru.NewLRU[*ipRuleCacheEntry](config)
	if err != nil {
		return nil, err
	}
	return &memIpRuleCache{
		lru: l,
	}, nil
}

type Service struct {
	env environment.Env

	cache ipRuleCache
}

func New(env environment.Env) (*Service, error) {
	cache, err := newIpRuleCache()
	if err != nil {
		return nil, err
	}

	svc := &Service{
		env:   env,
		cache: cache,
	}
	if sns := env.GetServerNotificationService(); sns != nil {
		go func() {
			for msg := range sns.Subscribe(&snpb.InvalidateIPRulesCache{}) {
				ic, ok := msg.(*snpb.InvalidateIPRulesCache)
				if !ok {
					alert.UnexpectedEvent("iprules_invalid_proto_type", "received proto type %T", msg)
					continue
				}
				if err := svc.refreshRules(env.GetServerContext(), ic.GetGroupId()); err != nil {
					log.Warningf("could not refresh IP rules for group %q: %s", ic.GetGroupId(), err)
				}
			}
		}()
	}
	return svc, nil
}

func Register(env *real_environment.RealEnv) error {
	if !*enableIPRules {
		return nil
	}

	enforcer, err := New(env)
	if err != nil {
		return err
	}
	env.SetIPRulesService(enforcer)
	return nil
}

func (s *Service) loadRulesFromDB(ctx context.Context, groupID string) ([]*tables.IPRule, error) {
	rq := s.env.GetDBHandle().NewQuery(ctx, "iprules_load_rules").Raw(
		`SELECT * FROM "IPRules" WHERE group_id = ? ORDER BY created_at_usec`, groupID)
	rules, err := db.ScanAll(rq, &tables.IPRule{})
	if err != nil {
		return nil, err
	}
	return rules, nil
}

func (s *Service) loadParsedRulesFromDB(ctx context.Context, groupID string, skipRuleID string) ([]*net.IPNet, error) {
	rs, err := s.loadRulesFromDB(ctx, groupID)
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

func (s *Service) refreshRules(ctx context.Context, groupID string) error {
	pr, err := s.loadParsedRulesFromDB(ctx, groupID, "" /*=skipRuleId*/)
	if err != nil {
		return err
	}
	s.cache.Add(groupID, pr)
	log.CtxInfof(ctx, "refreshed IP rules for group %s", groupID)
	return nil
}

func (s *Service) checkRules(ctx context.Context, groupID string, skipCache bool, skipRuleID string) error {
	rawClientIP := clientip.Get(ctx)
	clientIP := net.ParseIP(rawClientIP)
	// Client IP is not parsable.
	if clientIP == nil {
		return status.FailedPreconditionErrorf("client IP %q is not valid", rawClientIP)
	}

	allowed, ok := s.cache.Get(groupID)
	if !ok || skipCache {
		pr, err := s.loadParsedRulesFromDB(ctx, groupID, skipRuleID)
		if err != nil {
			return err
		}
		// if skipRuleID is set, the retrieved rule list maye be incomplete.
		if skipRuleID == "" {
			s.cache.Add(groupID, pr)
		}
		allowed = pr
	}

	for _, a := range allowed {
		if a.Contains(clientIP) {
			return nil
		}
	}

	return status.PermissionDeniedErrorf("Client %q is not allowed by Organization IP rules", rawClientIP)
}

func (s *Service) authorize(ctx context.Context, groupID string) error {
	start := time.Now()
	err := s.checkRules(ctx, groupID, false /*=skipCache*/, "" /*skipRuleID*/)
	metrics.IPRulesCheckLatencyUsec.With(
		prometheus.Labels{metrics.StatusHumanReadableLabel: status.MetricsLabel(err)},
	).Observe(float64(time.Since(start).Microseconds()))
	return err
}

func (s *Service) AuthorizeGroup(ctx context.Context, groupID string) error {
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

func (s *Service) Authorize(ctx context.Context) error {
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

	return s.authorize(ctx, u.GetGroupID())
}

func (s *Service) AuthorizeHTTPRequest(ctx context.Context, r *http.Request) error {
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

func (s *Service) checkAccess(ctx context.Context, groupID string) error {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		return err
	}
	return nil
}

func (s *Service) GetRule(ctx context.Context, groupID string, ruleID string) (*tables.IPRule, error) {
	if err := s.checkAccess(ctx, groupID); err != nil {
		return nil, err
	}

	r := &tables.IPRule{}
	err := s.env.GetDBHandle().NewQuery(ctx, "iprules_get").Raw(
		`SELECT * FROM "IPRules" WHERE group_id = ? AND ip_rule_id = ?`, groupID, ruleID).Take(r)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("rule %q not found", ruleID)
		}
		return nil, err
	}
	return r, nil
}

func (s *Service) GetIPRuleConfig(ctx context.Context, req *irpb.GetRulesConfigRequest) (*irpb.GetRulesConfigResponse, error) {
	if err := s.checkAccess(ctx, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	g, err := s.env.GetUserDB().GetGroupByID(ctx, req.GetRequestContext().GetGroupId())
	if err != nil {
		return nil, err
	}

	return &irpb.GetRulesConfigResponse{EnforceIpRules: g.EnforceIPRules}, nil
}

func (s *Service) SetIPRuleConfig(ctx context.Context, req *irpb.SetRulesConfigRequest) (*irpb.SetRulesConfigResponse, error) {
	if err := s.checkAccess(ctx, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	if req.GetEnforceIpRules() {
		err := s.checkRules(ctx, req.GetRequestContext().GetGroupId(), true /*=skipCache*/, "" /*=skipRuleID*/)
		if err != nil {
			if status.IsPermissionDeniedError(err) {
				return nil, status.InvalidArgumentErrorf("Enabling IP rule enforcement would block your IP (%s) from accessing the organization.", clientip.Get(ctx))
			}
			return nil, err
		}
	}

	g, err := s.env.GetUserDB().GetGroupByID(ctx, req.GetRequestContext().GetGroupId())
	if err != nil {
		return nil, err
	}
	g.EnforceIPRules = req.GetEnforceIpRules()
	if _, err := s.env.GetUserDB().UpdateGroup(ctx, g); err != nil {
		return nil, err
	}
	return &irpb.SetRulesConfigResponse{}, nil
}

func (s *Service) GetRules(ctx context.Context, req *irpb.GetRulesRequest) (*irpb.GetRulesResponse, error) {
	rules, err := s.loadRulesFromDB(ctx, req.GetRequestContext().GetGroupId())
	if err != nil {
		return nil, err
	}

	rsp := &irpb.GetRulesResponse{}
	for _, r := range rules {
		rsp.IpRules = append(rsp.IpRules, &irpb.IPRule{
			IpRuleId:    r.IPRuleID,
			Cidr:        r.CIDR,
			Description: r.Description,
		})
	}
	return rsp, nil
}

func validateIPRange(value string) (string, error) {
	prefix, err := netip.ParsePrefix(value)
	if err == nil {
		if prefix.Addr().Is6() && !*allowIPV6 {
			return "", status.InvalidArgumentErrorf("IPv6 addresses are not supported")
		}
		return prefix.String(), nil
	}

	if addr, err := netip.ParseAddr(value); err == nil {
		if addr.Is6() && !*allowIPV6 {
			return "", status.InvalidArgumentErrorf("IPv6 addresses are not supported")
		}
		return addr.String() + "/" + strconv.Itoa(addr.BitLen()), nil
	}

	return "", status.InvalidArgumentErrorf("Invalid IP range %q", value)
}

func (s *Service) publishRuleInvalidation(ctx context.Context, groupID string) {
	if sns := s.env.GetServerNotificationService(); sns != nil {
		if err := sns.Publish(ctx, &snpb.InvalidateIPRulesCache{GroupId: groupID}); err != nil {
			log.CtxWarningf(ctx, "could not send cache invalidation notification for group %q: %s", groupID, err)
		}
	}
}

func (s *Service) AddRule(ctx context.Context, req *irpb.AddRuleRequest) (*irpb.AddRuleResponse, error) {
	if err := s.checkAccess(ctx, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	cidr, err := validateIPRange(req.GetRule().GetCidr())
	if err != nil {
		return nil, err
	}

	id, err := tables.PrimaryKeyForTable("IPRules")
	if err != nil {
		return nil, err
	}

	groupID := req.GetRequestContext().GetGroupId()
	r := req.GetRule()
	q := `INSERT INTO "IPRules" (created_at_usec, ip_rule_id, group_id, cidr, description) VALUES (?, ?, ?, ?, ?)`
	if err := s.env.GetDBHandle().NewQuery(ctx, "iprules_add").Raw(
		q, time.Now().UnixMicro(), id, groupID, cidr, r.GetDescription()).Exec().Error; err != nil {
		return nil, err
	}
	r.IpRuleId = id

	s.publishRuleInvalidation(ctx, groupID)

	return &irpb.AddRuleResponse{Rule: r}, nil
}

func (s *Service) UpdateRule(ctx context.Context, req *irpb.UpdateRuleRequest) (*irpb.UpdateRuleResponse, error) {
	if err := s.checkAccess(ctx, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	cidr, err := validateIPRange(req.GetRule().GetCidr())
	if err != nil {
		return nil, err
	}

	groupID := req.GetRequestContext().GetGroupId()
	r := req.GetRule()
	q := `UPDATE "IPRules" SET cidr = ?, description = ? WHERE group_id = ? AND ip_rule_id = ?`
	if err := s.env.GetDBHandle().NewQuery(ctx, "iprules_update").Raw(
		q, cidr, r.GetDescription(), groupID, r.GetIpRuleId()).Exec().Error; err != nil {
		return nil, err
	}

	s.publishRuleInvalidation(ctx, groupID)

	return &irpb.UpdateRuleResponse{}, nil
}

func (s *Service) DeleteRule(ctx context.Context, req *irpb.DeleteRuleRequest) (*irpb.DeleteRuleResponse, error) {
	if err := s.checkAccess(ctx, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	g, err := s.env.GetUserDB().GetGroupByID(ctx, req.GetRequestContext().GetGroupId())
	if err != nil {
		return nil, err
	}
	if g.EnforceIPRules {
		// Check if deleting the rule would lock out the client calling this
		// API.
		err := s.checkRules(ctx, req.GetRequestContext().GetGroupId(), true /*=skipCache*/, req.GetIpRuleId())
		if err != nil {
			if status.IsPermissionDeniedError(err) {
				return nil, status.InvalidArgumentErrorf("Deleting this rule would block your IP (%s) from accessing the organization.", clientip.Get(ctx))
			}
			return nil, err
		}
	}

	groupID := req.GetRequestContext().GetGroupId()
	q := `DELETE FROM "IPRules" WHERE group_id = ? AND ip_rule_id = ?`
	if err := s.env.GetDBHandle().NewQuery(ctx, "iprules_delete").Raw(
		q, groupID, req.GetIpRuleId()).Exec().Error; err != nil {
		return nil, err
	}

	s.publishRuleInvalidation(ctx, groupID)

	return &irpb.DeleteRuleResponse{}, nil
}
