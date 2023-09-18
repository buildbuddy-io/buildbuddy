package iprules

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
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
)

var (
	enableIPRules = flag.Bool("auth.ip_rules.enable", false, "If true, IP rules will be checked during auth.")
	cacheTTL      = flag.Duration("auth.ip_rules.cache_ttl", 5*time.Minute, "Duration of time IP rules will be cached in memory.")
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

	return &Service{
		env:   env,
		cache: cache,
	}, nil
}

func Register(env environment.Env) error {
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

func (s *Service) loadRulesFromDB(ctx context.Context, groupID string) ([]*net.IPNet, error) {
	rows, err := s.env.GetDBHandle().DB(ctx).Raw(
		`SELECT * FROM "IPRules" WHERE group_id = ?`, groupID).Rows()
	if err != nil {
		return nil, err
	}

	var allowed []*net.IPNet
	for rows.Next() {
		r := &tables.IPRule{}
		if err := s.env.GetDBHandle().DB(ctx).ScanRows(rows, r); err != nil {
			return nil, err
		}
		_, ipNet, err := net.ParseCIDR(r.CIDR)
		if err != nil {
			alert.UnexpectedEvent("unparsable CIDR rule", "rule %q", r.CIDR)
			continue
		}
		allowed = append(allowed, ipNet)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return allowed, nil
}

func (s *Service) checkRules(ctx context.Context, groupID string) error {
	rawClientIP := clientip.Get(ctx)
	clientIP := net.ParseIP(rawClientIP)
	// Client IP is not parsable.
	if clientIP == nil {
		return status.FailedPreconditionErrorf("client IP %q is not valid", rawClientIP)
	}

	allowed, ok := s.cache.Get(groupID)
	if !ok {
		rs, err := s.loadRulesFromDB(ctx, groupID)
		if err != nil {
			return err
		}
		s.cache.Add(groupID, rs)
		allowed = rs
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
	err := s.checkRules(ctx, groupID)
	metrics.IPRulesCheckLatencyUsec.With(
		prometheus.Labels{metrics.StatusHumanReadableLabel: status.MetricsLabel(err)},
	).Observe(float64(time.Since(start).Microseconds()))
	return err
}

func (s *Service) AuthorizeGroup(ctx context.Context, groupID string) error {
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
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		if authutil.IsAnonymousUserError(err) {
			return nil
		}
		return err
	}

	// Server admins in impersonation mode can bypass IP rules.
	if u.IsImpersonating() {
		return nil
	}

	if !u.GetEnforceIPRules() {
		return nil
	}

	return s.authorize(ctx, u.GetGroupID())
}

func (s *Service) AuthorizeHTTPRequest(ctx context.Context, r *http.Request) error {
	// GetUser is used by the frontend to know what the user is allowed to
	//  do, including whether or not they are allowed access by IP rules.
	if r.URL.Path == "/rpc/BuildBuddyService/GetUser" {
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
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return err
	}
	if err := authutil.AuthorizeGroupRole(u, groupID, role.Admin); err != nil {
		return err
	}
	return nil
}

func (s *Service) AddRule(ctx context.Context, req *irpb.AddRuleRequest) (*irpb.AddRuleResponse, error) {
	if err := s.checkAccess(ctx, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	id, err := tables.PrimaryKeyForTable("IPRules")
	if err != nil {
		return nil, err
	}

	groupID := req.GetRequestContext().GetGroupId()
	r := req.GetRule()
	q := `INSERT INTO "IPRules" (created_at_usec, ip_rule_id, group_id, cidr, description) VALUES (?, ?, ?, ?, ?)`
	if err := s.env.GetDBHandle().DB(ctx).Exec(q, time.Now().UnixMicro(), id, groupID, r.GetCidr(), r.GetDescription()).Error; err != nil {
		return nil, err
	}
	r.IpRuleId = id
	return &irpb.AddRuleResponse{Rule: r}, nil
}

func (s *Service) UpdateRule(ctx context.Context, req *irpb.UpdateRuleRequest) (*irpb.UpdateRuleResponse, error) {
	if err := s.checkAccess(ctx, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	groupID := req.GetRequestContext().GetGroupId()
	r := req.GetRule()
	q := `UPDATE "IPRules" SET cidr = ?, description = ? WHERE group_id = ? AND ip_rule_id = ?`
	if err := s.env.GetDBHandle().DB(ctx).Exec(q, r.GetCidr(), r.GetDescription(), groupID, r.GetIpRuleId()).Error; err != nil {
		return nil, err
	}
	return &irpb.UpdateRuleResponse{}, nil
}

func (s *Service) DeleteRule(ctx context.Context, req *irpb.DeleteRuleRequest) (*irpb.DeleteRuleResponse, error) {
	if err := s.checkAccess(ctx, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	groupID := req.GetRequestContext().GetGroupId()
	q := `DELETE FROM "IPRules" WHERE group_id = ? AND ip_rule_id = ?`
	if err := s.env.GetDBHandle().DB(ctx).Exec(q, groupID, req.GetIpRuleId()).Error; err != nil {
		return nil, err
	}
	return &irpb.DeleteRuleResponse{}, nil
}
