package ip_rules_service

import (
	"context"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
	snpb "github.com/buildbuddy-io/buildbuddy/proto/server_notification"
)

var (
	allowIPV6 = flag.Bool("auth.ip_rules.allow_ipv6", false, "If true, IPv6 rules will be allowed.")
)

type Service struct {
	env      environment.Env
	enforcer interfaces.IPRulesEnforcer
}

func New(env environment.Env) (*Service, error) {
	if env.GetIPRulesEnforcer() == nil {
		return nil, status.FailedPreconditionError("IPRulesService requires an IPRulesEnforcer")
	}
	return &Service{
		env:      env,
		enforcer: env.GetIPRulesEnforcer(),
	}, nil
}

func Register(env *real_environment.RealEnv) error {
	service, err := New(env)
	if err != nil {
		return err
	}
	env.SetIPRulesService(service)
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

func (s *Service) loadRulesFromDB(ctx context.Context, groupID string) ([]*tables.IPRule, error) {
	rq := s.env.GetDBHandle().NewQuery(ctx, "iprules_load_rules").Raw(
		`SELECT * FROM "IPRules" WHERE group_id = ? ORDER BY created_at_usec`, groupID)
	rules, err := db.ScanAll(rq, &tables.IPRule{})
	if err != nil {
		return nil, err
	}
	return rules, nil
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
		groupID := req.GetRequestContext().GetGroupId()
		s.enforcer.InvalidateCache(ctx, groupID)
		err := s.enforcer.Check(ctx, groupID, "" /*=skipRuleID*/)
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

func (s *Service) GetIPRules(ctx context.Context, req *irpb.GetRulesRequest) (*irpb.GetRulesResponse, error) {
	if err := s.enforcer.AuthorizeGetIPRules(ctx); err != nil {
		return nil, err
	}
	return s.GetRules(ctx, req)
}

func validateIPRange(value string) (string, error) {
	value = strings.TrimSpace(value)

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
		// Check if deleting the rule would block the client calling this API.
		groupID := req.GetRequestContext().GetGroupId()
		s.enforcer.InvalidateCache(ctx, groupID)
		err := s.enforcer.Check(ctx, groupID, req.GetIpRuleId())
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
