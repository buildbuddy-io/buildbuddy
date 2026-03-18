package ip_rules_service_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ip_rules_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
	snpb "github.com/buildbuddy-io/buildbuddy/proto/server_notification"
)

type call struct {
	groupID      string
	invalidation bool
	check        bool
	skipRuleID   string
}

type fakeIPRulesEnforcer struct {
	calls    []call
	checkErr error
}

func (f *fakeIPRulesEnforcer) Authorize(ctx context.Context) error {
	return nil
}

func (f *fakeIPRulesEnforcer) AuthorizeGroup(ctx context.Context, groupID string) error {
	return nil
}

func (f *fakeIPRulesEnforcer) AuthorizeHTTPRequest(ctx context.Context, r *http.Request) error {
	return nil
}

func (f *fakeIPRulesEnforcer) InvalidateCache(ctx context.Context, groupID string) {
	f.calls = append(f.calls, call{
		groupID:      groupID,
		invalidation: true,
	})
}

func (f *fakeIPRulesEnforcer) Check(ctx context.Context, groupID string, skipRuleID string) error {
	f.calls = append(f.calls, call{
		groupID:    groupID,
		check:      true,
		skipRuleID: skipRuleID,
	})
	return f.checkErr
}

type fakeServerNotificationService struct {
	published []proto.Message
}

func (f *fakeServerNotificationService) Subscribe(msgType proto.Message) <-chan proto.Message {
	ch := make(chan proto.Message)
	close(ch)
	return ch
}

func (f *fakeServerNotificationService) Publish(ctx context.Context, msg proto.Message) error {
	f.published = append(f.published, proto.Clone(msg))
	return nil
}

func setupService(t *testing.T, enforcer *fakeIPRulesEnforcer, sns *fakeServerNotificationService) (*ip_rules_service.Service, environment.Env, context.Context, string) {
	t.Helper()

	env := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, env)
	if enforcer == nil {
		enforcer = &fakeIPRulesEnforcer{}
	}
	env.SetIPRulesEnforcer(enforcer)
	if sns != nil {
		env.SetServerNotificationService(sns)
	}

	u := enterprise_testauth.CreateRandomUser(t, env, "org1.invalid")
	authCtx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(context.Background(), u.UserID)
	require.NoError(t, err)
	groupID := u.Groups[0].Group.GroupID

	g, err := env.GetUserDB().GetGroupByID(authCtx, groupID)
	require.NoError(t, err)
	if g.URLIdentifier == "" {
		g.URLIdentifier = strings.ToLower(groupID + "-slug")
		_, err = env.GetUserDB().UpdateGroup(authCtx, g)
		require.NoError(t, err)
	}

	service, err := ip_rules_service.New(env)
	require.NoError(t, err)
	return service, env, authCtx, groupID
}

func setGroupEnforcement(t *testing.T, ctx context.Context, env environment.Env, groupID string, enabled bool) {
	t.Helper()

	g, err := env.GetUserDB().GetGroupByID(ctx, groupID)
	require.NoError(t, err)
	g.EnforceIPRules = enabled
	_, err = env.GetUserDB().UpdateGroup(ctx, g)
	require.NoError(t, err)
}

func requireInvalidationMessages(t *testing.T, sns *fakeServerNotificationService, wantGroupIDs ...string) {
	t.Helper()

	require.Len(t, sns.published, len(wantGroupIDs))
	for i, groupID := range wantGroupIDs {
		msg, ok := sns.published[i].(*snpb.InvalidateIPRulesCache)
		require.True(t, ok)
		require.Equal(t, groupID, msg.GetGroupId())
	}
}

func TestValidateIPRange(t *testing.T) {
	flags.Set(t, "auth.ip_rules.allow_ipv6", false)

	tests := []struct {
		name      string
		value     string
		wantCIDR  string
		allowIPv6 bool
		wantErr   func(error) bool
	}{
		{
			name:     "ipv4 address",
			value:    " 1.2.3.4 ",
			wantCIDR: "1.2.3.4/32",
		},
		{
			name:     "ipv4 prefix",
			value:    "1.2.3.0/24",
			wantCIDR: "1.2.3.0/24",
		},
		{
			name:    "invalid value",
			value:   "not an ip",
			wantErr: status.IsInvalidArgumentError,
		},
		{
			name:    "ipv6 disabled",
			value:   "2001:db8::1",
			wantErr: status.IsInvalidArgumentError,
		},
		{
			name:      "ipv6 enabled",
			value:     "2001:db8::1",
			wantCIDR:  "2001:db8::1/128",
			allowIPv6: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "auth.ip_rules.allow_ipv6", test.allowIPv6)

			svc, _, authCtx, groupID := setupService(t, nil, nil)
			addRsp, err := svc.AddRule(authCtx, &irpb.AddRuleRequest{
				RequestContext: &ctxpb.RequestContext{GroupId: groupID},
				Rule: &irpb.IPRule{
					Cidr:        test.value,
					Description: test.name,
				},
			})
			if test.wantErr != nil {
				require.Error(t, err)
				require.True(t, test.wantErr(err))
				return
			}

			require.NoError(t, err)
			stored, err := svc.GetRule(authCtx, groupID, addRsp.GetRule().GetIpRuleId())
			require.NoError(t, err)
			require.Equal(t, test.wantCIDR, stored.CIDR)
		})
	}
}

func TestAddUpdateDeleteRuleCRUD(t *testing.T) {
	sns := &fakeServerNotificationService{}
	svc, _, authCtx, groupID := setupService(t, nil, sns)

	addRsp, err := svc.AddRule(authCtx, &irpb.AddRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		Rule: &irpb.IPRule{
			Cidr:        " 1.2.3.4 ",
			Description: "initial rule",
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, addRsp.GetRule().GetIpRuleId())

	ruleID := addRsp.GetRule().GetIpRuleId()
	stored, err := svc.GetRule(authCtx, groupID, ruleID)
	require.NoError(t, err)
	require.Equal(t, "1.2.3.4/32", stored.CIDR)
	require.Equal(t, "initial rule", stored.Description)

	listRsp, err := svc.GetRules(authCtx, &irpb.GetRulesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
	})
	require.NoError(t, err)
	require.Len(t, listRsp.GetIpRules(), 1)
	require.Equal(t, "1.2.3.4/32", listRsp.GetIpRules()[0].GetCidr())

	_, err = svc.UpdateRule(authCtx, &irpb.UpdateRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		Rule: &irpb.IPRule{
			IpRuleId:    ruleID,
			Cidr:        "8.8.8.8",
			Description: "updated rule",
		},
	})
	require.NoError(t, err)

	stored, err = svc.GetRule(authCtx, groupID, ruleID)
	require.NoError(t, err)
	require.Equal(t, "8.8.8.8/32", stored.CIDR)
	require.Equal(t, "updated rule", stored.Description)

	_, err = svc.DeleteRule(authCtx, &irpb.DeleteRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		IpRuleId:       ruleID,
	})
	require.NoError(t, err)

	_, err = svc.GetRule(authCtx, groupID, ruleID)
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
	requireInvalidationMessages(t, sns, groupID, groupID, groupID)
}

func TestGetRuleNotFound(t *testing.T) {
	svc, _, authCtx, groupID := setupService(t, nil, nil)

	_, err := svc.GetRule(authCtx, groupID, "missing")
	require.Error(t, err)
	require.True(t, status.IsNotFoundError(err))
}

func TestSetAndGetIPRuleConfig(t *testing.T) {
	enforcer := &fakeIPRulesEnforcer{}
	svc, _, authCtx, groupID := setupService(t, enforcer, nil)

	cfgRsp, err := svc.GetIPRuleConfig(authCtx, &irpb.GetRulesConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
	})
	require.NoError(t, err)
	require.False(t, cfgRsp.GetEnforceIpRules())

	_, err = svc.SetIPRuleConfig(authCtx, &irpb.SetRulesConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		EnforceIpRules: true,
	})
	require.NoError(t, err)
	require.Equal(t, []call{
		{groupID: groupID, invalidation: true},
		{groupID: groupID, check: true},
	}, enforcer.calls)

	cfgRsp, err = svc.GetIPRuleConfig(authCtx, &irpb.GetRulesConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
	})
	require.NoError(t, err)
	require.True(t, cfgRsp.GetEnforceIpRules())

	_, err = svc.SetIPRuleConfig(authCtx, &irpb.SetRulesConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		EnforceIpRules: false,
	})
	require.NoError(t, err)
	require.Len(t, enforcer.calls, 2)

	cfgRsp, err = svc.GetIPRuleConfig(authCtx, &irpb.GetRulesConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
	})
	require.NoError(t, err)
	require.False(t, cfgRsp.GetEnforceIpRules())
}

func TestSetIPRuleConfigRejectsLockout(t *testing.T) {
	enforcer := &fakeIPRulesEnforcer{checkErr: status.PermissionDeniedError("blocked")}
	svc, _, authCtx, groupID := setupService(t, enforcer, nil)
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "9.8.7.6")

	_, err := svc.SetIPRuleConfig(authCtx, &irpb.SetRulesConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		EnforceIpRules: true,
	})
	require.Error(t, err)
	require.True(t, status.IsInvalidArgumentError(err))
	require.Contains(t, err.Error(), "9.8.7.6")
	require.Equal(t, []call{
		{groupID: groupID, invalidation: true},
		{groupID: groupID, check: true},
	}, enforcer.calls)

	cfgRsp, err := svc.GetIPRuleConfig(authCtx, &irpb.GetRulesConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
	})
	require.NoError(t, err)
	require.False(t, cfgRsp.GetEnforceIpRules())
}

func TestDeleteRuleRejectsLockout(t *testing.T) {
	enforcer := &fakeIPRulesEnforcer{}
	sns := &fakeServerNotificationService{}
	svc, env, authCtx, groupID := setupService(t, enforcer, sns)

	addRsp, err := svc.AddRule(authCtx, &irpb.AddRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		Rule: &irpb.IPRule{
			Cidr:        "1.2.3.4/32",
			Description: "rule",
		},
	})
	require.NoError(t, err)

	setGroupEnforcement(t, authCtx, env, groupID, true)
	enforcer.checkErr = status.PermissionDeniedError("blocked")
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "1.2.3.4")

	_, err = svc.DeleteRule(authCtx, &irpb.DeleteRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		IpRuleId:       addRsp.GetRule().GetIpRuleId(),
	})
	require.Error(t, err)
	require.True(t, status.IsInvalidArgumentError(err))
	require.Contains(t, err.Error(), "1.2.3.4")
	require.Equal(t, []call{
		{
			groupID:      groupID,
			invalidation: true,
		},
		{
			groupID:    groupID,
			check:      true,
			skipRuleID: addRsp.GetRule().GetIpRuleId(),
		}}, enforcer.calls)

	_, err = svc.GetRule(authCtx, groupID, addRsp.GetRule().GetIpRuleId())
	require.NoError(t, err)
	requireInvalidationMessages(t, sns, groupID)
}
