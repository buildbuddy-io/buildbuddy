package iprules_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/iprules"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func newIPRulesService(t *testing.T, env environment.Env) *iprules.Service {
	flags.Set(t, "auth.ip_rules.enable", true)
	flags.Set(t, "auth.ip_rules.cache_ttl", 0)

	s, err := iprules.New(env)
	require.NoError(t, err)
	return s
}

func getEnv(t *testing.T) environment.Env {
	env := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, env)
	return env
}

func TestEnforcementNotEnabled(t *testing.T) {
	env := getEnv(t)
	ctx := context.Background()

	u := enterprise_testauth.CreateRandomUser(t, env, "org1.invalid")

	irs := newIPRulesService(t, env)
	auther := env.GetAuthenticator().(*testauth.TestAuthenticator)
	authCtx, err := auther.WithAuthenticatedUser(ctx, u.UserID)
	require.NoError(t, err)

	// The test group does not have IP rule enabled, so the check should pass.
	err = irs.Authorize(authCtx)
	require.NoError(t, err)

	// Checking by group ID should also pass.
	err = irs.AuthorizeGroup(ctx, u.Groups[0].Group.GroupID)
	require.NoError(t, err)
}

func TestEnforcement(t *testing.T) {
	env := getEnv(t)
	ctx := context.Background()

	u := enterprise_testauth.CreateRandomUser(t, env, "org1.invalid")
	g := u.Groups[0].Group
	groupID := g.GroupID

	irs := newIPRulesService(t, env)
	auther := env.GetAuthenticator().(*testauth.TestAuthenticator)
	authCtx, err := auther.WithAuthenticatedUser(ctx, u.UserID)
	require.NoError(t, err)

	_, err = irs.AddRule(authCtx, &irpb.AddRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		Rule:           &irpb.IPRule{Cidr: "1.2.3.0/24", Description: "rule1"},
	})
	require.NoError(t, err)
	rsp, err := irs.AddRule(authCtx, &irpb.AddRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		Rule:           &irpb.IPRule{Cidr: "4.5.6.7/32", Description: "rule2"},
	})
	require.NoError(t, err)
	rule2 := rsp.GetRule()

	// Rules not in effect yet, should pass.
	err = irs.Authorize(authCtx)
	require.NoError(t, err)

	// Enable IP rule enforcement.
	g.EnforceIPRules = true
	urlIdentifier := "foo"
	g.URLIdentifier = &urlIdentifier
	_, err = env.GetUserDB().InsertOrUpdateGroup(authCtx, &g)
	require.NoError(t, err)

	// Re-auth to pick up new group settings.
	authCtx, err = auther.WithAuthenticatedUser(ctx, u.UserID)
	require.NoError(t, err)

	// No IP in context, check should fail.
	err = irs.Authorize(authCtx)
	require.Error(t, err)
	require.True(t, status.IsFailedPreconditionError(err))

	// An IP in the middle of the first rule.
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "1.2.3.15")
	err = irs.Authorize(authCtx)
	require.NoError(t, err)

	// Exact match to second rule.
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "4.5.6.7")
	err = irs.Authorize(authCtx)
	require.NoError(t, err)

	// Non-matching IP.
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "5.6.7.8")
	err = irs.Authorize(authCtx)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))

	// Update the second rule to something else. Old rule should no longer
	// apply.
	rule2.Cidr = "8.8.8.8/32"
	rule2.Description = "updated rule2"
	_, err = irs.UpdateRule(authCtx, &irpb.UpdateRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		Rule:           rule2,
	})
	require.NoError(t, err)
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "4.5.6.7")
	err = irs.Authorize(authCtx)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))

	// New rule value should apply.
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "8.8.8.8")
	err = irs.Authorize(authCtx)
	require.NoError(t, err)

	// Delete rule2. Its value should no longer apply.
	_, err = irs.DeleteRule(authCtx, &irpb.DeleteRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupID},
		IpRuleId:       rule2.IpRuleId,
	})
	require.NoError(t, err)
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "8.8.8.8")
	err = irs.Authorize(authCtx)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}
