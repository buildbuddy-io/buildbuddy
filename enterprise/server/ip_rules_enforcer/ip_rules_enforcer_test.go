package ip_rules_enforcer_test

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/ip_rules_enforcer"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	irpb "github.com/buildbuddy-io/buildbuddy/proto/iprules"
)

type fakeServerNotificationService struct {
	ch chan proto.Message
}

func (f *fakeServerNotificationService) Subscribe(msgType proto.Message) <-chan proto.Message {
	return f.ch
}

func (f *fakeServerNotificationService) Publish(ctx context.Context, msg proto.Message) error {
	return nil
}

type fakeIPRulesService struct {
	rsp *irpb.GetRulesResponse
}

func (s *fakeIPRulesService) GetIPRules(ctx context.Context, req *irpb.GetRulesRequest) (*irpb.GetRulesResponse, error) {
	return s.rsp, nil
}

func startRemoteIPRulesServer(t *testing.T, env environment.Env, svc *fakeIPRulesService) string {
	t.Helper()

	server := grpc.NewServer()
	irpb.RegisterIPRulesServiceServer(server, svc)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	go func() {
		_ = server.Serve(lis)
	}()
	return "grpc://" + lis.Addr().String()
}

func newIPRulesEnforcer(t *testing.T, env environment.Env) *ip_rules_enforcer.Enforcer {
	t.Helper()

	flags.Set(t, "auth.ip_rules.enable", true)
	flags.Set(t, "auth.ip_rules.cache_ttl", 0)

	s, err := ip_rules_enforcer.New(env)
	require.NoError(t, err)
	return s
}

func getEnv(t *testing.T) *testenv.TestEnv {
	t.Helper()

	env := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, env)
	return env
}

func setupAuthenticatedUser(t *testing.T, env *testenv.TestEnv) (context.Context, string, string) {
	t.Helper()

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
	return authCtx, u.UserID, groupID
}

func setGroupEnforcement(t *testing.T, env *testenv.TestEnv, ctx context.Context, groupID string, enabled bool) {
	t.Helper()

	g, err := env.GetUserDB().GetGroupByID(ctx, groupID)
	require.NoError(t, err)
	g.EnforceIPRules = enabled
	_, err = env.GetUserDB().UpdateGroup(ctx, g)
	require.NoError(t, err)
}

func reauthenticate(t *testing.T, env *testenv.TestEnv, userID string) context.Context {
	t.Helper()

	authCtx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(context.Background(), userID)
	require.NoError(t, err)
	return authCtx
}

func insertRule(t *testing.T, env *testenv.TestEnv, groupID, cidr, description string) string {
	t.Helper()

	id, err := tables.PrimaryKeyForTable("IPRules")
	require.NoError(t, err)

	q := `INSERT INTO "IPRules" (created_at_usec, ip_rule_id, group_id, cidr, description) VALUES (?, ?, ?, ?, ?)`
	err = env.GetDBHandle().NewQuery(context.Background(), "test_insert_ip_rule").Raw(
		q, time.Now().UnixMicro(), id, groupID, cidr, description).Exec().Error
	require.NoError(t, err)
	return id
}

func contextWithClientIdentity(t *testing.T, ctx context.Context, service interfaces.ClientIdentityService) context.Context {
	t.Helper()

	ctx, err := service.AddIdentityToContext(ctx)
	require.NoError(t, err)
	outgoingMD, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	ctx = metadata.NewIncomingContext(ctx, outgoingMD)
	ctx, err = service.ValidateIncomingIdentity(ctx)
	require.NoError(t, err)
	return ctx
}

func TestNoOpEnforcer(t *testing.T) {
	flags.Set(t, "auth.ip_rules.enable", false)

	env := getEnv(t)
	err := ip_rules_enforcer.Register(env)
	require.NoError(t, err)

	enforcer := env.GetIPRulesEnforcer()
	require.IsType(t, &ip_rules_enforcer.NoOpEnforcer{}, enforcer)
	require.NoError(t, enforcer.Authorize(context.Background()))
	require.NoError(t, enforcer.AuthorizeGroup(context.Background(), "G1"))
	require.NoError(t, enforcer.AuthorizeHTTPRequest(context.Background(), httptest.NewRequest("GET", "/rpc/BuildBuddyService/GetUser", nil)))
	require.NoError(t, enforcer.Check(context.Background(), "G1", ""))
}

func TestAuthorizeAndAuthorizeGroup_EnforcementNotEnabled(t *testing.T) {
	env := getEnv(t)
	irs := newIPRulesEnforcer(t, env)
	authCtx, _, groupID := setupAuthenticatedUser(t, env)

	err := irs.Authorize(authCtx)
	require.NoError(t, err)

	err = irs.AuthorizeGroup(authCtx, groupID)
	require.NoError(t, err)
}

func TestAuthorize_UnauthenticatedBypasses(t *testing.T) {
	env := getEnv(t)
	irs := newIPRulesEnforcer(t, env)

	err := irs.Authorize(context.Background())
	require.NoError(t, err)

	ctx := authutil.AuthContextWithError(context.Background(), status.UnauthenticatedError("Invalid API Key"))
	err = irs.Authorize(ctx)
	require.NoError(t, err)
}

func TestAuthorizeAndAuthorizeGroup_Enforcement(t *testing.T) {
	env := getEnv(t)
	irs := newIPRulesEnforcer(t, env)
	authCtx, userID, groupID := setupAuthenticatedUser(t, env)

	insertRule(t, env, groupID, "1.2.3.0/24", "rule1")
	insertRule(t, env, groupID, "4.5.6.7/32", "rule2")

	err := irs.Authorize(authCtx)
	require.NoError(t, err)

	setGroupEnforcement(t, env, authCtx, groupID, true)
	authCtx = reauthenticate(t, env, userID)

	err = irs.Authorize(authCtx)
	require.Error(t, err)
	require.True(t, status.IsFailedPreconditionError(err))

	matchingCtx := context.WithValue(authCtx, clientip.ContextKey, "1.2.3.15")
	err = irs.Authorize(matchingCtx)
	require.NoError(t, err)

	exactCtx := context.WithValue(authCtx, clientip.ContextKey, "4.5.6.7")
	err = irs.Authorize(exactCtx)
	require.NoError(t, err)

	nonMatchingCtx := context.WithValue(authCtx, clientip.ContextKey, "5.6.7.8")
	err = irs.Authorize(nonMatchingCtx)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))

	err = irs.AuthorizeGroup(matchingCtx, groupID)
	require.NoError(t, err)

	err = irs.AuthorizeGroup(nonMatchingCtx, groupID)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestCheckSkipRuleID(t *testing.T) {
	env := getEnv(t)
	irs := newIPRulesEnforcer(t, env)
	_, _, groupID := setupAuthenticatedUser(t, env)

	ruleID := insertRule(t, env, groupID, "1.2.3.4/32", "rule1")
	ctx := context.WithValue(context.Background(), clientip.ContextKey, "1.2.3.4")

	err := irs.Check(ctx, groupID, "" /* skipRuleID */)
	require.NoError(t, err)

	err = irs.Check(ctx, groupID, ruleID)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestAuthorizeHTTPRequest(t *testing.T) {
	env := getEnv(t)
	irs := newIPRulesEnforcer(t, env)
	authCtx, userID, groupID := setupAuthenticatedUser(t, env)

	insertRule(t, env, groupID, "1.2.3.4/32", "rule1")
	setGroupEnforcement(t, env, authCtx, groupID, true)
	authCtx = reauthenticate(t, env, userID)

	err := irs.AuthorizeHTTPRequest(authCtx, httptest.NewRequest("GET", "http://example/rpc/BuildBuddyService/GetUser", nil))
	require.NoError(t, err)

	err = irs.AuthorizeHTTPRequest(authCtx, httptest.NewRequest("GET", "http://example/rpc/BuildBuddyService/GetGroup", nil))
	require.NoError(t, err)

	err = irs.AuthorizeHTTPRequest(authCtx, httptest.NewRequest("GET", "http://example/non-api", nil))
	require.NoError(t, err)

	err = irs.AuthorizeHTTPRequest(authCtx, httptest.NewRequest("GET", "http://example/rpc/BuildBuddyService/SearchInvocation", nil))
	require.Error(t, err)
	require.True(t, status.IsFailedPreconditionError(err))

	err = irs.AuthorizeHTTPRequest(authCtx, httptest.NewRequest("GET", "http://example/api/v1/invocation", nil))
	require.Error(t, err)
	require.True(t, status.IsFailedPreconditionError(err))
}

func TestAuthorize_TrustedClientIdentityBypasses(t *testing.T) {
	env := getEnv(t)
	enterprise_testenv.AddClientIdentity(t, env, interfaces.ClientIdentityApp)

	irs := newIPRulesEnforcer(t, env)
	authCtx, userID, groupID := setupAuthenticatedUser(t, env)

	insertRule(t, env, groupID, "1.2.3.4/32", "rule1")
	setGroupEnforcement(t, env, authCtx, groupID, true)
	authCtx = reauthenticate(t, env, userID)
	authCtx = contextWithClientIdentity(t, authCtx, env.GetClientIdentityService())
	authCtx = context.WithValue(authCtx, clientip.ContextKey, "5.6.7.8")

	err := irs.Authorize(authCtx)
	require.NoError(t, err)
}

func TestRemoteIPRulesEnforced(t *testing.T) {
	env := getEnv(t)
	svc := &fakeIPRulesService{
		rsp: &irpb.GetRulesResponse{
			IpRules: []*irpb.IPRule{
				{IpRuleId: "rule-1", Cidr: "1.2.3.0/24"},
				{IpRuleId: "bad-rule", Cidr: "not-a-cidr"},
				{IpRuleId: "rule-2", Cidr: "4.5.6.7/32"},
			},
		},
	}
	flags.Set(t, "auth.ip_rules.remote.target", startRemoteIPRulesServer(t, env, svc))

	irs, err := ip_rules_enforcer.New(env)
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(
		context.WithValue(context.Background(), clientip.ContextKey, "1.2.3.4"),
		metadata.Pairs("x-buildbuddy-api-key", "AK123"),
	)
	err = irs.Check(ctx, "GR1", "")
	require.NoError(t, err)

	ctx = context.WithValue(context.Background(), clientip.ContextKey, "4.5.6.7")
	err = irs.Check(ctx, "GR1", "")
	require.NoError(t, err)

	ctx = context.WithValue(context.Background(), clientip.ContextKey, "8.8.8.8")
	err = irs.Check(ctx, "GR1", "")
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestRefresherStopsOnShutdown(t *testing.T) {
	env := getEnv(t)

	// Install a noop server notification service to ensure the refresher runs.
	env.SetServerNotificationService(&fakeServerNotificationService{
		ch: make(chan proto.Message),
	})

	// The env starts some goroutines that aren't cleaned up. Ignore them.
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	_ = newIPRulesEnforcer(t, env)

	env.GetHealthChecker().Shutdown()
	env.GetHealthChecker().WaitForGracefulShutdown()
}
