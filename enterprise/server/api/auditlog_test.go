package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/http/protolet"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setAuditLogTestFlags(t *testing.T) {
	flags.Set(t, "app.audit_logs_enabled", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "auth.api_key_group_cache_ttl", 0)
}

func newAuditLogHTTPHandler(t *testing.T, env *testenv.TestEnv) http.Handler {
	t.Helper()
	testenv.RegisterLocalGRPCServer(t, env)
	handlers, err := protolet.GenerateHTTPHandlers("/api/v1/", "api.v1", NewAPIServer(env), env.GetGRPCServer())
	require.NoError(t, err)
	return interceptors.WrapAuthenticatedExternalProtoletHandler(env, "/api/v1/", handlers)
}

func parseGetAuditLogResponse(t *testing.T, body []byte) *apipb.GetAuditLogResponse {
	t.Helper()
	apiRsp := &apipb.GetAuditLogResponse{}
	require.NoError(t, protojson.Unmarshal(body, apiRsp))
	return apiRsp
}

type auditLogTestEnv struct {
	*testenv.TestEnv
	ctx  context.Context
	auth *testauth.TestAuthenticator
}

func newAuditLogTestEnv(t *testing.T) *auditLogTestEnv {
	t.Helper()
	setAuditLogTestFlags(t)

	env := enterprise_testenv.New(t)
	auth := enterprise_testauth.Configure(t, env)
	require.NoError(t, auditlog.Register(env))
	return &auditLogTestEnv{
		ctx:     context.Background(),
		TestEnv: env,
		auth:    auth,
	}
}

func (e *auditLogTestEnv) createUserGroup(t *testing.T, domain string) (context.Context, tables.Group) {
	t.Helper()
	user := enterprise_testauth.CreateRandomUser(t, e.TestEnv, domain)
	userCtx, err := e.auth.WithAuthenticatedUser(e.ctx, user.UserID)
	require.NoError(t, err)

	authUser, err := e.GetUserDB().GetUser(userCtx)
	require.NoError(t, err)
	require.Len(t, authUser.Groups, 1)
	return userCtx, authUser.Groups[0].Group
}

func (e *auditLogTestEnv) updateGroup(t *testing.T, userCtx context.Context, group tables.Group, update func(*tables.Group)) tables.Group {
	t.Helper()
	update(&group)
	_, err := e.GetUserDB().UpdateGroup(userCtx, &group)
	require.NoError(t, err)
	return group
}

func (e *auditLogTestEnv) createAuditReaderKey(t *testing.T, userCtx context.Context, groupID string) string {
	t.Helper()
	key, err := e.GetAuthDB().CreateAPIKey(
		userCtx,
		groupID,
		"audit-reader",
		[]cappb.Capability{cappb.Capability_AUDIT_LOG_READ},
		0,     /*=expiresIn*/
		false, /*=visibleToDevelopers*/
	)
	require.NoError(t, err)
	return key.Value
}

func (e *auditLogTestEnv) logGroupUpdate(t *testing.T, userCtx context.Context, groupID, name string) {
	t.Helper()
	e.GetAuditLogger().LogForGroup(userCtx, groupID, alpb.Action_UPDATE, &grpb.UpdateGroupRequest{Name: name})
}

type crossOrgAuditLogEnv struct {
	*auditLogTestEnv
	childCtx   context.Context
	childGroup tables.Group
	apiKey     string
	handler    http.Handler
}

func newCrossOrgAuditLogEnv(t *testing.T) *crossOrgAuditLogEnv {
	t.Helper()
	env := newAuditLogTestEnv(t)

	parentCtx, parentGroup := env.createUserGroup(t, "parent.test")
	parentGroup = env.updateGroup(t, parentCtx, parentGroup, func(g *tables.Group) {
		g.IsParent = true
		g.SamlIdpMetadataUrl = "https://idp.example.test/metadata"
		g.URLIdentifier = "parent-org"
	})

	childCtx, childGroup := env.createUserGroup(t, "child.test")
	childGroup = env.updateGroup(t, childCtx, childGroup, func(g *tables.Group) {
		g.SamlIdpMetadataUrl = parentGroup.SamlIdpMetadataUrl
		g.URLIdentifier = "child-org"
	})

	apiKey := env.createAuditReaderKey(t, parentCtx, parentGroup.GroupID)
	return &crossOrgAuditLogEnv{
		auditLogTestEnv: env,
		childCtx:        childCtx,
		childGroup:      childGroup,
		apiKey:          apiKey,
		handler:         newAuditLogHTTPHandler(t, env.TestEnv),
	}
}

func (e *crossOrgAuditLogEnv) postGetAuditLog(t *testing.T, req *apipb.GetAuditLogRequest) *httptest.ResponseRecorder {
	t.Helper()
	reqBody, err := protojson.Marshal(req)
	require.NoError(t, err)

	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/GetAuditLog", bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(authutil.APIKeyHeader, e.apiKey)
	rsp := httptest.NewRecorder()
	e.handler.ServeHTTP(rsp, httpReq)
	return rsp
}

func TestGetAuditLog(t *testing.T) {
	env := newAuditLogTestEnv(t)
	userCtx, group := env.createUserGroup(t, "example.io")
	keyCtx := env.GetAuthenticator().AuthContextFromAPIKey(env.ctx, env.createAuditReaderKey(t, userCtx, group.GroupID))

	// Seed two events inside the window and one just before it.
	env.logGroupUpdate(t, userCtx, group.GroupID, "before-window")
	time.Sleep(2 * time.Millisecond)
	windowStart := time.Now()
	time.Sleep(2 * time.Millisecond)
	env.logGroupUpdate(t, userCtx, group.GroupID, "update-1")
	env.logGroupUpdate(t, userCtx, group.GroupID, "update-2")

	// Fetch the first page pinned to the requested time window.
	s := NewAPIServer(env.TestEnv)
	page1, err := s.GetAuditLog(keyCtx, &apipb.GetAuditLogRequest{
		Selector: &apipb.AuditLogSelector{
			StartTime: timestamppb.New(windowStart),
		},
		PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, page1.GetEntry(), 1)
	require.NotEmpty(t, page1.GetNextPageToken())
	require.NotContains(t, page1.GetNextPageToken(), "|")

	// Add a later event and resume from the returned page token.
	time.Sleep(2 * time.Millisecond)
	env.logGroupUpdate(t, userCtx, group.GroupID, "update-3")

	page2, err := s.GetAuditLog(keyCtx, &apipb.GetAuditLogRequest{
		PageSize:  1,
		PageToken: page1.GetNextPageToken(),
	})
	require.NoError(t, err)
	require.Len(t, page2.GetEntry(), 1)
	require.Empty(t, page2.GetNextPageToken())

	name1 := page1.GetEntry()[0].GetRequest().GetApiRequest().GetUpdateGroup().GetName()
	name2 := page2.GetEntry()[0].GetRequest().GetApiRequest().GetUpdateGroup().GetName()
	require.NotEqual(t, name1, name2)
	require.ElementsMatch(t, []string{"update-1", "update-2"}, []string{name1, name2})
}

func TestParentAuditReaderCanQueryChildGroupViaRequestContext(t *testing.T) {
	e := newCrossOrgAuditLogEnv(t)

	// Seed a child-org event that the parent org API key should be able to read.
	e.logGroupUpdate(t, e.childCtx, e.childGroup.GroupID, "child-update")

	// Explicitly select the child org via request_context.group_id.
	rsp := e.postGetAuditLog(t, &apipb.GetAuditLogRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: e.childGroup.GroupID},
	})
	require.Equal(t, http.StatusOK, rsp.Code, rsp.Body.String())

	apiRsp := parseGetAuditLogResponse(t, rsp.Body.Bytes())
	require.Len(t, apiRsp.GetEntry(), 1)
	require.Equal(t, "child-update", apiRsp.GetEntry()[0].GetRequest().GetApiRequest().GetUpdateGroup().GetName())
}

func TestParentAuditReaderPageTokenWithoutRequestContextFails(t *testing.T) {
	e := newCrossOrgAuditLogEnv(t)

	// Seed enough child-org data to require a second page.
	e.logGroupUpdate(t, e.childCtx, e.childGroup.GroupID, "child-update-1")
	time.Sleep(2 * time.Millisecond)
	e.logGroupUpdate(t, e.childCtx, e.childGroup.GroupID, "child-update-2")

	// Fetch page 1 while explicitly selecting the child org.
	page1Rsp := e.postGetAuditLog(t, &apipb.GetAuditLogRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: e.childGroup.GroupID},
		PageSize:       1,
	})
	require.Equal(t, http.StatusOK, page1Rsp.Code, page1Rsp.Body.String())

	page1 := parseGetAuditLogResponse(t, page1Rsp.Body.Bytes())
	require.Len(t, page1.GetEntry(), 1)
	require.NotEmpty(t, page1.GetNextPageToken())

	// Resume with only the page token; the request falls back to the parent org.
	page2Rsp := e.postGetAuditLog(t, &apipb.GetAuditLogRequest{
		PageToken: page1.GetNextPageToken(),
	})
	require.NotEqual(t, http.StatusOK, page2Rsp.Code, page2Rsp.Body.String())
	require.Contains(t, page2Rsp.Body.String(), "page_token does not match selected group")
}
