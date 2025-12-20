package auditlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

func TestGetLogs(t *testing.T) {
	flags.Set(t, "app.audit_logs_enabled", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "testenv.use_clickhouse", true)

	group1AdminID := "US1"
	group1AdminFirstName := "FirstName"
	group1AdminLastName := "LastName"
	group1AuditorID := "US2"
	group1CacheUserID := "US3"
	group1ID := "GR1"

	group2AdminID := "US777"
	group2ID := "GR2"

	//groupID2 := "GR2"

	ctx := context.Background()
	env := enterprise_testenv.New(t)

	err := env.GetUserDB().InsertUser(ctx, &tables.User{
		UserID:    group1AdminID,
		SubID:     group1AdminID,
		FirstName: group1AdminFirstName,
		LastName:  group1AdminLastName,
	})
	require.NoError(t, err)
	err = env.GetUserDB().InsertUser(ctx, &tables.User{
		UserID: group2AdminID,
		SubID:  group2AdminID,
	})
	require.NoError(t, err)

	adminCaps := []cappb.Capability{cappb.Capability_ORG_ADMIN}
	adminUser := &testauth.TestUser{
		UserID:       group1AdminID,
		GroupID:      group1ID,
		Capabilities: adminCaps,
		GroupMemberships: []*interfaces.GroupMembership{
			{GroupID: group1ID, Capabilities: adminCaps},
		},
	}
	auditCaps := []cappb.Capability{cappb.Capability_AUDIT_LOG_READ}
	auditUser := &testauth.TestUser{
		UserID:       group1AdminID,
		GroupID:      group1ID,
		Capabilities: auditCaps,
		GroupMemberships: []*interfaces.GroupMembership{
			{GroupID: group1ID, Capabilities: auditCaps},
		},
	}
	cacheCaps := []cappb.Capability{cappb.Capability_CACHE_WRITE}
	cacheUser := &testauth.TestUser{
		UserID:  group1AdminID,
		GroupID: group1ID,
		GroupMemberships: []*interfaces.GroupMembership{
			{GroupID: group1ID, Capabilities: cacheCaps},
		},
	}
	group2Admin := &testauth.TestUser{
		UserID:       group2AdminID,
		GroupID:      group2ID,
		Capabilities: adminCaps,
		GroupMemberships: []*interfaces.GroupMembership{
			{GroupID: group1ID, Capabilities: adminCaps},
		},
	}

	testAuth := testauth.NewTestAuthenticator(map[string]interfaces.UserInfo{
		group1AdminID:     adminUser,
		group1AuditorID:   auditUser,
		group1CacheUserID: cacheUser,
		group2AdminID:     group2Admin,
	})
	env.SetAuthenticator(testAuth)

	err = auditlog.Register(env)
	require.NoError(t, err)

	adminCtx, err := testAuth.WithAuthenticatedUser(ctx, group1AdminID)
	require.NoError(t, err)

	// Add some events for the first group.
	groupUpdate := &grpb.UpdateGroupRequest{Name: "group1"}
	env.GetAuditLogger().LogForGroup(adminCtx, group1ID, alpb.Action_UPDATE, groupUpdate)
	groupUsersUpdate := &grpb.UpdateGroupUsersRequest{
		Update: []*grpb.UpdateGroupUsersRequest_Update{
			{
				UserId:           &uidpb.UserId{Id: group1AdminID},
				MembershipAction: grpb.UpdateGroupUsersRequest_Update_ADD,
				Role:             grpb.Group_DEVELOPER_ROLE,
			},
		},
	}
	env.GetAuditLogger().LogForGroup(adminCtx, group1ID, alpb.Action_UPDATE, groupUsersUpdate)

	// Add an event for another group for good measure.
	group2AdminCtx, err := testAuth.WithAuthenticatedUser(ctx, group2AdminID)
	require.NoError(t, err)
	env.GetAuditLogger().LogForGroup(group2AdminCtx, group2ID, alpb.Action_UPDATE, groupUpdate)

	// These are the expected entries when querying group 1.
	expected := []*alpb.Entry{
		{
			AuthenticationInfo: &alpb.AuthenticationInfo{User: &alpb.AuthenticatedUser{UserId: group1AdminID}},
			Resource:           &alpb.ResourceID{Type: alpb.ResourceType_GROUP},
			Action:             alpb.Action_UPDATE,
			Request:            &alpb.Entry_Request{ApiRequest: &alpb.Entry_APIRequest{UpdateGroup: groupUpdate}},
		},
		{
			AuthenticationInfo: &alpb.AuthenticationInfo{User: &alpb.AuthenticatedUser{UserId: group1AdminID}},
			Resource:           &alpb.ResourceID{Type: alpb.ResourceType_GROUP},
			Action:             alpb.Action_UPDATE,
			Request: &alpb.Entry_Request{
				ApiRequest:    &alpb.Entry_APIRequest{UpdateGroupUsers: groupUsersUpdate},
				IdDescriptors: []*alpb.Entry_Request_IDDescriptor{{Id: "US1", Value: group1AdminFirstName + " " + group1AdminLastName}},
			},
		},
	}
	// Group1 admin should be able to get the logs for group1.
	rsp, err := env.GetAuditLogger().GetLogs(adminCtx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: group1ID},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(expected, rsp.GetEntries(), protocmp.Transform(), protocmp.IgnoreFields(&alpb.Entry{}, "event_time")))

	// Group1 auditor should be able to get the logs for group1.
	auditorCtx, err := testAuth.WithAuthenticatedUser(ctx, group1AuditorID)
	require.NoError(t, err)
	rsp, err = env.GetAuditLogger().GetLogs(auditorCtx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: group1ID},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(expected, rsp.GetEntries(), protocmp.Transform(), protocmp.IgnoreFields(&alpb.Entry{}, "event_time")))

	// Group1 cache user should not be able to get the logs for group1.
	cacheUserCtx, err := testAuth.WithAuthenticatedUser(ctx, group1CacheUserID)
	require.NoError(t, err)
	_, err = env.GetAuditLogger().GetLogs(cacheUserCtx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: group1ID},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}

func newFakeUser(userID, domain string) *tables.User {
	return &tables.User{
		UserID:    userID,
		SubID:     userID + "-SubID",
		FirstName: userID + "-FirstName",
		LastName:  userID + "-LastName",
		Email:     userID + "@" + domain,
	}
}

func createUser(t *testing.T, ctx context.Context, env environment.Env, userID, domain string) *tables.User {
	user := newFakeUser(userID, domain)
	err := env.GetUserDB().InsertUser(ctx, user)
	require.NoError(t, err)
	return user
}

func authUserCtx(ctx context.Context, env environment.Env, t *testing.T, userID string) context.Context {
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	ctx, err := auth.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	return ctx
}

func getGroup(t *testing.T, ctx context.Context, env environment.Env) *tables.GroupRole {
	tu, err := env.GetUserDB().GetUser(ctx)
	require.NoError(t, err, "failed to get self-owned group")
	require.Len(t, tu.Groups, 1, "getGroup: user must be part of exactly one group")
	return tu.Groups[0]
}

func TestChildGroupAuth(t *testing.T) {
	flags.Set(t, "auth.api_key_group_cache_ttl", 0)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	flags.Set(t, "app.audit_logs_enabled", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "testenv.use_clickhouse", true)
	env := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, env)
	err := auditlog.Register(env)
	require.NoError(t, err)
	udb := env.GetUserDB()
	ctx := context.Background()

	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	auth.APIKeyProvider = func(ctx context.Context, apiKey string) (interfaces.UserInfo, error) {
		ui, err := env.GetAuthDB().GetAPIKeyGroupFromAPIKey(ctx, apiKey)
		require.NoError(t, err)
		return claims.APIKeyGroupClaims(ctx, ui)
	}

	// Start with two independent groups.
	createUser(t, ctx, env, "US1", "org1.io")
	ctx1 := authUserCtx(ctx, env, t, "US1")
	us1Group := getGroup(t, ctx1, env).Group
	grp1AuditKey, err := env.GetAuthDB().CreateAPIKey(
		ctx1, us1Group.GroupID, "audit",
		[]cappb.Capability{cappb.Capability_AUDIT_LOG_READ},
		0, /*=expiresIn*/
		false /*=visibleToDevelopers*/)
	require.NoError(t, err)
	group1AuditorCtx := env.GetAuthenticator().AuthContextFromAPIKey(ctx, grp1AuditKey.Value)

	createUser(t, ctx, env, "US2", "org2.io")
	ctx2 := authUserCtx(ctx, env, t, "US2")
	us2Group := getGroup(t, ctx2, env).Group
	grp2AuditKey, err := env.GetAuthDB().CreateAPIKey(
		ctx2, us2Group.GroupID, "audit",
		[]cappb.Capability{cappb.Capability_AUDIT_LOG_READ},
		0, /*=expiresIn*/
		false /*=visibleToDevelopers*/)
	require.NoError(t, err)

	// Key for group1 shouldn't be able to query anything in group2.
	errCtx := env.GetAuthenticator().AuthContextFromAPIKey(
		requestcontext.ContextWithProtoRequestContext(
			ctx, &ctxpb.RequestContext{GroupId: us2Group.GroupID}), grp1AuditKey.Value)
	err, _ = authutil.AuthErrorFromContext(errCtx)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))

	// Key for group1 should be able to query logs for group1.
	_, err = env.GetAuditLogger().GetLogs(group1AuditorCtx, &alpb.GetAuditLogsRequest{
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.NoError(t, err, "should be able to query logs for group1")

	// Update the groups to have matching SAML URLs.

	us1Group.SamlIdpMetadataUrl = "https://some/saml/url"
	us1Group.URLIdentifier = "org1"
	_, err = udb.UpdateGroup(ctx1, &us1Group)
	require.NoError(t, err)
	us2Group.SamlIdpMetadataUrl = us1Group.SamlIdpMetadataUrl
	us2Group.URLIdentifier = "org2"
	_, err = udb.UpdateGroup(ctx2, &us2Group)
	require.NoError(t, err)

	// Re-auth and try again. Key for group1 should still not be able to query the
	// second group.
	errCtx = env.GetAuthenticator().AuthContextFromAPIKey(
		requestcontext.ContextWithProtoRequestContext(
			ctx, &ctxpb.RequestContext{GroupId: us2Group.GroupID}), grp1AuditKey.Value)
	err, _ = authutil.AuthErrorFromContext(errCtx)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))

	// Now mark the first group as a "parent" organization.
	// Since they share the same SAML IDP Metadata URL, an audit log key
	// from the first group should be able to query the child group, but
	// not vice versa.
	us1Group.IsParent = true
	_, err = udb.UpdateGroup(ctx1, &us1Group)
	require.NoError(t, err)
	group1AuditorCtx = env.GetAuthenticator().AuthContextFromAPIKey(
		requestcontext.ContextWithProtoRequestContext(
			ctx, &ctxpb.RequestContext{GroupId: us2Group.GroupID}), grp1AuditKey.Value)
	err, _ = authutil.AuthErrorFromContext(group1AuditorCtx)
	require.NoError(t, err)
	_, err = env.GetAuditLogger().GetLogs(group1AuditorCtx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: us2Group.GroupID},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.NoError(t, err, "should be able to query logs for group2")

	errCtx = env.GetAuthenticator().AuthContextFromAPIKey(
		requestcontext.ContextWithProtoRequestContext(
			ctx, &ctxpb.RequestContext{GroupId: us1Group.GroupID}), grp2AuditKey.Value)
	err, _ = authutil.AuthErrorFromContext(errCtx)
	require.Error(t, err, "should not be able to query audit logs of group1")
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestFilterEntry_RedactsBuildBuddyUsers(t *testing.T) {
	flags.Set(t, "app.audit_logs_enabled", true)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "testenv.use_clickhouse", true)
	env := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, env)
	err := auditlog.Register(env)
	require.NoError(t, err)

	ctx := context.Background()

	// Create a buildbuddy.io user.
	bbUser := createUser(t, ctx, env, "BB1", "buildbuddy.io")
	bbCtx := authUserCtx(ctx, env, t, bbUser.UserID)
	bbGroup := getGroup(t, bbCtx, env).Group

	// Create an external user.
	extUser := createUser(t, ctx, env, "EXT1", "example.com")
	extCtx := authUserCtx(ctx, env, t, extUser.UserID)
	extGroup := getGroup(t, extCtx, env).Group

	// Log an event for each user.
	groupUpdate := &grpb.UpdateGroupRequest{Name: "test-group"}
	env.GetAuditLogger().LogForGroup(bbCtx, bbGroup.GroupID, alpb.Action_UPDATE, groupUpdate)
	env.GetAuditLogger().LogForGroup(extCtx, extGroup.GroupID, alpb.Action_UPDATE, groupUpdate)

	// Retrieve logs for buildbuddy user - should be redacted.
	bbResp, err := env.GetAuditLogger().GetLogs(bbCtx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: bbGroup.GroupID},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)
	require.Len(t, bbResp.Entries, 1)
	require.Equal(t, "Buildbuddy Admin", bbResp.Entries[0].AuthenticationInfo.User.UserEmail)
	require.Equal(t, "0.0.0.0", bbResp.Entries[0].AuthenticationInfo.ClientIp)

	// Retrieve logs for external user - should NOT be redacted.
	extResp, err := env.GetAuditLogger().GetLogs(extCtx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: extGroup.GroupID},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)
	require.Len(t, extResp.Entries, 1)
	require.Equal(t, extUser.Email, extResp.Entries[0].AuthenticationInfo.User.UserEmail)
	// Client IP should not be "0.0.0.0" for external users (it may be empty in test environment).
	require.NotEqual(t, "0.0.0.0", extResp.Entries[0].AuthenticationInfo.ClientIp)
}
