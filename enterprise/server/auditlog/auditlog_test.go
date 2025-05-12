package auditlog_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
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
