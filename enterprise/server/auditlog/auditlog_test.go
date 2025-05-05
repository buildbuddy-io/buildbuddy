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

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

func checkGroupUpdate(t *testing.T, e *alpb.Entry, request *grpb.UpdateGroupRequest) {
	require.Equal(t, alpb.ResourceType_GROUP, e.GetResource().GetType())
	require.Equal(t, alpb.Action_UPDATE, e.GetAction())
	require.Empty(t, cmp.Diff(request, e.GetRequest().GetApiRequest().UpdateGroup, protocmp.Transform()))
}

func TestGetLogs(t *testing.T) {
	flags.Set(t, "app.audit_logs_enabled", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "testenv.use_clickhouse", true)

	userID1 := "US1"
	groupID1 := "GR1"
	groupID2 := "GR2"
	groupID3 := "GR3"

	ctx := context.Background()
	env := enterprise_testenv.New(t)

	err := env.GetUserDB().InsertUser(ctx, &tables.User{UserID: userID1, SubID: userID1})
	require.NoError(t, err)

	// user1 is an admin in both group1 and group2.
	u1 := &testauth.TestUser{
		UserID:  userID1,
		GroupID: groupID1,
		GroupMemberships: []*interfaces.GroupMembership{
			{GroupID: groupID1, Capabilities: []akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY}},
			{GroupID: groupID2, Capabilities: []akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY}},
		},
	}

	testAuth := testauth.NewTestAuthenticator(map[string]interfaces.UserInfo{userID1: u1})
	env.SetAuthenticator(testAuth)

	err = auditlog.Register(env)
	require.NoError(t, err)

	user1Ctx, err := testAuth.WithAuthenticatedUser(ctx, userID1)
	require.NoError(t, err)

	// Add audit log events for all groups.
	gr1Update := &grpb.UpdateGroupRequest{Name: "group1"}
	env.GetAuditLogger().LogForGroup(user1Ctx, groupID1, alpb.Action_UPDATE, gr1Update)
	gr2Update := &grpb.UpdateGroupRequest{Name: "group2"}
	env.GetAuditLogger().LogForGroup(user1Ctx, groupID2, alpb.Action_UPDATE, gr2Update)
	gr3Update := &grpb.UpdateGroupRequest{Name: "group3"}
	env.GetAuditLogger().LogForGroup(user1Ctx, groupID3, alpb.Action_UPDATE, gr3Update)

	// User1 should be able to retrieve logs for group 1.
	rsp, err := env.GetAuditLogger().GetLogs(user1Ctx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: groupID1},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)
	require.Len(t, rsp.Entries, 1)
	checkGroupUpdate(t, rsp.Entries[0], gr1Update)

	// User1 should be able to retrieve logs for group 2 as well.
	rsp, err = env.GetAuditLogger().GetLogs(user1Ctx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: groupID2},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)
	require.Len(t, rsp.Entries, 1)
	checkGroupUpdate(t, rsp.Entries[0], gr2Update)

	// User1 should not be able to retrieve logs for group 3.
	_, err = env.GetAuditLogger().GetLogs(user1Ctx, &alpb.GetAuditLogsRequest{
		RequestContext:  &ctxpb.RequestContext{GroupId: groupID3},
		TimestampAfter:  timestamppb.New(time.Time{}),
		TimestampBefore: timestamppb.New(time.Now()),
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}
