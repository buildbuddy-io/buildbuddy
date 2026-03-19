package api

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGetAuditLog(t *testing.T) {
	flags.Set(t, "app.audit_logs_enabled", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "testenv.use_clickhouse", true)

	ctx := context.Background()
	env := enterprise_testenv.New(t)
	auth := enterprise_testauth.Configure(t, env)
	require.NoError(t, auditlog.Register(env))

	user := enterprise_testauth.CreateRandomUser(t, env, "example.io")
	userCtx, err := auth.WithAuthenticatedUser(ctx, user.UserID)
	require.NoError(t, err)
	authUser, err := env.GetUserDB().GetUser(userCtx)
	require.NoError(t, err)
	require.NotEmpty(t, authUser.Groups)
	groupID := authUser.Groups[0].Group.GroupID

	key, err := env.GetAuthDB().CreateAPIKey(
		userCtx,
		groupID,
		"audit-reader",
		[]cappb.Capability{cappb.Capability_AUDIT_LOG_READ},
		0,     /*=expiresIn*/
		false, /*=visibleToDevelopers*/
	)
	require.NoError(t, err)
	keyCtx := env.GetAuthenticator().AuthContextFromAPIKey(ctx, key.Value)

	env.GetAuditLogger().LogForGroup(userCtx, groupID, alpb.Action_UPDATE, &grpb.UpdateGroupRequest{Name: "before-window"})
	time.Sleep(2 * time.Millisecond)
	windowStart := time.Now()
	time.Sleep(2 * time.Millisecond)
	env.GetAuditLogger().LogForGroup(userCtx, groupID, alpb.Action_UPDATE, &grpb.UpdateGroupRequest{Name: "update-1"})
	env.GetAuditLogger().LogForGroup(userCtx, groupID, alpb.Action_UPDATE, &grpb.UpdateGroupRequest{Name: "update-2"})

	s := NewAPIServer(env)
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

	time.Sleep(2 * time.Millisecond)
	env.GetAuditLogger().LogForGroup(userCtx, groupID, alpb.Action_UPDATE, &grpb.UpdateGroupRequest{Name: "update-3"})

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
