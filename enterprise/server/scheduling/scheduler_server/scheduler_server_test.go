package scheduler_server

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"

	"github.com/stretchr/testify/require"
)

func getScheduleServer(t *testing.T, userOwnedEnabled, groupOwnedEnabled bool, user string) (*SchedulerServer, context.Context) {
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{})

	flags.Set(t, "remote_execution.default_pool_name", "defaultPoolName")
	flags.Set(t, "remote_execution.shared_executor_pool_group_id", "sharedGroupID")

	s := &SchedulerServer{env: env}

	testUsers := make(map[string]interfaces.UserInfo, 0)
	testUsers["user1"] = &testauth.TestUser{UserID: "user1", GroupID: "group1", UseGroupOwnedExecutors: groupOwnedEnabled}

	ta := testauth.NewTestAuthenticator(testUsers)
	env.SetAuthenticator(ta)
	s.enableUserOwnedExecutors = userOwnedEnabled

	ctx := context.Background()
	if user != "" {
		authenticatedCtx, err := ta.WithAuthenticatedUser(context.Background(), user)
		require.NoError(t, err)
		ctx = authenticatedCtx
	}
	return s, ctx
}

func TestSchedulerServerGetGroupIDAndDefaultPoolForUserUserOwnedDisabled(t *testing.T) {
	s, ctx := getScheduleServer(t, false, false, "")
	g, p, err := s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", false)
	require.NoError(t, err)
	require.Equal(t, "", g)
	require.Equal(t, "defaultPoolName", p)
}

func TestSchedulerServerGetGroupIDAndDefaultPoolForUserNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	g, p, err := s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", g)
	require.Equal(t, "defaultPoolName", p)
}

func TestSchedulerServerGetGroupIDAndDefaultPoolForUserWithOS(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "user1")
	s.forceUserOwnedDarwinExecutors = false
	g, p, err := s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", g)
	require.Equal(t, "defaultPoolName", p)

	g, p, err = s.GetGroupIDAndDefaultPoolForUser(ctx, "darwin", false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", g)
	require.Equal(t, "defaultPoolName", p)

	s.forceUserOwnedDarwinExecutors = true
	g, p, err = s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", g)
	require.Equal(t, "defaultPoolName", p)

	g, p, err = s.GetGroupIDAndDefaultPoolForUser(ctx, "darwin", false)
	require.NoError(t, err)
	require.Equal(t, "group1", g)
	require.Equal(t, "", p)
}

func TestSchedulerServerGetGroupIDAndDefaultPoolForUserDarwin(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	g, p, err := s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", g)
	require.Equal(t, "defaultPoolName", p)
}

func TestSchedulerServerGetGroupIDAndDefaultPoolForUserDarwinNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	s.forceUserOwnedDarwinExecutors = true
	_, _, err := s.GetGroupIDAndDefaultPoolForUser(ctx, "darwin", false)
	require.Error(t, err)
}

func TestSchedulerServerGetGroupIDAndDefaultPoolForUserSelfHostedNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	_, _, err := s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", true)
	require.Error(t, err)
}

func TestSchedulerServerGetGroupIDAndDefaultPoolForUserSelfHosted(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "user1")
	g, p, err := s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", true)
	require.NoError(t, err)
	require.Equal(t, "group1", g)
	require.Equal(t, "", p)
}

func TestSchedulerServerGetGroupIDAndDefaultPoolForUserSelfHostedByDefault(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, "user1")
	g, p, err := s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", false)
	require.NoError(t, err)
	require.Equal(t, "group1", g)
	require.Equal(t, "", p)

	g, p, err = s.GetGroupIDAndDefaultPoolForUser(ctx, "linux", true)
	require.NoError(t, err)
	require.Equal(t, "group1", g)
	require.Equal(t, "", p)
}
