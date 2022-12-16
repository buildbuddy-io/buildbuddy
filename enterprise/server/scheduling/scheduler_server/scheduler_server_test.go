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

func TestSchedulerServerGetPoolInfoUserOwnedDisabled(t *testing.T) {
	s, ctx := getScheduleServer(t, false, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoWithOS(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "user1")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	p, err = s.GetPoolInfo(ctx, "darwin", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	s.forceUserOwnedDarwinExecutors = true
	p, err = s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	p, err = s.GetPoolInfo(ctx, "darwin", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)
}

func TestSchedulerServerGetPoolInfoWithRequestedPoolWithAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, "user1")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "my-pool", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "my-pool", p.Name)
}

func TestSchedulerServerGetPoolInfoWithRequestedPoolWithNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "my-pool", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "my-pool", p.Name)
}

func TestSchedulerServerGetPoolInfoDarwin(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoDarwinNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	s.forceUserOwnedDarwinExecutors = true
	_, err := s.GetPoolInfo(ctx, "darwin", "", "" /*=workflowID*/, false)
	require.Error(t, err)
}

func TestSchedulerServerGetPoolInfoSelfHostedNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	_, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.Error(t, err)
}

func TestSchedulerServerGetPoolInfoSelfHosted(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "user1")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	// Linux workflows should still use the shared pool.
	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}

func TestSchedulerServerGetPoolInfoSelfHostedByDefault(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, "user1")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	// Linux workflows should still use the shared pool.
	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}
