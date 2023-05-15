package scheduler_server

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/service"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"

	"github.com/stretchr/testify/require"
)

func getScheduleServer(t *testing.T, userOwnedEnabled, groupOwnedEnabled, workflowsEnabled bool, user string) (*SchedulerServer, context.Context) {
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{})

	flags.Set(t, "remote_execution.default_pool_name", "defaultPoolName")
	flags.Set(t, "remote_execution.shared_executor_pool_group_id", "sharedGroupID")

	s := &SchedulerServer{env: env}

	testUsers := make(map[string]interfaces.UserInfo, 0)
	testUsers["user1"] = &testauth.TestUser{UserID: "user1", GroupID: "group1", UseGroupOwnedExecutors: groupOwnedEnabled}

	ta := testauth.NewTestAuthenticator(testUsers)
	env.SetAuthenticator(ta)
	s.enableUserOwnedExecutors = userOwnedEnabled

	if workflowsEnabled {
		flags.Set(t, "remote_execution.workflows_pool_name", "workflows")
		env.SetWorkflowService(service.NewWorkflowService(env))
	}

	ctx := context.Background()
	if user != "" {
		authenticatedCtx, err := ta.WithAuthenticatedUser(context.Background(), user)
		require.NoError(t, err)
		ctx = authenticatedCtx
	}
	return s, ctx
}

func TestGetPoolInfo_UserOwnedDisabled(t *testing.T) {
	s, ctx := getScheduleServer(t, false, false, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestGetPoolInfo_NoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestGetPoolInfo_WithOS(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, false, "user1")
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

func TestGetPoolInfo_WorkflowPoolWithAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, true, "user1")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "workflows", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}

func TestGetPoolInfo_WorkflowPoolWithNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, true, "")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "workflows", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}

func TestGetPoolInfo_InvalidSharedPool(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, false, "")
	_, err := s.GetPoolInfo(ctx, "linux", "nonexistent-pool", "" /*=workflowID*/, false)
	require.Error(t, err)
}

func TestGetPoolInfo_Darwin(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestGetPoolInfo_DarwinNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, false, "")
	s.forceUserOwnedDarwinExecutors = true
	_, err := s.GetPoolInfo(ctx, "darwin", "", "" /*=workflowID*/, false)
	require.Error(t, err)
}

func TestGetPoolInfo_SelfHostedNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, false, "")
	_, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.Error(t, err)
}

func TestGetPoolInfo_SelfHosted(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, true, "user1")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	// Linux workflows should respect useSelfHosted bool.
	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}

func TestGetPoolInfo_SelfHostedByDefault(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, true, "user1")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	// Linux workflows should respect useSelfHosted bool.
	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}
