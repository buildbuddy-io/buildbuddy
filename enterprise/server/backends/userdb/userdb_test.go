package userdb_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	grp "github.com/buildbuddy-io/buildbuddy/proto/group"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func newTestEnv(t *testing.T) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers(
		"US1", "GR1",
		"US2", "GR2",
	)))
	udb, err := userdb.NewUserDB(te, te.GetDBHandle())
	require.NoError(t, err)
	te.SetUserDB(udb)
	return te
}

func authUserCtx(ctx context.Context, env *testenv.TestEnv, t *testing.T, userID string) context.Context {
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	ctx, err := auth.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	return ctx
}

func findGroupUser(t *testing.T, userID string, groupUsers []*grpb.GetGroupUsersResponse_GroupUser) *grpb.GetGroupUsersResponse_GroupUser {
	for _, user := range groupUsers {
		if user.User.UserId.Id == userID {
			return user
		}
	}
	require.FailNowf(t, "could not find group user", "user ID: %s", userID)
	return nil
}

func TestGetImpersonatedUser_UserWithoutImpersonationPerms_PermissionDenied(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create a user and an authenticated context for that user
	err := udb.InsertUser(ctx, &tables.User{
		UserID:    "US1",
		SubID:     "SubID1",
		FirstName: "FirstName1",
		LastName:  "LastName1",
		Email:     "user1@org1.io",
	})
	require.NoError(t, err)

	ctx1 := authUserCtx(ctx, env, t, "US1")

	// Now try to impersonate as that user; should fail since test users cannot
	// impersonate.
	u, err := udb.GetImpersonatedUser(ctx1)

	require.Nil(t, u)
	require.Error(t, err)
	require.Equal(t, status.Message(err), "Authenticated user does not have permissions to impersonate a user.")
}

func TestCreateUser_Cloud_CreatesSelfOwnedGroup(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	err := udb.InsertUser(ctx, &tables.User{
		UserID:    "US1",
		SubID:     "SubID1",
		FirstName: "FirstName1",
		LastName:  "LastName1",
		Email:     "user1@org1.io",
	})
	require.NoError(t, err)

	ctx1 := authUserCtx(ctx, env, t, "US1")

	u, err := udb.GetUser(ctx1)
	require.NoError(t, err)

	require.Len(t, u.Groups, 1, "cloud users should be added to their self-owned group")

	selfOwnedGroup := u.Groups[0].Group
	require.Equal(t, "US1", selfOwnedGroup.UserID, "user ID of self-owned group should be the owner's user ID")

	groupUsers, err := udb.GetGroupUsers(ctx1, selfOwnedGroup.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)

	require.Len(t, groupUsers, 1, "self-owned group should have 1 member")
	groupUser := groupUsers[0]

	require.Equal(t, grpb.Group_ADMIN_ROLE, groupUser.Role, "users should be admins of their self-owned group")
}

func TestCreateUser_OnPrem_OnlyFirstUserCreatedShouldBeMadeAdminOfDefaultGroup(t *testing.T) {
	flags.Set(t, "app.add_user_to_domain_group", false)
	flags.Set(t, "app.create_group_per_user", false)
	flags.Set(t, "app.no_default_user_group", false)
	env := newTestEnv(t)
	udb := env.GetUserDB()
	ctx := context.Background()

	err := udb.InsertUser(ctx, &tables.User{
		UserID:    "US1",
		SubID:     "SubID1",
		FirstName: "FirstName1",
		LastName:  "LastName1",
		Email:     "user1@org1.io",
	})
	require.NoError(t, err)
	err = udb.InsertUser(ctx, &tables.User{
		UserID:    "US2",
		SubID:     "SubID2",
		FirstName: "FirstName2",
		LastName:  "LastName2",
		Email:     "user2@org1.io",
	})
	require.NoError(t, err)

	ctx1 := authUserCtx(ctx, env, t, "US1")

	u, err := udb.GetUser(ctx1)
	require.NoError(t, err)

	require.Len(t, u.Groups, 1, "US1 should be added to the default group")

	defaultGroup := u.Groups[0].Group
	groupUsers, err := udb.GetGroupUsers(ctx1, defaultGroup.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)
	require.Len(t, groupUsers, 2, "default group should have 2 members")
	us1 := findGroupUser(t, "US1", groupUsers)
	require.Equal(t, grpb.Group_ADMIN_ROLE, us1.Role, "first user added to the default group should be made an admin")
	us2 := findGroupUser(t, "US2", groupUsers)
	require.Equal(t, grpb.Group_DEVELOPER_ROLE, us2.Role, "second user added to the default group should have the default role")
}

func TestAddUserToGroup_AddsUserWithDefaultRole(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create some users
	err := udb.InsertUser(ctx, &tables.User{
		UserID:    "US1",
		SubID:     "SubID1",
		FirstName: "FirstName1",
		LastName:  "LastName1",
		Email:     "user1@org1.io",
	})
	require.NoError(t, err)
	err = udb.InsertUser(ctx, &tables.User{
		UserID:    "US2",
		SubID:     "SubID2",
		FirstName: "FirstName2",
		LastName:  "LastName2",
		Email:     "user2@org2.io",
	})
	require.NoError(t, err)

	// Get US1's self owned group
	ctx1 := authUserCtx(ctx, env, t, "US1")
	u, err := udb.GetUser(ctx1)
	require.NoError(t, err)
	require.Len(t, u.Groups, 1, "cloud users should be added to their self-owned group")
	us1Group := u.Groups[0].Group

	// Add US2 to it
	ctx2 := authUserCtx(ctx, env, t, "US2")
	udb.AddUserToGroup(ctx2, "US2", us1Group.GroupID)

	// Make sure they were added with the proper role
	groupUsers, err := udb.GetGroupUsers(ctx1, us1Group.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)
	require.Len(t, groupUsers, 2, "US1's group should have 2 members after adding US2")
	us2 := findGroupUser(t, "US2", groupUsers)
	require.Equal(t, grpb.Group_DEVELOPER_ROLE, us2.Role, "users should have default role after being added to another group")
}

func TestAddUserToGroup_EmptyGroup_UserGetsAdminRole(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create a user
	err := udb.InsertUser(ctx, &tables.User{
		UserID:    "US1",
		SubID:     "SubID1",
		FirstName: "FirstName1",
		LastName:  "LastName1",
		Email:     "user1@org1.io",
	})
	require.NoError(t, err)
	// Create an empty group
	slug := "foo"
	groupID, err := udb.InsertOrUpdateGroup(ctx, &tables.Group{
		URLIdentifier: &slug,
	})
	require.NoError(t, err)
	// Add US1 to it
	err = udb.AddUserToGroup(ctx, "US1", groupID)
	require.NoError(t, err)

	// Make sure they are the group admin
	ctx1 := authUserCtx(ctx, env, t, "US1")
	groupUsers, err := udb.GetGroupUsers(ctx1, groupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)
	require.Len(t, groupUsers, 1)
	gu := groupUsers[0]
	require.Equal(t, grpb.Group_ADMIN_ROLE, gu.Role, "users should have admin role when added to a new group")
}

func TestUpdateGroupUsers_Role(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create a user
	err := udb.InsertUser(ctx, &tables.User{
		UserID:    "US1",
		SubID:     "SubID1",
		FirstName: "FirstName1",
		LastName:  "LastName1",
		Email:     "user1@org1.io",
	})
	require.NoError(t, err)

	// Get the user's self owned group
	ctx1 := authUserCtx(ctx, env, t, "US1")
	u, err := udb.GetUser(ctx1)
	require.NoError(t, err)
	require.Len(t, u.Groups, 1, "cloud users should be added to their self-owned group")
	us1Group := u.Groups[0].Group

	// Make sure we can update their role within the group
	err = udb.UpdateGroupUsers(ctx, us1Group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{{
		UserId: &uidpb.UserId{Id: "US1"},
		Role:   grpb.Group_DEVELOPER_ROLE,
	}})
	require.NoError(t, err)

	groupUsers, err := udb.GetGroupUsers(ctx1, us1Group.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)
	us1 := findGroupUser(t, "US1", groupUsers)
	require.Equal(t, grpb.Group_DEVELOPER_ROLE, us1.Role, "user role should be DEVELOPER")
}
