package userdb_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
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
	env := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, env)
	return env
}

func authUserCtx(ctx context.Context, env environment.Env, t *testing.T, userID string) context.Context {
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

func createUser(t *testing.T, ctx context.Context, env environment.Env, userID, domain string) {
	err := env.GetUserDB().InsertUser(ctx, &tables.User{
		UserID:    userID,
		SubID:     userID + "-SubID",
		FirstName: userID + "-FirstName",
		LastName:  userID + "-LastName",
		Email:     userID + "@" + domain,
	})
	require.NoError(t, err)
}

func getSingleAPIKey(t *testing.T, ctx context.Context, env environment.Env, groupID string) *tables.APIKey {
	keys, err := env.GetUserDB().GetAPIKeys(ctx, groupID, true /*=checkVisibility*/)
	require.NoError(t, err)
	require.Len(t, keys, 1, "expected exactly one API key")
	return keys[0]
}

func getSelfOwnedGroup(t *testing.T, ctx context.Context, env environment.Env) *tables.GroupRole {
	tu, err := env.GetUserDB().GetUser(ctx)
	require.NoError(t, err, "failed to get self-owned group")
	for _, g := range tu.Groups {
		if g.Group.UserID == tu.UserID {
			return g
		}
	}
	require.FailNowf(t, "failed to locate self-owned group", "user: %s", tu.UserID)
	return nil
}

func stringPointer(val string) *string {
	return &val
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

	createUser(t, ctx, env, "US1", "org1.io")
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

func TestCreateUser_Cloud_JoinsOnlyDomainGroup(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.add_user_to_domain_group", true)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create the user US1; this should also create GR1.
	createUser(t, ctx, env, "US1", "org1.io")
	ctx1 := authUserCtx(ctx, env, t, "US1")

	// Attach a slug to GR1 (orgs don't get slugs when they are created as
	// part of InsertUser).
	orgGroupID, err := udb.InsertOrUpdateGroup(ctx1, &tables.Group{
		GroupID:       "GR1",
		URLIdentifier: stringPointer("gr1-slug"),
		OwnedDomain:   "org1.io",
	})
	require.NoError(t, err)

	u, err := udb.GetUser(ctx1)
	require.NoError(t, err)

	require.Len(t, u.Groups, 1, "cloud users should be added to their domain group")

	selectedGroup := u.Groups[0].Group
	require.Equal(t, orgGroupID, selectedGroup.GroupID, "group ID of selected group should be the org's group")

	groupUsers, err := udb.GetGroupUsers(ctx1, selectedGroup.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)

	require.Len(t, groupUsers, 1, "org group should have 1 member")
	groupUser := groupUsers[0]

	require.Equal(t, grpb.Group_ADMIN_ROLE, groupUser.Role, "first user should be admins of the org group")

	createUser(t, ctx, env, "US2", "org2.io")
	ctx2 := authUserCtx(ctx, env, t, "US2")

	u2, err := udb.GetUser(ctx2)
	require.NoError(t, err)

	require.Len(t, u2.Groups, 1, "cloud users who's domain doesn't match should be added to a personal group")

	selectedGroup = u2.Groups[0].Group
	require.NotEqual(t, orgGroupID, selectedGroup.GroupID, "group ID of selected group should not be the org's group")

	groupUsers, err = udb.GetGroupUsers(ctx2, selectedGroup.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)

	require.Len(t, groupUsers, 1, "org1.io should still have 1 member, since US2 is in org2.io")
	groupUser = groupUsers[0]

	// Note: US3 has @org1.io email (US1's owned domain)
	createUser(t, ctx, env, "US3", "org1.io")
	ctx3 := authUserCtx(ctx, env, t, "US3")

	u3, err := udb.GetUser(ctx3)
	require.NoError(t, err)

	require.Len(t, u.Groups, 1, "cloud users should be added to their domain group")

	selectedGroup = u3.Groups[0].Group
	require.Equal(t, orgGroupID, selectedGroup.GroupID, "group ID of selected group should be the org's group")

	// Have US1 inspect their group members again (for org1); they should
	// now see one more member (US3).
	groupUsers, err = udb.GetGroupUsers(ctx1, selectedGroup.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)

	require.Len(t, groupUsers, 2, "org1.io group should have 2 members, since US3 is in org1.io")
	groupUser = groupUsers[1]

	require.Equal(t, grpb.Group_DEVELOPER_ROLE, groupUser.Role, "second user should have the role developer")

}

func TestCreateUser_OnPrem_OnlyFirstUserCreatedShouldBeMadeAdminOfDefaultGroup(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.add_user_to_domain_group", false)
	flags.Set(t, "app.create_group_per_user", false)
	flags.Set(t, "app.no_default_user_group", false)
	// Re-init to create the default group.
	_, err := userdb.NewUserDB(env, env.GetDBHandle())
	require.NoError(t, err)

	udb := env.GetUserDB()
	ctx := context.Background()

	// Create 2 users in the same org
	createUser(t, ctx, env, "US1", "org1.io")
	createUser(t, ctx, env, "US2", "org2.io")

	ctx1 := authUserCtx(ctx, env, t, "US1")
	u, err := udb.GetUser(ctx1)
	require.NoError(t, err)

	require.Len(t, u.Groups, 1, "US1 should be added to the default group")
	require.Equal(t, userdb.DefaultGroupID, u.Groups[0].Group.GroupID)

	defaultGroup := u.Groups[0].Group
	groupUsers, err := udb.GetGroupUsers(ctx1, defaultGroup.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)
	require.Len(t, groupUsers, 2, "default group should have 2 members")
	us1 := findGroupUser(t, "US1", groupUsers)
	require.Equal(t, grpb.Group_ADMIN_ROLE, us1.Role, "first user added to the default group should be made an admin")
	us2 := findGroupUser(t, "US2", groupUsers)
	require.Equal(t, grpb.Group_DEVELOPER_ROLE, us2.Role, "second user added to the default group should have the default role")
}

func TestInsertOrUpdateGroup(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create some users (in different orgs)
	createUser(t, ctx, env, "US1", "org1.io")
	ctx1 := authUserCtx(ctx, env, t, "US1")
	createUser(t, ctx, env, "US2", "org2.io")
	ctx2 := authUserCtx(ctx, env, t, "US2")

	g1Update := &tables.Group{GroupID: "GR1", URLIdentifier: stringPointer("gr1")}

	_, err := udb.InsertOrUpdateGroup(ctx, g1Update)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error for update from anonymous user; got: %s", err)

	_, err = udb.InsertOrUpdateGroup(ctx2, g1Update)
	require.Truef(
		t, status.IsPermissionDeniedError(err),
		"expected PermissionDenied error for update from US2; got: %s", err)

	_, err = udb.InsertOrUpdateGroup(ctx1, g1Update)
	require.NoError(t, err)
}

func TestAddUserToGroup_AddsUserWithDefaultRole(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create some users (in different orgs)
	createUser(t, ctx, env, "US1", "org1.io")
	createUser(t, ctx, env, "US2", "org2.io")

	// Get US1's self owned group
	ctx1 := authUserCtx(ctx, env, t, "US1")
	u, err := udb.GetUser(ctx1)
	require.NoError(t, err)
	require.Len(t, u.Groups, 1, "cloud users should be added to their self-owned group")
	us1Group := u.Groups[0].Group

	// Try adding US2 to it without proper auth; should fail.
	err = udb.AddUserToGroup(ctx, "US2", us1Group.GroupID)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error adding US2 to GR1 as anonymous user; got: %s ", err)

	ctx2 := authUserCtx(ctx, env, t, "US2")
	err = udb.AddUserToGroup(ctx2, "US2", us1Group.GroupID)
	require.Truef(
		t, status.IsPermissionDeniedError(err),
		"expected PermissionDenied error adding US2 to GR1 as US2; got: %s ", err)

	err = udb.AddUserToGroup(ctx1, "US2", us1Group.GroupID)
	require.NoError(t, err, "US1 should be able to add US2 to GR1")

	// Make sure they were added with the proper role
	groupUsers, err := udb.GetGroupUsers(ctx1, us1Group.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)
	require.Len(t, groupUsers, 2, "US1's group should have 2 members after adding US2")
	us2 := findGroupUser(t, "US2", groupUsers)
	require.Equal(t, grpb.Group_DEVELOPER_ROLE, us2.Role, "users should have default role after being added to another group")
}

func TestCreateGroup(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create a user
	createUser(t, ctx, env, "US1", "org1.io")
	ctx1 := authUserCtx(ctx, env, t, "US1")

	// Create a new group as US1
	groupID, err := udb.CreateGroup(ctx1, &tables.Group{})
	require.NoError(t, err)

	// Re-authenticate to pick up the new group membership
	ctx1 = authUserCtx(ctx, env, t, "US1")

	// Make sure they are the group admin
	groupUsers, err := udb.GetGroupUsers(ctx1, groupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err, "failed to get group users")
	require.Len(t, groupUsers, 1)
	gu := groupUsers[0]
	require.Equal(t, grpb.Group_ADMIN_ROLE, gu.Role, "users should have admin role when added to a new group")
}

func TestAddUserToGroup_UserPreviouslyRequestedAccess_UpdatesMembershipStatus(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	// Create a user
	createUser(t, ctx, env, "US1", "org1.io")
	ctx1 := authUserCtx(ctx, env, t, "US1")
	groupID1 := getSelfOwnedGroup(t, ctx1, env).Group.GroupID

	// Now create user US2, also with @org1.io email.
	// Note, the group does not own the org1.io domain, so US2 shouldn't be
	// auto-added to the group.
	createUser(t, ctx, env, "US2", "org1.io")
	ctx2 := authUserCtx(ctx, env, t, "US2")

	// Have US2 *request* to join US1's group
	err := udb.RequestToJoinGroup(ctx2, "US2", groupID1)
	require.NoError(t, err)

	// Now *add* US2 to the group; should update their membership request.
	err = udb.AddUserToGroup(ctx1, "US2", groupID1)
	require.NoError(t, err)
}

func TestUpdateGroupUsers_Role(t *testing.T) {
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()
	ctx := context.Background()

	createUser(t, ctx, env, "US1", "org1.io")
	ctx1 := authUserCtx(ctx, env, t, "US1")
	us1Group := getSelfOwnedGroup(t, ctx1, env).Group

	err := udb.UpdateGroupUsers(ctx1, us1Group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{{
		UserId: &uidpb.UserId{Id: "US1"},
		Role:   grpb.Group_DEVELOPER_ROLE,
	}})
	require.NoError(t, err, "US1 should be able to update their own group role")

	groupUsers, err := udb.GetGroupUsers(ctx1, us1Group.GroupID, []grp.GroupMembershipStatus{grp.GroupMembershipStatus_MEMBER})
	require.NoError(t, err)
	us1 := findGroupUser(t, "US1", groupUsers)
	require.Equal(t, grpb.Group_DEVELOPER_ROLE, us1.Role, "US1 role should be DEVELOPER")

	createUser(t, ctx, env, "US2", "org2.io")
	ctx2 := authUserCtx(ctx, env, t, "US2")

	_, err = udb.GetGroupUsers(ctx2, us1Group.GroupID, nil /*=status*/)
	require.True(
		t, status.IsPermissionDeniedError(err),
		"expected PermissionDeniedError if US2 tries to list US1's group users; got: %T",
		err)

	err = udb.UpdateGroupUsers(ctx2, us1Group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{{
		UserId: &uidpb.UserId{Id: "US1"},
		Role:   grpb.Group_ADMIN_ROLE,
	}})
	require.True(
		t, status.IsPermissionDeniedError(err),
		"expected PermissionDeniedError if US2 tries to update US1's group role; got: %T",
		err)
}

func TestUpdateAPIKey(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()

	createUser(t, ctx, env, "US1", "org1.io")
	ctx1 := authUserCtx(ctx, env, t, "US1")
	gr1 := getSelfOwnedGroup(t, ctx1, env)

	k1 := getSingleAPIKey(t, ctx1, env, gr1.Group.GroupID)
	k1.Label = "US1-Updated-Label"
	err := udb.UpdateAPIKey(ctx1, k1)

	require.NoError(t, err, "US1 should be able to update their own API key")

	createUser(t, ctx, env, "US2", "org2.io")
	ctx2 := authUserCtx(ctx, env, t, "US2")

	k1.Label = "US2-Updated-Label"
	err = udb.UpdateAPIKey(ctx2, k1)

	require.True(
		t, status.IsPermissionDeniedError(err),
		"expected PermissionDeniedError if US2 tries to update US1's API key; got: %T",
		err)
}

func TestDeleteAPIKey(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	udb := env.GetUserDB()

	createUser(t, ctx, env, "US1", "org1.io")
	ctx1 := authUserCtx(ctx, env, t, "US1")
	gr1 := getSelfOwnedGroup(t, ctx1, env)

	k1 := getSingleAPIKey(t, ctx1, env, gr1.Group.GroupID)

	createUser(t, ctx, env, "US2", "org2.io")
	ctx2 := authUserCtx(ctx, env, t, "US2")

	err := udb.DeleteAPIKey(ctx2, k1.APIKeyID)

	require.True(
		t, status.IsPermissionDeniedError(err),
		"expected PermissionDeniedError if US2 tries to delete US1's API key; got: %T",
		err)

	err = udb.DeleteAPIKey(ctx1, k1.APIKeyID)

	require.NoError(t, err, "US1 should be able to delete their API key")

	keys, err := env.GetUserDB().GetAPIKeys(ctx1, gr1.Group.GroupID, true /*=checkVisibility*/)

	require.NoError(t, err)
	require.Empty(t, keys, "US1 group's keys should be empty after deleting")
}
