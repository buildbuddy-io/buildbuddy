package notification

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	npb "github.com/buildbuddy-io/buildbuddy/proto/notification"
)

func TestGetAdminEmailsReturnsUniqueAdminRecipients(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"APIKEY1": apiKeyUser("AK1", "GR1", cappb.Capability_SEND_NOTIFICATION),
	})
	env.SetAuthenticator(auth)
	authCtx := auth.AuthContextFromAPIKey(ctx, "APIKEY1")
	createGroup(t, ctx, env.GetDBHandle(), "GR1", "Acme", "acme")
	createAdminRecipient(t, ctx, env.GetDBHandle(), "GR1", "UA1", "Admin", "One", " admin1@example.com ")
	createAdminRecipient(t, ctx, env.GetDBHandle(), "GR1", "UA2", "Admin", "Duplicate", "admin1@example.com")
	createAdminRecipient(t, ctx, env.GetDBHandle(), "GR1", "UA3", "Admin", "Two", "admin2@example.com")
	createUserAndMembership(t, ctx, env.GetDBHandle(), "GR1", "UD1", "Dev", "One", "dev@example.com", uint32(role.Developer), int32(grpb.GroupMembershipStatus_MEMBER))
	// An admin of a different group must not be included in GR1's recipients.
	createGroup(t, ctx, env.GetDBHandle(), "GR2", "Other", "other")
	createAdminRecipient(t, ctx, env.GetDBHandle(), "GR2", "UA4", "Other", "Admin", "other-admin@example.com")

	service := New(env, &fakeEmailSender{})
	got, err := service.getAdminEmails(authCtx, "GR1")
	require.NoError(t, err)

	assert.Equal(t, []email.Address{
		{Name: "Admin One", Email: "admin1@example.com"},
		{Name: "Admin Two", Email: "admin2@example.com"},
	}, got.recipients)
	assert.Equal(t, "Acme", groupDisplayName(got))
}

func TestGetAdminEmailsIncludesUserListAdminRecipients(t *testing.T) {
	flags.Set(t, "auth.enable_user_lists", true)
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"APIKEY1": apiKeyUser("AK1", "GR1", cappb.Capability_SEND_NOTIFICATION),
	})
	env.SetAuthenticator(auth)
	authCtx := auth.AuthContextFromAPIKey(ctx, "APIKEY1")
	createGroup(t, ctx, env.GetDBHandle(), "GR1", "Acme", "acme")
	createAdminRecipient(t, ctx, env.GetDBHandle(), "GR1", "UA1", "Direct", "Admin", "direct-admin@example.com")
	createUser(t, ctx, env.GetDBHandle(), "UL1", "List", "Admin", "list-admin@example.com")
	createUserListMembership(t, ctx, env.GetDBHandle(), "GR1", "LIST1", "UL1", uint32(role.Admin))

	service := New(env, &fakeEmailSender{})
	got, err := service.getAdminEmails(authCtx, "GR1")
	require.NoError(t, err)

	assert.Equal(t, []email.Address{
		{Name: "Direct Admin", Email: "direct-admin@example.com"},
		{Name: "List Admin", Email: "list-admin@example.com"},
	}, got.recipients)
}

func TestGetAdminEmailsRequiresRecipients(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"APIKEY1": apiKeyUser("AK1", "GR1", cappb.Capability_SEND_NOTIFICATION),
	})
	env.SetAuthenticator(auth)
	authCtx := auth.AuthContextFromAPIKey(ctx, "APIKEY1")

	service := New(env, &fakeEmailSender{})
	_, err := service.getAdminEmails(authCtx, "GR1")
	assert.True(t, status.IsFailedPreconditionError(err))
}

func TestSendNotificationRequiresNotificationCapability(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"NO_CAP":   apiKeyUser("AK1", "GR1", cappb.Capability_CACHE_WRITE),
		"WITH_CAP": apiKeyUser("AK2", "GR1", cappb.Capability_SEND_NOTIFICATION),
	})
	env.SetAuthenticator(auth)
	service := New(env, &fakeEmailSender{})
	req := &npb.SendNotificationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
	}

	// A caller whose key lacks SEND_NOTIFICATION is rejected.
	_, err := service.SendNotification(auth.AuthContextFromAPIKey(ctx, "NO_CAP"), req)
	assert.True(t, status.IsPermissionDeniedError(err))

	// A caller whose key has SEND_NOTIFICATION passes the capability check. The
	// request is then rejected for a different reason (no notification event is
	// set), confirming authorization succeeded.
	_, err = service.SendNotification(auth.AuthContextFromAPIKey(ctx, "WITH_CAP"), req)
	assert.True(t, status.IsInvalidArgumentError(err))
}

func TestNondeterminismDetected(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	auth := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"APIKEY1": apiKeyUser("AK1", "GR1", cappb.Capability_SEND_NOTIFICATION),
	})
	env.SetAuthenticator(auth)
	createGroup(t, ctx, env.GetDBHandle(), "GR1", "Acme", "acme")
	createAdminRecipient(t, ctx, env.GetDBHandle(), "GR1", "UA1", "Admin", "One", " admin1@example.com ")
	sender := &fakeEmailSender{}
	service := New(env, sender)
	req := &npb.SendNotificationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Event: &npb.SendNotificationRequest_NondeterminismDetected{
			NondeterminismDetected: &npb.NondeterminismDetected{
				BuildInvocationIds: []string{"INV1", "INV2"},
				ParentInvocationId: "INV0",
			},
		},
	}

	_, err := service.SendNotification(auth.AuthContextFromAPIKey(ctx, "APIKEY1"), req)
	assert.NoError(t, err)

	assert.Equal(t, 1, len(sender.messages))
	assert.Equal(t, "Nondeterminism detected in your build", sender.messages[0].Subject)
	assert.Equal(t, "<p>Hi Acme team,</p>\n<p>BuildBuddy detected nondeterminism in your repository. Running the same command twice produced different outputs.</p>\n<p><a href=\"http://localhost:8080/invocation/INV0\">See the affected actions.</a></p>", sender.messages[0].Body)
}

func apiKeyUser(apiKeyID, groupID string, caps ...cappb.Capability) *claims.Claims {
	u := user("", groupID, caps...)
	u.APIKeyID = apiKeyID
	u.APIKeyOwnerGroupID = groupID
	return u
}

func user(userID, groupID string, caps ...cappb.Capability) *claims.Claims {
	return &claims.Claims{
		UserID:        userID,
		GroupID:       groupID,
		AllowedGroups: []string{groupID},
		Capabilities:  caps,
		GroupMemberships: []*interfaces.GroupMembership{
			{
				GroupID:      groupID,
				Capabilities: caps,
			},
		},
	}
}

func createGroup(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, groupID, name, urlIdentifier string) {
	require.NoError(t, dbh.NewQuery(ctx, "notification_test_create_group").Create(&tables.Group{
		GroupID:       groupID,
		Name:          name,
		URLIdentifier: urlIdentifier,
	}))
}

func createAdminRecipient(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, groupID, userID, firstName, lastName, emailAddress string) {
	createUserAndMembership(t, ctx, dbh, groupID, userID, firstName, lastName, emailAddress, uint32(role.Admin), int32(grpb.GroupMembershipStatus_MEMBER))
}

func createUserAndMembership(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, groupID, userID, firstName, lastName, emailAddress string, userRole uint32, membershipStatus int32) {
	createUser(t, ctx, dbh, userID, firstName, lastName, emailAddress)
	require.NoError(t, dbh.NewQuery(ctx, "notification_test_create_user_group").Create(&tables.UserGroup{
		UserUserID:       userID,
		GroupGroupID:     groupID,
		Role:             userRole,
		MembershipStatus: membershipStatus,
	}))
}

func createUser(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, userID, firstName, lastName, emailAddress string) {
	require.NoError(t, dbh.NewQuery(ctx, "notification_test_create_user").Create(&tables.User{
		UserID:    userID,
		FirstName: firstName,
		LastName:  lastName,
		Email:     emailAddress,
	}))
}

func createUserListMembership(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, groupID, userListID, userID string, listRole uint32) {
	require.NoError(t, dbh.NewQuery(ctx, "notification_test_create_user_list").Create(&tables.UserList{
		UserListID: userListID,
		GroupID:    groupID,
		Name:       userListID,
	}))
	require.NoError(t, dbh.NewQuery(ctx, "notification_test_create_user_user_list").Create(&tables.UserUserList{
		UserUserID:         userID,
		UserListUserListID: userListID,
	}))
	require.NoError(t, dbh.NewQuery(ctx, "notification_test_create_user_list_group").Create(&tables.UserListGroup{
		UserListUserListID: userListID,
		GroupGroupID:       groupID,
		Role:               listRole,
	}))
}

type fakeEmailSender struct {
	mu       sync.Mutex
	messages []*email.Message
	err      error
}

func (s *fakeEmailSender) Send(ctx context.Context, msg *email.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.messages = append(s.messages, msg)
	return nil
}
