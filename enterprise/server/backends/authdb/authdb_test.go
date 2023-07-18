package authdb_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	crand "crypto/rand"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auditlog"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/authdb"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauditlog"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	alpb "github.com/buildbuddy-io/buildbuddy/proto/auditlog"
)

func TestSessionInsertUpdateDeleteRead(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	env := setupEnv(t)
	adb := env.GetAuthDB()

	// Insert many sessions; should all succeed
	const nSessions = 10
	for i := 0; i < nSessions; i++ {
		sid := strconv.Itoa(i)
		s := &tables.Session{
			SubID:        "SubID-" + sid,
			AccessToken:  "AccessToken-" + sid,
			RefreshToken: "RefreshToken-" + sid,
		}
		err := adb.InsertOrUpdateUserSession(ctx, sid, s)
		require.NoError(t, err)
	}

	// Try updating a random session; should succeed.
	sidToUpdate := strconv.Itoa(rand.Intn(nSessions))
	s := &tables.Session{AccessToken: "UPDATED-AccessToken-" + sidToUpdate}
	err := adb.InsertOrUpdateUserSession(ctx, sidToUpdate, s)
	require.NoError(t, err)

	// Try deleting a different random session; should succeed.
	sidToDelete := strconv.Itoa(rand.Intn(nSessions))
	for sidToDelete == sidToUpdate {
		sidToDelete = strconv.Itoa(rand.Intn(nSessions))
	}
	err = adb.ClearSession(ctx, sidToDelete)
	require.NoError(t, err)

	// Read back all the sessions, including the updated and deleted ones.
	for i := 0; i < nSessions; i++ {
		sid := strconv.Itoa(i)
		s, err := adb.ReadSession(ctx, sid)
		if sid == sidToDelete {
			require.Truef(
				t, db.IsRecordNotFound(err),
				"expected RecordNotFound, got: %v", err)
			continue
		}

		require.NoError(t, err)
		expected := &tables.Session{
			Model:        s.Model,
			SessionID:    sid,
			SubID:        "SubID-" + sid,
			AccessToken:  "AccessToken-" + sid,
			RefreshToken: "RefreshToken-" + sid,
		}
		if sid == sidToUpdate {
			expected.AccessToken = "UPDATED-AccessToken-" + sid
		}
		require.Equal(t, expected, s)
	}
}

func TestGetAPIKeyGroupFromAPIKey(t *testing.T) {
	for _, encrypt := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypt_%t", encrypt), func(t *testing.T) {
			if encrypt {
				key := make([]byte, 32)
				crand.Read(key)
				flags.Set(t, "auth.api_key_encryption.key", base64.StdEncoding.EncodeToString(key))
				flags.Set(t, "auth.api_key_encryption.encrypt_new_keys", true)
			}
			ctx := context.Background()
			env := setupEnv(t)
			adb := env.GetAuthDB()

			keys := createRandomAPIKeys(t, ctx, env)
			randKey := keys[rand.Intn(len(keys))]

			akg, err := adb.GetAPIKeyGroupFromAPIKey(ctx, randKey.Value)
			require.NoError(t, err)

			assert.Equal(t, "", akg.GetUserID())
			assert.Equal(t, randKey.GroupID, akg.GetGroupID())
			assert.Equal(t, randKey.Capabilities, akg.GetCapabilities())
			assert.Equal(t, false, akg.GetUseGroupOwnedExecutors())

			// Using an invalid or empty value should produce an error
			akg, err = adb.GetAPIKeyGroupFromAPIKey(ctx, "")
			require.Nil(t, akg)
			require.Truef(
				t, status.IsUnauthenticatedError(err),
				"expected Unauthenticated error; got: %v", err)
			akg, err = adb.GetAPIKeyGroupFromAPIKey(ctx, "INVALID")
			require.Nil(t, akg)
			require.Truef(
				t, status.IsUnauthenticatedError(err),
				"expected Unauthenticated error; got: %v", err)
		})
	}
}

func TestBackfillUnencryptedKeys(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t)
	adb := env.GetAuthDB()

	keys := createRandomAPIKeys(t, ctx, env)

	// Create a new AuthDB instance with encryption backfill enabled. This
	// should encrypt all the keys created above.
	key := make([]byte, 32)
	crand.Read(key)
	flags.Set(t, "auth.api_key_encryption.key", base64.StdEncoding.EncodeToString(key))
	flags.Set(t, "auth.api_key_encryption.encrypt_new_keys", true)
	flags.Set(t, "auth.api_key_encryption.encrypt_old_keys", true)
	adb, err := authdb.NewAuthDB(env, env.GetDBHandle())
	require.NoError(t, err)

	// Verify that we can still find the keys after backfill.
	for _, k := range keys {
		akg, err := adb.GetAPIKeyGroupFromAPIKey(ctx, k.Value)
		require.NoError(t, err)

		assert.Equal(t, "", akg.GetUserID())
		assert.Equal(t, k.GroupID, akg.GetGroupID())
		assert.Equal(t, k.Capabilities, akg.GetCapabilities())
		assert.Equal(t, false, akg.GetUseGroupOwnedExecutors())
	}
}

func TestGetAPIKeyGroupFromAPIKeyID(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	env := setupEnv(t)
	adb := env.GetAuthDB()

	keys := createRandomAPIKeys(t, ctx, env)
	randKey := keys[rand.Intn(len(keys))]

	akg, err := adb.GetAPIKeyGroupFromAPIKeyID(ctx, randKey.APIKeyID)
	require.NoError(t, err)

	assert.Equal(t, "", akg.GetUserID())
	assert.Equal(t, randKey.GroupID, akg.GetGroupID())
	assert.Equal(t, randKey.Capabilities, akg.GetCapabilities())
	assert.Equal(t, false, akg.GetUseGroupOwnedExecutors())

	// Using an invalid or empty value should produce an error
	akg, err = adb.GetAPIKeyGroupFromAPIKeyID(ctx, "")
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)
	akg, err = adb.GetAPIKeyGroupFromAPIKeyID(ctx, "INVALID")
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)
}

func TestGetAPIKeyGroupFromBasicAuth(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	env := setupEnv(t)
	adb := env.GetAuthDB()

	keys := createRandomAPIKeys(t, ctx, env)
	randKey := keys[rand.Intn(len(keys))]

	// Look up the write token for the group
	g, err := env.GetUserDB().GetGroupByID(ctx, randKey.GroupID)
	require.NoError(t, err)
	require.Equal(
		t, randKey.GroupID, g.GroupID,
		"sanity check: group ID should match the API key ID")

	akg, err := adb.GetAPIKeyGroupFromBasicAuth(ctx, g.GroupID, g.WriteToken)
	require.NoError(t, err)

	assert.Equal(t, "", akg.GetUserID())
	assert.Equal(t, randKey.GroupID, akg.GetGroupID())
	assert.Equal(t, randKey.Capabilities, akg.GetCapabilities())
	assert.Equal(t, false, akg.GetUseGroupOwnedExecutors())

	// Using invalid or empty values should produce an error
	akg, err = adb.GetAPIKeyGroupFromBasicAuth(ctx, "", "")
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)
	akg, err = adb.GetAPIKeyGroupFromBasicAuth(ctx, "", g.WriteToken)
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)
	akg, err = adb.GetAPIKeyGroupFromBasicAuth(ctx, g.GroupID, "")
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)
	akg, err = adb.GetAPIKeyGroupFromBasicAuth(ctx, "INVALID", g.WriteToken)
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)
	akg, err = adb.GetAPIKeyGroupFromBasicAuth(ctx, g.GroupID, "INVALID")
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)
}

func TestGetAPIKeys(t *testing.T) {
	for _, encrypt := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypt_%t", encrypt), func(t *testing.T) {
			if encrypt {
				key := make([]byte, 32)
				crand.Read(key)
				flags.Set(t, "auth.api_key_encryption.key", base64.StdEncoding.EncodeToString(key))
				flags.Set(t, "auth.api_key_encryption.encrypt_new_keys", true)
			}
			ctx := context.Background()
			env := setupEnv(t)
			adb := env.GetAuthDB()

			users := enterprise_testauth.CreateRandomGroups(t, env)
			// Get a random admin user.
			var admin *tables.User
			for _, u := range users {
				if role.Role(u.Groups[0].Role) == role.Admin {
					admin = u
					break
				}
			}
			require.NotNil(t, admin)
			groupID := admin.Groups[0].Group.GroupID
			auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
			adminCtx, err := auth.WithAuthenticatedUser(ctx, admin.UserID)
			require.NoError(t, err)
			keys, err := adb.GetAPIKeys(adminCtx, groupID)
			require.NoError(t, err)

			// Verify that we can auth using all of the returned keys.
			for _, k := range keys {
				_, err := adb.GetAPIKeyGroupFromAPIKey(ctx, k.Value)
				require.NoError(t, err)
			}
		})
	}
}

func TestGetAPIKeyGroup_UserOwnedKeys(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	env := setupEnv(t)
	adb := env.GetAuthDB()

	users := enterprise_testauth.CreateRandomGroups(t, env)
	// Get a random admin user.
	var admin *tables.User
	for _, u := range users {
		if role.Role(u.Groups[0].Role) == role.Admin {
			admin = u
			break
		}
	}
	require.NotNil(t, admin)
	// Look up one of their keys and convert it to a user-owned key.
	// TODO(bduffany): Once user-level keys are implemented in UserDB, use that
	// instead of directly updating the key in the DB.
	groupID := admin.Groups[0].Group.GroupID
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	adminCtx, err := auth.WithAuthenticatedUser(ctx, admin.UserID)
	require.NoError(t, err)
	keys, err := adb.GetAPIKeys(adminCtx, groupID)
	require.NoError(t, err)
	key := keys[0]
	key.UserID = admin.UserID
	err = env.GetDBHandle().DB(ctx).Updates(key).Error
	require.NoError(t, err)
	g, err := env.GetUserDB().GetGroupByID(adminCtx, key.GroupID)
	require.NoError(t, err)
	require.NotEmpty(t, g.WriteToken)

	// Should not be able to use this user-level key, since groups have the
	// setting disabled.
	akg, err := adb.GetAPIKeyGroupFromAPIKey(ctx, key.Value)
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)

	akg, err = adb.GetAPIKeyGroupFromAPIKeyID(ctx, key.APIKeyID)
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)

	// The user-owned key should have been the only key in the org, so the
	// basic auth lookup should fail here.
	akg, err = adb.GetAPIKeyGroupFromBasicAuth(ctx, g.GroupID, g.WriteToken)
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)

	// Now enable user-owned keys for the group.
	g.UserOwnedKeysEnabled = true
	_, err = env.GetUserDB().InsertOrUpdateGroup(adminCtx, g)
	require.NoError(t, err)

	// Should now be able to use the user-owned key.
	akg, err = adb.GetAPIKeyGroupFromAPIKey(ctx, key.Value)
	require.NoError(t, err)
	assert.Equal(t, key.UserID, akg.GetUserID())
	assert.Equal(t, key.GroupID, akg.GetGroupID())
	assert.Equal(t, key.Capabilities, akg.GetCapabilities())
	assert.Equal(t, false, akg.GetUseGroupOwnedExecutors())

	akg, err = adb.GetAPIKeyGroupFromAPIKeyID(ctx, key.APIKeyID)
	require.NoError(t, err)
	assert.Equal(t, key.UserID, akg.GetUserID())
	assert.Equal(t, key.GroupID, akg.GetGroupID())
	assert.Equal(t, key.Capabilities, akg.GetCapabilities())
	assert.Equal(t, false, akg.GetUseGroupOwnedExecutors())

	// The basic auth lookup should still fail, since it should never return
	// a user-owned key.
	akg, err = adb.GetAPIKeyGroupFromBasicAuth(ctx, g.GroupID, g.WriteToken)
	require.Nil(t, akg)
	require.Truef(
		t, status.IsUnauthenticatedError(err),
		"expected Unauthenticated error; got: %v", err)
}

func TestLookupUserFromSubID(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	env := setupEnv(t)
	adb := env.GetAuthDB()

	users := enterprise_testauth.CreateRandomGroups(t, env)
	randUser := users[rand.Intn(len(users))]

	u, err := adb.LookupUserFromSubID(ctx, randUser.SubID)
	require.NoError(t, err)
	require.Equal(t, randUser, u)

	// Using empty or invalid values should produce an error
	u, err = adb.LookupUserFromSubID(ctx, "")
	require.Nil(t, u)
	require.Truef(
		t, db.IsRecordNotFound(err),
		"expected RecordNotFound error; got: %v", err)
	u, err = adb.LookupUserFromSubID(ctx, "INVALID")
	require.Nil(t, u)
	require.Truef(
		t, db.IsRecordNotFound(err),
		"expected RecordNotFound error; got: %v", err)
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

func TestAPIKeyAuditLogs(t *testing.T) {
	ctx := context.Background()
	env := setupEnv(t)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	al := testauditlog.New(t)
	env.SetAuditLogger(al)
	udb := env.GetUserDB()

	// Create a user
	userID := "US1"
	userDomain := "org1.io"
	admin := createUser(t, ctx, env, userID, userDomain)
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	adminCtx, err := auth.WithAuthenticatedUser(ctx, admin.UserID)
	require.NoError(t, err)

	// Create a new group as US1
	groupID, err := udb.CreateGroup(adminCtx, &tables.Group{
		UserOwnedKeysEnabled: true,
	})
	require.NoError(t, err)

	// Re-authenticate to pick up the new group membership
	adminCtx, err = auth.WithAuthenticatedUser(ctx, admin.UserID)
	require.NoError(t, err)

	// Create Org API key.
	var key *akpb.ApiKey
	{
		al.Reset()
		req := &akpb.CreateApiKeyRequest{
			RequestContext:      nil,
			GroupId:             groupID,
			Label:               "my key",
			Capability:          []akpb.ApiKey_Capability{akpb.ApiKey_CAS_WRITE_CAPABILITY},
			VisibleToDevelopers: true,
		}
		resp, err := env.GetBuildBuddyServer().CreateApiKey(adminCtx, req)
		require.NoError(t, err)
		require.Len(t, al.GetAllEntries(), 1)
		e := al.GetAllEntries()[0]
		require.Equal(t, alpb.ResourceType_GROUP_API_KEY, e.Resource.GetType())
		require.Equal(t, resp.ApiKey.Id, e.Resource.GetId())
		require.Equal(t, auditlog.CreateAPIKey, e.Method)
		require.Equal(t, req, e.Request)
		key = resp.ApiKey
	}

	// List Org API keys.
	{
		al.Reset()
		req := &akpb.GetApiKeysRequest{GroupId: groupID}
		_, err = env.GetBuildBuddyServer().GetApiKeys(adminCtx, req)
		require.NoError(t, err)
		require.Len(t, al.GetAllEntries(), 1)
		e := al.GetAllEntries()[0]
		require.Equal(t, alpb.ResourceType_GROUP_API_KEY, e.Resource.GetType())
		require.Equal(t, auditlog.ListAPIKeys, e.Method)
		require.Equal(t, req, e.Request)
	}

	// Update Org API key.
	{
		al.Reset()
		req := &akpb.UpdateApiKeyRequest{
			Id:                  key.Id,
			Label:               "new label",
			Capability:          []akpb.ApiKey_Capability{akpb.ApiKey_REGISTER_EXECUTOR_CAPABILITY},
			VisibleToDevelopers: false,
		}
		_, err = env.GetBuildBuddyServer().UpdateApiKey(adminCtx, req)
		require.NoError(t, err)
		require.Len(t, al.GetAllEntries(), 1)
		e := al.GetAllEntries()[0]
		require.Equal(t, alpb.ResourceType_GROUP_API_KEY, e.Resource.GetType())
		require.Equal(t, key.Id, e.Resource.GetId())
		require.Equal(t, auditlog.UpdateAPIKey, e.Method)
		require.Equal(t, req, e.Request)
	}

	// Delete Org API key.
	{
		al.Reset()
		req := &akpb.DeleteApiKeyRequest{
			Id: key.Id,
		}
		_, err = env.GetBuildBuddyServer().DeleteApiKey(adminCtx, req)
		require.NoError(t, err)
		require.Len(t, al.GetAllEntries(), 1)
		e := al.GetAllEntries()[0]
		require.Equal(t, alpb.ResourceType_GROUP_API_KEY, e.Resource.GetType())
		require.Equal(t, key.Id, e.Resource.GetId())
		require.Equal(t, auditlog.DeleteAPIKey, e.Method)
		require.Equal(t, req, e.Request)
	}

	// Create User API key.
	{
		al.Reset()
		req := &akpb.CreateApiKeyRequest{
			GroupId:    groupID,
			Label:      "my key",
			Capability: []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY},
		}
		resp, err := env.GetBuildBuddyServer().CreateUserApiKey(adminCtx, req)
		require.NoError(t, err)
		require.Len(t, al.GetAllEntries(), 1)
		e := al.GetAllEntries()[0]
		require.Equal(t, alpb.ResourceType_USER_API_KEY, e.Resource.GetType())
		require.Equal(t, resp.ApiKey.Id, e.Resource.GetId())
		require.Equal(t, auditlog.CreateAPIKey, e.Method)
		require.Equal(t, req, e.Request)
		key = resp.ApiKey
	}

	// List User API keys (no audit log entries).
	{
		al.Reset()
		req := &akpb.GetApiKeysRequest{GroupId: groupID}
		_, err = env.GetBuildBuddyServer().GetUserApiKeys(adminCtx, req)
		require.NoError(t, err)
		require.Empty(t, al.GetAllEntries())
	}

	// Update User API key.
	{
		al.Reset()
		req := &akpb.UpdateApiKeyRequest{
			Id:         key.Id,
			Label:      "new label",
			Capability: []akpb.ApiKey_Capability{akpb.ApiKey_CAS_WRITE_CAPABILITY},
		}
		_, err = env.GetBuildBuddyServer().UpdateUserApiKey(adminCtx, req)
		require.NoError(t, err)
		require.Len(t, al.GetAllEntries(), 1)
		e := al.GetAllEntries()[0]
		require.Equal(t, alpb.ResourceType_USER_API_KEY, e.Resource.GetType())
		require.Equal(t, key.Id, e.Resource.GetId())
		require.Equal(t, auditlog.UpdateAPIKey, e.Method)
		require.Equal(t, req, e.Request)
	}

	{
		al.Reset()
		req := &akpb.DeleteApiKeyRequest{
			Id: key.Id,
		}
		_, err = env.GetBuildBuddyServer().DeleteUserApiKey(adminCtx, req)
		require.NoError(t, err)
		require.Len(t, al.GetAllEntries(), 1)
		e := al.GetAllEntries()[0]
		require.Equal(t, alpb.ResourceType_USER_API_KEY, e.Resource.GetType())
		require.Equal(t, key.Id, e.Resource.GetId())
		require.Equal(t, auditlog.DeleteAPIKey, e.Method)
		require.Equal(t, req, e.Request)
	}
}

func createRandomAPIKeys(t *testing.T, ctx context.Context, env environment.Env) []*tables.APIKey {
	users := enterprise_testauth.CreateRandomGroups(t, env)
	var allKeys []*tables.APIKey
	// List the org API keys accessible to any admins we created
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	for _, u := range users {
		authCtx, err := auth.WithAuthenticatedUser(ctx, u.UserID)
		require.NoError(t, err)
		if role.Role(u.Groups[0].Role) != role.Admin {
			continue
		}
		keys, err := env.GetAuthDB().GetAPIKeys(authCtx, u.Groups[0].Group.GroupID)
		require.NoError(t, err)
		allKeys = append(allKeys, keys...)
	}
	require.NotEmpty(t, allKeys, "sanity check: should have created some random API keys")
	return allKeys
}

func setupEnv(t *testing.T) environment.Env {
	flags.Set(t, "app.user_owned_keys_enabled", true)
	env := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, env) // provisions AuthDB and UserDB
	return env
}
