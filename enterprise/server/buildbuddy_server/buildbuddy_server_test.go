// This is a test for server/buildbuddy_server that exercises enterprise
// features that cannot be referenced in the non-enterprise test.
package buildbuddy_server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	openfeatureTesting "github.com/open-feature/go-sdk/openfeature/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

const (
	maxGroupsPerUserExperiment = "app.max_groups_per_user"
)

func configureMaxGroupsPerUserExperiment(t *testing.T, env *testenv.TestEnv, maxGroups int64) {
	provider := openfeatureTesting.NewTestProvider()
	provider.UsingFlags(t, map[string]memprovider.InMemoryFlag{
		maxGroupsPerUserExperiment: {
			State:          memprovider.Enabled,
			DefaultVariant: "configured",
			Variants:       map[string]any{"configured": int(maxGroups)},
		},
	})
	require.NoError(t, openfeature.SetProviderAndWait(provider))
	t.Cleanup(provider.Cleanup)
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
}

func authUserCtx(ctx context.Context, env environment.Env, t *testing.T, userID string) context.Context {
	auth := env.GetAuthenticator().(*testauth.TestAuthenticator)
	ctx, err := auth.WithAuthenticatedUser(ctx, userID)
	require.NoError(t, err)
	return ctx
}

func getGroup(t *testing.T, ctx context.Context, env environment.Env) *tables.GroupRole {
	tu, err := env.GetUserDB().GetUser(ctx)
	require.NoError(t, err, "failed to get self-owned group")
	require.Len(t, tu.Groups, 1, "getGroup: user must be part of exactly one group")
	return tu.Groups[0]
}

func TestUpdateGroup_WriterExecutorAccess(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)

	ctx := context.Background()
	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	group := getGroup(t, userCtx, te).Group
	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	_, err = server.UpdateGroup(userCtx, &grpb.UpdateGroupRequest{
		RequestContext:              &ctxpb.RequestContext{GroupId: group.GroupID},
		Name:                        group.Name,
		UrlIdentifier:               "writer-executor-access",
		SuggestionPreference:        group.SuggestionPreference,
		WriterExecutorAccessEnabled: true,
	})
	require.NoError(t, err)

	updatedGroup, err := te.GetUserDB().GetGroupByID(userCtx, group.GroupID)
	require.NoError(t, err)
	require.True(t, updatedGroup.WriterExecutorAccessEnabled)
	rsp, err := server.GetUser(userCtx, &uspb.GetUserRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
	})
	require.NoError(t, err)
	require.Len(t, rsp.GetUserGroup(), 1)
	require.True(t, rsp.GetUserGroup()[0].GetWriterExecutorAccessEnabled())
}

func TestCreateGroup(t *testing.T) {
	flags.Set(t, "auth.api_key_group_cache_ttl", 0)
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	auth := te.GetAuthenticator()
	te.SetAuthenticator(auth)
	ctx := context.Background()

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)

	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	parentGroup := getGroup(t, userCtx, te).Group
	parentGroup.SamlIdpMetadataUrl = "https://some/saml/url"
	parentGroup.URLIdentifier = "foo"
	_, err = te.GetUserDB().UpdateGroup(userCtx, &parentGroup)
	require.NoError(t, err)

	// Set up server admin and update group status to enterprise
	flags.Set(t, "auth.admin_group_id", parentGroup.GroupID)
	adminRole, err := role.ToProto(role.Admin)
	require.NoError(t, err)
	err = te.GetUserDB().UpdateGroupUsers(userCtx, parentGroup.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
		{
			UserId: &uidpb.UserId{Id: "US1"},
			Role:   adminRole,
		},
	})
	require.NoError(t, err)
	userCtx = authUserCtx(ctx, te, t, "US1")
	err = te.GetUserDB().UpdateGroupStatus(userCtx, parentGroup.GroupID, grpb.Group_ENTERPRISE_GROUP_STATUS)
	require.NoError(t, err)

	adminKey, err := te.GetAuthDB().CreateAPIKey(
		userCtx, parentGroup.GroupID, "admin",
		[]cappb.Capability{cappb.Capability_ORG_ADMIN},
		0, /*=expiresIn*/
		false /*=visibleToDevelopers*/)
	require.NoError(t, err)
	adminKeyCtx := te.GetAuthenticator().AuthContextFromAPIKey(ctx, adminKey.Value)

	// Enable all organization-creation restrictions. Enterprise parent orgs
	// should always be able to create child orgs using an org API key.
	configureMaxGroupsPerUserExperiment(t, te, 1)
	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	// Create a new group. The SAML IDP Metadata URL should not be set as the
	// first group is not marked as a "parent".
	rsp, err := server.CreateGroup(adminKeyCtx, &grpb.CreateGroupRequest{
		Name:          "test",
		UrlIdentifier: "test",
	})
	require.NoError(t, err)
	g, err := te.GetUserDB().GetGroupByID(ctx, rsp.GetId())
	require.NoError(t, err)
	require.Empty(t, g.SamlIdpMetadataUrl)
	require.Equal(t, grpb.Group_ENTERPRISE_GROUP_STATUS, g.Status)

	// Make the first group a parent and try again.
	// The SAML IDP Metadata URL should match that of the original group.
	parentGroup.IsParent = true
	_, err = te.GetUserDB().UpdateGroup(userCtx, &parentGroup)
	require.NoError(t, err)
	rsp, err = server.CreateGroup(adminKeyCtx, &grpb.CreateGroupRequest{
		Name:          "test2",
		UrlIdentifier: "test2",
	})
	require.NoError(t, err)
	g, err = te.GetUserDB().GetGroupByID(ctx, rsp.GetId())
	require.NoError(t, err)
	require.Equal(t, parentGroup.SamlIdpMetadataUrl, g.SamlIdpMetadataUrl)
	require.Equal(t, grpb.Group_ENTERPRISE_GROUP_STATUS, g.Status)
	require.False(t, g.IsParent)

	// Enterprise trial groups provisioned using an org admin API key inherit
	// enterprise trial status as well.
	err = te.GetUserDB().UpdateGroupStatus(userCtx, parentGroup.GroupID, grpb.Group_ENTERPRISE_TRIAL_GROUP_STATUS)
	require.NoError(t, err)
	adminKeyCtx = te.GetAuthenticator().AuthContextFromAPIKey(ctx, adminKey.Value)
	rsp, err = server.CreateGroup(adminKeyCtx, &grpb.CreateGroupRequest{
		Name:          "test3",
		UrlIdentifier: "test3",
	})
	require.NoError(t, err)
	g, err = te.GetUserDB().GetGroupByID(ctx, rsp.GetId())
	require.NoError(t, err)
	require.Equal(t, grpb.Group_ENTERPRISE_TRIAL_GROUP_STATUS, g.Status)
}

func TestCreateGroup_Allowed(t *testing.T) {
	for _, tc := range []struct {
		name          string
		maxGroups     int
		groupStatus   grpb.Group_GroupStatus
		ownedGroups   int
		invitedGroups int
		orgAPIKey     bool
		userAPIKey    bool
		expectDenied  bool
	}{
		{
			name:        "limit_disabled",
			maxGroups:   0,
			groupStatus: grpb.Group_FREE_TIER_GROUP_STATUS,
			ownedGroups: 2,
		},
		{
			name:        "free_user_below_limit",
			maxGroups:   2,
			groupStatus: grpb.Group_FREE_TIER_GROUP_STATUS,
			ownedGroups: 1,
		},
		{
			name:          "invited_groups_do_not_count_toward_limit",
			maxGroups:     2,
			groupStatus:   grpb.Group_FREE_TIER_GROUP_STATUS,
			ownedGroups:   1,
			invitedGroups: 2,
		},
		{
			name:         "free_user_at_limit",
			maxGroups:    2,
			groupStatus:  grpb.Group_FREE_TIER_GROUP_STATUS,
			ownedGroups:  2,
			expectDenied: true,
		},
		{
			name:         "blocked_user_below_limit",
			maxGroups:    2,
			groupStatus:  grpb.Group_BLOCKED_GROUP_STATUS,
			ownedGroups:  1,
			expectDenied: true,
		},
		{
			name:        "enterprise_user_not_limited",
			maxGroups:   2,
			groupStatus: grpb.Group_ENTERPRISE_GROUP_STATUS,
			ownedGroups: 2,
		},
		{
			name:        "enterprise_trial_user_not_limited",
			maxGroups:   2,
			groupStatus: grpb.Group_ENTERPRISE_TRIAL_GROUP_STATUS,
			ownedGroups: 2,
		},
		{
			name:        "enterprise_org_api_key_not_limited",
			maxGroups:   2,
			groupStatus: grpb.Group_ENTERPRISE_GROUP_STATUS,
			ownedGroups: 2,
			orgAPIKey:   true,
		},
		{
			name:        "enterprise_trial_org_api_key_not_limited",
			maxGroups:   2,
			groupStatus: grpb.Group_ENTERPRISE_TRIAL_GROUP_STATUS,
			ownedGroups: 2,
			orgAPIKey:   true,
		},
		{
			name:        "enterprise_user_api_key_not_limited",
			maxGroups:   2,
			groupStatus: grpb.Group_ENTERPRISE_GROUP_STATUS,
			ownedGroups: 2,
			userAPIKey:  true,
		},
		{
			name:        "enterprise_trial_user_api_key_not_limited",
			maxGroups:   2,
			groupStatus: grpb.Group_ENTERPRISE_TRIAL_GROUP_STATUS,
			ownedGroups: 2,
			userAPIKey:  true,
		},
		{
			name:         "unknown_status_org_api_key_denied",
			maxGroups:    1,
			groupStatus:  grpb.Group_UNKNOWN_GROUP_STATUS,
			ownedGroups:  1,
			orgAPIKey:    true,
			expectDenied: true,
		},
		{
			name:         "blocked_org_api_key_denied_when_group_limit_enabled",
			maxGroups:    1,
			groupStatus:  grpb.Group_BLOCKED_GROUP_STATUS,
			ownedGroups:  1,
			orgAPIKey:    true,
			expectDenied: true,
		},
		{
			name:         "blocked_org_api_key_denied_without_group_limit",
			groupStatus:  grpb.Group_BLOCKED_GROUP_STATUS,
			ownedGroups:  1,
			orgAPIKey:    true,
			expectDenied: true,
		},
		{
			name:         "non_enterprise_org_api_key_denied",
			maxGroups:    5,
			groupStatus:  grpb.Group_FREE_TIER_GROUP_STATUS,
			ownedGroups:  1,
			orgAPIKey:    true,
			expectDenied: true,
		},
		{
			name:         "non_enterprise_user_api_key_denied",
			maxGroups:    5,
			groupStatus:  grpb.Group_FREE_TIER_GROUP_STATUS,
			ownedGroups:  1,
			userAPIKey:   true,
			expectDenied: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.userAPIKey {
				flags.Set(t, "app.user_owned_keys_enabled", true)
			}
			te := enterprise_testenv.New(t)
			enterprise_testauth.Configure(t, te)
			auth := te.GetAuthenticator()
			te.SetAuthenticator(auth)
			ctx := context.Background()

			flags.Set(t, "app.create_group_per_user", true)
			flags.Set(t, "app.no_default_user_group", true)
			err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
			require.NoError(t, err)
			userCtx := authUserCtx(ctx, te, t, "US1")
			group := getGroup(t, userCtx, te).Group
			group.URLIdentifier = "initial-group"
			group.UserOwnedKeysEnabled = tc.userAPIKey
			_, err = te.GetUserDB().UpdateGroup(userCtx, &group)
			require.NoError(t, err)

			flags.Set(t, "auth.admin_group_id", group.GroupID)
			adminRole, err := role.ToProto(role.Admin)
			require.NoError(t, err)
			err = te.GetUserDB().UpdateGroupUsers(userCtx, group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
				{
					UserId: &uidpb.UserId{Id: "US1"},
					Role:   adminRole,
				},
			})
			require.NoError(t, err)
			userCtx = authUserCtx(ctx, te, t, "US1")
			err = te.GetUserDB().UpdateGroupStatus(userCtx, group.GroupID, tc.groupStatus)
			require.NoError(t, err)
			userCtx = authUserCtx(ctx, te, t, "US1")

			for i := 1; i < tc.ownedGroups; i++ {
				_, err := te.GetUserDB().CreateGroup(userCtx, &tables.Group{
					Name:          fmt.Sprintf("Existing group %d", i),
					URLIdentifier: fmt.Sprintf("existing-group-%d", i),
					UserID:        "US1",
				})
				require.NoError(t, err)
				userCtx = authUserCtx(ctx, te, t, "US1")
			}

			if tc.invitedGroups > 0 {
				err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US2", SubID: "US2SubID"})
				require.NoError(t, err)
				otherUserCtx := authUserCtx(ctx, te, t, "US2")
				otherGroupIDs := []string{getGroup(t, otherUserCtx, te).Group.GroupID}
				for i := 1; i < tc.invitedGroups; i++ {
					groupID, err := te.GetUserDB().CreateGroup(otherUserCtx, &tables.Group{
						Name:          fmt.Sprintf("Inviting group %d", i),
						URLIdentifier: fmt.Sprintf("inviting-group-%d", i),
						UserID:        "US2",
					})
					require.NoError(t, err)
					otherGroupIDs = append(otherGroupIDs, groupID)
				}
				for _, groupID := range otherGroupIDs {
					groupCtx := requestcontext.ContextWithProtoRequestContext(ctx, &ctxpb.RequestContext{GroupId: groupID})
					groupCtx = authUserCtx(groupCtx, te, t, "US2")
					err := te.GetUserDB().UpdateGroupUsers(groupCtx, groupID, []*grpb.UpdateGroupUsersRequest_Update{{
						UserId:           &uidpb.UserId{Id: "US1"},
						MembershipAction: grpb.UpdateGroupUsersRequest_Update_ADD,
					}})
					require.NoError(t, err)
				}
				userCtx = authUserCtx(ctx, te, t, "US1")
			}
			user, err := te.GetUserDB().GetUser(userCtx)
			require.NoError(t, err)
			require.Len(t, user.Groups, tc.ownedGroups+tc.invitedGroups)

			configureMaxGroupsPerUserExperiment(t, te, int64(tc.maxGroups))
			server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
			require.NoError(t, err)

			requestCtx := userCtx
			if tc.orgAPIKey {
				adminKey, err := te.GetAuthDB().CreateAPIKey(
					userCtx, group.GroupID, "admin",
					[]cappb.Capability{cappb.Capability_ORG_ADMIN},
					0, /*=expiresIn*/
					false /*=visibleToDevelopers*/)
				require.NoError(t, err)
				requestCtx = te.GetAuthenticator().AuthContextFromAPIKey(ctx, adminKey.Value)
			} else if tc.userAPIKey {
				userKey, err := te.GetAuthDB().CreateUserAPIKey(
					userCtx, group.GroupID, "US1", "user key",
					nil /*=capabilities*/, 0 /*=expiresIn*/)
				require.NoError(t, err)
				requestCtx = te.GetAuthenticator().AuthContextFromAPIKey(ctx, userKey.Value)
			}
			rsp, err := server.CreateGroup(requestCtx, &grpb.CreateGroupRequest{
				Name:          "My Org",
				UrlIdentifier: "my-org",
			})

			if tc.expectDenied {
				require.Error(t, err)
				require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got %s", err)
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, rsp.GetId())
			}

			user, err = te.GetUserDB().GetUser(userCtx)
			require.NoError(t, err)
			expectedGroups := tc.ownedGroups + tc.invitedGroups
			// Groups created using an org API key are not owned by or directly
			// associated with the user that created the API key.
			if !tc.expectDenied && !tc.orgAPIKey {
				expectedGroups++
			}
			require.Len(t, user.Groups, expectedGroups)
		})
	}
}

func TestSetGroupStatus(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)

	ctx := context.Background()
	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	group := getGroup(t, userCtx, te).Group

	flags.Set(t, "auth.admin_group_id", group.GroupID)
	adminRole, err := role.ToProto(role.Admin)
	require.NoError(t, err)
	err = te.GetUserDB().UpdateGroupUsers(userCtx, group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
		{
			UserId: &uidpb.UserId{Id: "US1"},
			Role:   adminRole,
		},
	})
	require.NoError(t, err)
	userCtx = authUserCtx(ctx, te, t, "US1")

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	req := &grpb.SetGroupStatusRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Status:         grpb.Group_UNKNOWN_GROUP_STATUS,
	}
	rsp, err := server.SetGroupStatus(userCtx, req)
	require.NoError(t, err)
	require.NotNil(t, rsp)

	updatedGroup, err := te.GetUserDB().GetGroupByID(ctx, group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, grpb.Group_UNKNOWN_GROUP_STATUS, updatedGroup.Status)

	req.Status = grpb.Group_BLOCKED_GROUP_STATUS
	rsp, err = server.SetGroupStatus(userCtx, req)
	require.NoError(t, err)
	require.NotNil(t, rsp)

	updatedGroup, err = te.GetUserDB().GetGroupByID(ctx, group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, grpb.Group_BLOCKED_GROUP_STATUS, updatedGroup.Status)
}

func setUpSSOConfigTest(t *testing.T) (context.Context, *buildbuddy_server.BuildBuddyServer, environment.Env, *tables.Group) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)

	ctx := context.Background()
	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	group := getGroup(t, userCtx, te).Group

	flags.Set(t, "auth.admin_group_id", group.GroupID)
	adminRole, err := role.ToProto(role.Admin)
	require.NoError(t, err)
	err = te.GetUserDB().UpdateGroupUsers(userCtx, group.GroupID, []*grpb.UpdateGroupUsersRequest_Update{
		{UserId: &uidpb.UserId{Id: "US1"}, Role: adminRole},
	})
	require.NoError(t, err)
	userCtx = authUserCtx(ctx, te, t, "US1")

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)
	return userCtx, server, te, &group
}

func TestGetSSOConfig(t *testing.T) {
	userCtx, server, te, group := setUpSSOConfigTest(t)

	// Initially unset.
	rsp, err := server.GetSSOConfig(userCtx, &grpb.GetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
	})
	require.NoError(t, err)
	assert.Equal(t, "", rsp.GetConfig().GetSamlIdpMetadataUrl())

	// Persist a value directly and read it back.
	require.NoError(t, te.GetUserDB().UpdateGroupSamlIdpMetadataUrl(userCtx, group.GroupID, "https://idp.example.com/meta"))

	rsp, err = server.GetSSOConfig(userCtx, &grpb.GetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
	})
	require.NoError(t, err)
	assert.Equal(t, "https://idp.example.com/meta", rsp.GetConfig().GetSamlIdpMetadataUrl())
}

const testSamlMetadata = `<?xml version="1.0"?>
<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/metadata">
  <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/>
  </IDPSSODescriptor>
</EntityDescriptor>`

func TestValidateSamlIdpMetadataURL(t *testing.T) {
	// A TLS test server standing in for the IdP. Its client trusts the
	// server's self-signed cert, so we use it in place of the SSRF-blocking
	// production client for these unit tests.
	idp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/samlmetadata+xml")
		_, _ = w.Write([]byte(testSamlMetadata))
	}))
	defer idp.Close()

	notSaml := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html><body>not saml</body></html>`))
	}))
	defer notSaml.Close()

	for _, tc := range []struct {
		name    string
		url     string
		client  *http.Client
		wantErr bool
	}{
		{name: "valid_https_metadata", url: idp.URL, client: idp.Client(), wantErr: false},
		{name: "non_saml_content", url: notSaml.URL, client: notSaml.Client(), wantErr: true},
		{name: "http_scheme_rejected", url: "http://idp.example.com/meta", client: http.DefaultClient, wantErr: true},
		{name: "file_scheme_rejected", url: "file:///etc/passwd", client: http.DefaultClient, wantErr: true},
		{name: "missing_host_rejected", url: "https:///meta", client: http.DefaultClient, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := buildbuddy_server.ValidateSamlIdpMetadataURL(context.Background(), tc.client, tc.url)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateSamlIdpMetadataURL_BlocksPrivateIP(t *testing.T) {
	client := httpclient.New(nil /*=allowedPrivateIPNets*/, "test")
	err := buildbuddy_server.ValidateSamlIdpMetadataURL(context.Background(), client, "https://10.0.0.1/metadata")
	require.Error(t, err)
}

func TestSetSSOConfig_ClearsURL(t *testing.T) {
	userCtx, server, te, group := setUpSSOConfigTest(t)

	// Seed a value so we can verify it gets cleared.
	require.NoError(t, te.GetUserDB().UpdateGroupSamlIdpMetadataUrl(userCtx, group.GroupID, "https://idp.example.com/meta"))

	_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: ""},
	})
	require.NoError(t, err)

	updated, err := te.GetUserDB().GetGroupByID(context.Background(), group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, "", updated.SamlIdpMetadataUrl)
}

func TestSetSSOConfig_RejectsNonHTTPSScheme(t *testing.T) {
	userCtx, server, _, group := setUpSSOConfigTest(t)

	for _, metadataURL := range []string{"http://idp.example.com/meta", "file:///etc/passwd"} {
		_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
			RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
			Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: metadataURL},
		})
		require.Errorf(t, err, "expected %q to be rejected", metadataURL)
	}
}

// TestSetSSOConfig_OrgAdminNotServerAdmin verifies that an org admin who is not
// a server admin can manage their own group's SSO config (the self-serve case).
func TestSetSSOConfig_OrgAdminNotServerAdmin(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	// Point admin_group_id at a nonexistent group so the org admin is NOT also a
	// server admin.
	flags.Set(t, "auth.admin_group_id", "GR-NONEXISTENT")

	ctx := context.Background()
	admin := enterprise_testauth.CreateRandomUser(t, te, "sso-selfserve-test.io")
	adminCtx := authUserCtx(ctx, te, t, admin.UserID)
	group := getGroup(t, adminCtx, te).Group

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	// Seeding directly exercises the DB-layer authorization: an org admin (not a
	// server admin) can set the URL.
	require.NoError(t, te.GetUserDB().UpdateGroupSamlIdpMetadataUrl(adminCtx, group.GroupID, "https://idp.example.com/meta"))

	// Clearing via the RPC exercises the end-to-end self-serve path.
	_, err = server.SetSSOConfig(adminCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: ""},
	})
	require.NoError(t, err)

	updated, err := te.GetUserDB().GetGroupByID(context.Background(), group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, "", updated.SamlIdpMetadataUrl)
}

// TestSetSSOConfig_RejectsNonAdmin verifies that a non-admin member of a group
// cannot change the group's SSO config.
func TestSetSSOConfig_RejectsNonAdmin(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	// Point admin_group_id at a nonexistent group so group members aren't
	// server admins.
	flags.Set(t, "auth.admin_group_id", "GR-NONEXISTENT")

	ctx := context.Background()
	const domain = "sso-nonadmin-test.io"
	admin := enterprise_testauth.CreateRandomUser(t, te, domain)
	adminCtx := authUserCtx(ctx, te, t, admin.UserID)
	group := getGroup(t, adminCtx, te).Group
	// Own the domain so the next user auto-joins the group as a (non-admin)
	// developer.
	_, err := te.GetUserDB().UpdateGroup(adminCtx, &tables.Group{
		GroupID:       group.GroupID,
		URLIdentifier: "sso-nonadmin-slug",
		OwnedDomain:   domain,
	})
	require.NoError(t, err)
	dev := enterprise_testauth.CreateRandomUser(t, te, domain)
	devCtx := authUserCtx(ctx, te, t, dev.UserID)

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	_, err = server.SetSSOConfig(devCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: "https://idp.example.com/meta"},
	})
	// Rejected specifically for lacking admin capability, not some other error.
	require.Truef(t, status.IsPermissionDeniedError(err), "expected PermissionDenied, got: %v", err)
	require.Contains(t, status.Message(err), "missing required capabilities")
}

// TestSetSSOConfig_RejectsCrossTenantAdmin verifies that an admin of one org
// cannot read or change the SSO config of a different org.
func TestSetSSOConfig_RejectsCrossTenantAdmin(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	flags.Set(t, "auth.admin_group_id", "GR-NONEXISTENT")

	ctx := context.Background()
	adminA := enterprise_testauth.CreateRandomUser(t, te, "tenant-a.io")
	adminACtx := authUserCtx(ctx, te, t, adminA.UserID)
	adminB := enterprise_testauth.CreateRandomUser(t, te, "tenant-b.io")
	adminBCtx := authUserCtx(ctx, te, t, adminB.UserID)
	groupB := getGroup(t, adminBCtx, te).Group

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	// Admin of tenant A targeting tenant B's group is rejected specifically for
	// not being a member of tenant B, not some other error.
	_, err = server.SetSSOConfig(adminACtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupB.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: "https://idp.example.com/meta"},
	})
	require.Truef(t, status.IsPermissionDeniedError(err), "expected PermissionDenied from SetSSOConfig, got: %v", err)
	require.Contains(t, status.Message(err), "not a member of the requested organization")

	_, err = server.GetSSOConfig(adminACtx, &grpb.GetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: groupB.GroupID},
	})
	require.Truef(t, status.IsPermissionDeniedError(err), "expected PermissionDenied from GetSSOConfig, got: %v", err)
	require.Contains(t, status.Message(err), "not a member of the requested organization")
}
