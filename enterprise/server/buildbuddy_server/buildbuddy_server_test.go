// This is a test for server/buildbuddy_server that exercises enterprise
// features that cannot be referenced in the non-enterprise test.
package buildbuddy_server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

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

func TestCreateGroup(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)
	auth := te.GetAuthenticator()
	te.SetAuthenticator(auth)
	ctx := context.Background()

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	flags.Set(t, "app.restrict_multi_group_to_enterprise", true)

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
		0,    /*=expiresIn*/
		false /*=visibleToDevelopers*/)
	require.NoError(t, err)
	adminKeyCtx := te.GetAuthenticator().AuthContextFromAPIKey(ctx, adminKey.Value)

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
	require.False(t, g.IsParent)
}

func TestCreateGroup_StatusRestrictions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      grpb.Group_GroupStatus
		shouldAllow bool
	}{
		{"FreeTier", grpb.Group_FREE_TIER_GROUP_STATUS, false},
		{"Blocked", grpb.Group_BLOCKED_GROUP_STATUS, false},
		{"Unknown", grpb.Group_UNKNOWN_GROUP_STATUS, true},
		{"EnterpriseTrial", grpb.Group_ENTERPRISE_TRIAL_GROUP_STATUS, true},
		{"Enterprise", grpb.Group_ENTERPRISE_GROUP_STATUS, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := enterprise_testenv.New(t)
			enterprise_testauth.Configure(t, te)
			auth := te.GetAuthenticator()
			te.SetAuthenticator(auth)
			ctx := context.Background()

			flags.Set(t, "app.create_group_per_user", true)
			flags.Set(t, "app.no_default_user_group", true)
			flags.Set(t, "app.restrict_multi_group_to_enterprise", true)

			err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
			require.NoError(t, err)
			userCtx := authUserCtx(ctx, te, t, "US1")
			group := getGroup(t, userCtx, te).Group
			group.URLIdentifier = "test-group"
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
			err = te.GetUserDB().UpdateGroupStatus(userCtx, group.GroupID, tc.status)
			require.NoError(t, err)

			adminKey, err := te.GetAuthDB().CreateAPIKey(
				userCtx, group.GroupID, "admin",
				[]cappb.Capability{cappb.Capability_ORG_ADMIN},
				0,    /*=expiresIn*/
				false /*=visibleToDevelopers*/)
			require.NoError(t, err)
			adminKeyCtx := te.GetAuthenticator().AuthContextFromAPIKey(ctx, adminKey.Value)

			server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
			require.NoError(t, err)

			_, err = server.CreateGroup(adminKeyCtx, &grpb.CreateGroupRequest{
				Name:          "test",
				UrlIdentifier: "test",
			})
			if tc.shouldAllow {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "enterprise account is required")
			}
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
