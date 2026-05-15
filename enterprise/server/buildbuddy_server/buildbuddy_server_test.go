// This is a test for server/buildbuddy_server that exercises enterprise
// features that cannot be referenced in the non-enterprise test.
package buildbuddy_server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/stripe"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
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
		0, /*=expiresIn*/
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
		{"UsageBased", grpb.Group_USAGE_BASED_GROUP_STATUS, true},
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
				0, /*=expiresIn*/
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

func TestUsageBasedBillingSetup(t *testing.T) {
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)

	ctx := context.Background()
	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	group := getGroup(t, userCtx, te).Group
	group.Name = "Acme"
	group.URLIdentifier = "acme"
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
	require.NoError(t, te.GetUserDB().UpdateGroupStatus(userCtx, group.GroupID, grpb.Group_FREE_TIER_GROUP_STATUS))

	var sawCustomerRequest, sawCheckoutRequest, sawSessionRequest bool
	stripeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "sk_test_123", username)
		require.Empty(t, password)
		require.Equal(t, "2026-04-22.dahlia", r.Header.Get("Stripe-Version"))

		switch r.URL.Path {
		case "/v1/customers":
			sawCustomerRequest = true
			require.Equal(t, http.MethodPost, r.Method)
			require.Equal(t, "buildbuddy-usage-billing-customer-"+group.GroupID, r.Header.Get("Idempotency-Key"))
			require.NoError(t, r.ParseForm())
			require.Equal(t, "Acme", r.Form.Get("name"))
			require.Equal(t, group.GroupID, r.Form.Get("metadata[group_id]"))
			w.Write([]byte(`{"id":"cus_123"}`))
		case "/v1/checkout/sessions":
			sawCheckoutRequest = true
			require.Equal(t, http.MethodPost, r.Method)
			require.Empty(t, r.Header.Get("Idempotency-Key"))
			require.NoError(t, r.ParseForm())
			require.Equal(t, "setup", r.Form.Get("mode"))
			require.Equal(t, "usd", r.Form.Get("currency"))
			require.Equal(t, "cus_123", r.Form.Get("customer"))
			require.Equal(t, group.GroupID, r.Form.Get("client_reference_id"))
			require.Equal(t, group.GroupID, r.Form.Get("metadata[group_id]"))
			require.Equal(t, group.GroupID, r.Form.Get("setup_intent_data[metadata][group_id]"))
			require.Empty(t, r.Form.Get("payment_method_types[]"))
			w.Write([]byte(`{"id":"cs_123","url":"https://checkout.stripe.test/cs_123","customer":"cus_123","mode":"setup","status":"open"}`))
		case "/v1/checkout/sessions/cs_123":
			sawSessionRequest = true
			require.Equal(t, http.MethodGet, r.Method)
			w.Write([]byte(`{"id":"cs_123","customer":"cus_123","setup_intent":"seti_123","mode":"setup","status":"complete","client_reference_id":"` + group.GroupID + `"}`))
		default:
			t.Fatalf("unexpected Stripe path %q", r.URL.Path)
		}
	}))
	defer stripeServer.Close()

	flags.Set(t, "billing.stripe.api_key", "sk_test_123")
	flags.Set(t, "billing.stripe.enabled", true)
	flags.Set(t, "billing.stripe.api_url", stripeServer.URL)
	require.NoError(t, stripe.Register(te))
	buildBuddyURL, err := url.Parse("https://app.buildbuddy.test")
	require.NoError(t, err)
	flags.Set(t, "app.build_buddy_url", *buildBuddyURL)

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)
	createRsp, err := server.CreateUsageBasedBillingSetupSession(userCtx, &grpb.CreateUsageBasedBillingSetupSessionRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
	})
	require.NoError(t, err)
	require.Equal(t, "https://checkout.stripe.test/cs_123", createRsp.GetSetupUrl())
	require.Equal(t, "cs_123", createRsp.GetSetupSessionId())

	var billing tables.GroupBilling
	err = te.GetDBHandle().GORM(ctx, "test_get_group_billing").Where("group_id = ?", group.GroupID).Take(&billing).Error
	require.NoError(t, err)
	require.Equal(t, "cus_123", billing.ExternalCustomerID)
	require.Equal(t, "cs_123", billing.ExternalSetupSessionID)

	completeRsp, err := server.CompleteUsageBasedBillingSetup(userCtx, &grpb.CompleteUsageBasedBillingSetupRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		SetupSessionId: "cs_123",
	})
	require.NoError(t, err)
	require.NotNil(t, completeRsp)

	err = te.GetDBHandle().GORM(ctx, "test_get_completed_group_billing").Where("group_id = ?", group.GroupID).Take(&billing).Error
	require.NoError(t, err)
	require.Equal(t, "seti_123", billing.ExternalPaymentSetupID)
	updatedGroup, err := te.GetUserDB().GetGroupByID(ctx, group.GroupID)
	require.NoError(t, err)
	require.Equal(t, grpb.Group_USAGE_BASED_GROUP_STATUS, updatedGroup.Status)
	require.True(t, sawCustomerRequest)
	require.True(t, sawCheckoutRequest)
	require.True(t, sawSessionRequest)
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

const validSamlMetadata = `<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/saml">
  <md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/>
  </md:IDPSSODescriptor>
</md:EntityDescriptor>`

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

func TestSetSSOConfig_ValidMetadata(t *testing.T) {
	userCtx, server, te, group := setUpSSOConfigTest(t)

	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/samlmetadata+xml")
		_, _ = w.Write([]byte(validSamlMetadata))
	}))
	defer idp.Close()

	_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: idp.URL},
	})
	require.NoError(t, err)

	updated, err := te.GetUserDB().GetGroupByID(context.Background(), group.GroupID)
	require.NoError(t, err)
	assert.Equal(t, idp.URL, updated.SamlIdpMetadataUrl)
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

func TestSetSSOConfig_RejectsInvalidMetadata(t *testing.T) {
	userCtx, server, _, group := setUpSSOConfigTest(t)

	notSaml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html><body>hello</body></html>`))
	}))
	defer notSaml.Close()

	_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: notSaml.URL},
	})
	require.Error(t, err)
}

func TestSetSSOConfig_RejectsNonHTTPScheme(t *testing.T) {
	userCtx, server, _, group := setUpSSOConfigTest(t)

	_, err := server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: "file:///etc/passwd"},
	})
	require.Error(t, err)
}

func TestSetSSOConfig_RequiresServerAdmin(t *testing.T) {
	// Create a group that is NOT a server admin and verify the RPC is rejected.
	te := enterprise_testenv.New(t)
	enterprise_testauth.Configure(t, te)

	flags.Set(t, "app.create_group_per_user", true)
	flags.Set(t, "app.no_default_user_group", true)
	// Point admin_group_id at a different group ID so US1's group isn't admin.
	flags.Set(t, "auth.admin_group_id", "GR-NOT-MY-GROUP")

	ctx := context.Background()
	require.NoError(t, te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"}))
	userCtx := authUserCtx(ctx, te, t, "US1")
	group := getGroup(t, userCtx, te).Group

	server, err := buildbuddy_server.NewBuildBuddyServer(te, nil)
	require.NoError(t, err)

	_, err = server.SetSSOConfig(userCtx, &grpb.SetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
		Config:         &grpb.SSOConfig{SamlIdpMetadataUrl: "https://idp.example.com/meta"},
	})
	require.Error(t, err)

	_, err = server.GetSSOConfig(userCtx, &grpb.GetSSOConfigRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: group.GroupID},
	})
	require.Error(t, err)
}
