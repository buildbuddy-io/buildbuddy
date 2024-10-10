// This is a test for server/buildbuddy_server that exercises enterprise
// features that cannot be referenced in the non-enterprise test.
package buildbuddy_server_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
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

	err := te.GetUserDB().InsertUser(ctx, &tables.User{UserID: "US1", SubID: "US1SubID"})
	require.NoError(t, err)
	userCtx := authUserCtx(ctx, te, t, "US1")
	parentGroup := getGroup(t, userCtx, te).Group
	parentGroup.SamlIdpMetadataUrl = "https://some/saml/url"
	parentGroup.URLIdentifier = "foo"
	_, err = te.GetUserDB().UpdateGroup(userCtx, &parentGroup)
	require.NoError(t, err)

	adminKey, err := te.GetAuthDB().CreateAPIKey(
		userCtx, parentGroup.GroupID, "admin",
		[]akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY},
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
