package claims_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testkeys"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

func contextWithUnverifiedJWT(c *claims.Claims) context.Context {
	authCtx := claims.AuthContextWithJWT(context.Background(), c, nil)
	jwt := authCtx.Value(authutil.ContextTokenStringKey).(string)
	return context.WithValue(context.Background(), authutil.ContextTokenStringKey, jwt)
}

func requireClaimsEqual(t *testing.T, expected *claims.Claims, actual *claims.Claims) {
	expected.StandardClaims.ExpiresAt = 0
	actual.StandardClaims.ExpiresAt = 0
	require.Equal(t, expected, actual)
}

func TestJWT(t *testing.T) {
	c := &claims.Claims{UserID: "US123"}
	testContext := contextWithUnverifiedJWT(c)

	parsedClaims, err := claims.ClaimsFromContext(testContext)
	require.NoError(t, err)
	requireClaimsEqual(t, c, parsedClaims)
}

func TestInvalidJWTKey(t *testing.T) {
	c := &claims.Claims{UserID: "US123"}
	testContext := contextWithUnverifiedJWT(c)

	// Validation should fail since the JWT above was signed using a different
	// key.
	flags.Set(t, "auth.jwt_key", "foo")
	_, err := claims.ClaimsFromContext(testContext)
	require.ErrorContains(t, err, "signature is invalid")
}

func TestJWTKeyRotation(t *testing.T) {
	c := &claims.Claims{UserID: "US123"}

	// Get JWT signed using old key.
	testContext := contextWithUnverifiedJWT(c)

	// Validate with both keys in place.
	flags.Set(t, "auth.new_jwt_key", "new_jwt_key")
	parsedClaims, err := claims.ClaimsFromContext(testContext)
	require.NoError(t, err)
	requireClaimsEqual(t, c, parsedClaims)

	// Get JWT signed using new key.
	testContext = contextWithUnverifiedJWT(c)
	// Validate with both keys in place.
	flags.Set(t, "auth.new_jwt_key", "new_jwt_key")
	parsedClaims, err = claims.ClaimsFromContext(testContext)
	require.NoError(t, err)
	requireClaimsEqual(t, c, parsedClaims)
}

type fakeAPIKeyGroup struct {
	capabilities           int32
	apiKeyID               string
	userID                 string
	groupID                string
	childGroupIDs          []string
	useGroupOwnedExecutors bool
	cacheEncryptionEnabled bool
	enforceIPRules         bool
	groupStatus            grpb.Group_GroupStatus
}

func (f *fakeAPIKeyGroup) GetCapabilities() int32 {
	return f.capabilities
}

func (f *fakeAPIKeyGroup) GetAPIKeyID() string {
	return f.apiKeyID
}

func (f *fakeAPIKeyGroup) GetUserID() string {
	return f.userID
}

func (f *fakeAPIKeyGroup) GetGroupID() string {
	return f.groupID
}

func (f *fakeAPIKeyGroup) GetChildGroupIDs() []string {
	return f.childGroupIDs
}

func (f *fakeAPIKeyGroup) GetUseGroupOwnedExecutors() bool {
	return f.useGroupOwnedExecutors
}

func (f *fakeAPIKeyGroup) GetCacheEncryptionEnabled() bool {
	return f.cacheEncryptionEnabled
}

func (f *fakeAPIKeyGroup) GetEnforceIPRules() bool {
	return f.enforceIPRules
}

func (f *fakeAPIKeyGroup) GetGroupStatus() grpb.Group_GroupStatus {
	return f.groupStatus
}

func TestAPIKeyGroupClaimsWithRequestContext(t *testing.T) {
	require.NoError(t, claims.Init())
	ctx := context.Background()
	baseGroupID := "GR9000"
	caps := capabilities.AnonymousUserCapabilities
	akg := &fakeAPIKeyGroup{
		groupID:      baseGroupID,
		capabilities: capabilities.ToInt(caps),
		groupStatus:  grpb.Group_ENTERPRISE_GROUP_STATUS,
	}
	c, err := claims.APIKeyGroupClaims(ctx, akg)
	require.NoError(t, err)
	expectedBaseMembership := &interfaces.GroupMembership{
		GroupID:      baseGroupID,
		Capabilities: caps,
	}
	require.Equal(t, baseGroupID, c.GetGroupID())
	require.Equal(t, []string{baseGroupID}, c.GetAllowedGroups())
	require.Equal(t, []*interfaces.GroupMembership{expectedBaseMembership}, c.GetGroupMemberships())
	require.Equal(t, grpb.Group_ENTERPRISE_GROUP_STATUS, c.GetGroupStatus())

	// Should be able to set group ID to the base group ID via request context which should yield same results
	// as before.
	rctx := requestcontext.ContextWithProtoRequestContext(ctx, &ctxpb.RequestContext{GroupId: baseGroupID})
	c, err = claims.APIKeyGroupClaims(rctx, akg)
	require.NoError(t, err)
	require.Equal(t, baseGroupID, c.GetGroupID())
	require.Equal(t, baseGroupID, c.GetAPIKeyInfo().OwnerGroupID)
	require.Equal(t, []string{baseGroupID}, c.GetAllowedGroups())
	require.Equal(t, []*interfaces.GroupMembership{expectedBaseMembership}, c.GetGroupMemberships())

	// Trying to set any other group ID should yield an error.
	rctx = requestcontext.ContextWithProtoRequestContext(ctx, &ctxpb.RequestContext{GroupId: "BADGROUP"})
	_, err = claims.APIKeyGroupClaims(rctx, akg)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))

	// Now update the API key information to have a child group.
	childGroupID := "GR9999"
	akg = &fakeAPIKeyGroup{
		groupID:       baseGroupID,
		childGroupIDs: []string{childGroupID},
		capabilities:  capabilities.ToInt(caps),
		groupStatus:   grpb.Group_FREE_TIER_GROUP_STATUS,
	}

	// Regular call should return the parent group as the effective group.
	c, err = claims.APIKeyGroupClaims(ctx, akg)
	require.NoError(t, err)
	expectedChildMembership := &interfaces.GroupMembership{
		GroupID:      childGroupID,
		Capabilities: caps,
	}
	require.Equal(t, baseGroupID, c.GetGroupID())
	require.Equal(t, baseGroupID, c.GetAPIKeyInfo().OwnerGroupID)
	require.Equal(t, []string{baseGroupID, childGroupID}, c.GetAllowedGroups())
	require.Equal(t, []*interfaces.GroupMembership{expectedBaseMembership, expectedChildMembership}, c.GetGroupMemberships())
	require.Equal(t, grpb.Group_FREE_TIER_GROUP_STATUS, c.GetGroupStatus())

	// Should be able to change the effective group ID to the child group using the request context.
	rctx = requestcontext.ContextWithProtoRequestContext(ctx, &ctxpb.RequestContext{GroupId: childGroupID})
	c, err = claims.APIKeyGroupClaims(rctx, akg)
	require.NoError(t, err)
	require.Equal(t, childGroupID, c.GetGroupID())
	require.Equal(t, baseGroupID, c.GetAPIKeyInfo().OwnerGroupID) // onwer group should still be parent
	require.Equal(t, []string{baseGroupID, childGroupID}, c.GetAllowedGroups())
	require.Equal(t, []*interfaces.GroupMembership{expectedBaseMembership, expectedChildMembership}, c.GetGroupMemberships())

	// Trying to set any other group ID should still yield an error.
	rctx = requestcontext.ContextWithProtoRequestContext(ctx, &ctxpb.RequestContext{GroupId: "BADGROUP"})
	_, err = claims.APIKeyGroupClaims(rctx, akg)
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestExperimentTargetingGroupID(t *testing.T) {
	const (
		adminGroupID    = "GR1"
		nonAdminGroupID = "GR2"
	)

	for _, tc := range []struct {
		name                           string
		grpcMetadata                   metadata.MD
		apiKeyGroupID                  string
		apiKeyCapabilities             []cappb.Capability
		wantExperimentTargetingGroupID string
		wantPermissionDenied           bool
	}{
		{
			name:                           "header not set",
			grpcMetadata:                   nil,
			apiKeyGroupID:                  nonAdminGroupID,
			apiKeyCapabilities:             nil,
			wantExperimentTargetingGroupID: nonAdminGroupID,
			wantPermissionDenied:           false,
		},
		{
			name:                 "header set, but not a server admin",
			grpcMetadata:         metadata.Pairs("x-buildbuddy-experiment.group_id", nonAdminGroupID),
			apiKeyGroupID:        nonAdminGroupID,
			apiKeyCapabilities:   nil,
			wantPermissionDenied: true,
		},
		{
			name:                 "header set, and in the server admin group, but not with admin role",
			grpcMetadata:         metadata.Pairs("x-buildbuddy-experiment.group_id", nonAdminGroupID),
			apiKeyGroupID:        adminGroupID,
			apiKeyCapabilities:   nil,
			wantPermissionDenied: true,
		},
		{
			name:                           "header set, and in the server admin group, with admin role",
			grpcMetadata:                   metadata.Pairs("x-buildbuddy-experiment.group_id", nonAdminGroupID),
			apiKeyGroupID:                  adminGroupID,
			apiKeyCapabilities:             []cappb.Capability{cappb.Capability_ORG_ADMIN},
			wantPermissionDenied:           false,
			wantExperimentTargetingGroupID: nonAdminGroupID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)
			require.NoError(t, claims.Init())

			ctx := t.Context()
			ctx = metadata.NewIncomingContext(ctx, tc.grpcMetadata)
			env := testenv.GetTestEnv(t)
			auth := testauth.NewTestAuthenticator(t, testauth.TestUsers(
				"US1", adminGroupID,
				"US2", nonAdminGroupID,
			))
			env.SetAuthenticator(auth)
			akg := &fakeAPIKeyGroup{
				groupID:      tc.apiKeyGroupID,
				capabilities: capabilities.ToInt(tc.apiKeyCapabilities),
			}

			c, err := claims.APIKeyGroupClaims(ctx, akg)
			if tc.wantPermissionDenied {
				assert.Error(t, err)
				assert.True(t, status.IsPermissionDeniedError(err), "error: %+#v", err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantExperimentTargetingGroupID, c.GetExperimentTargetingGroupID())
			}
		})
	}
}

func TestGroupStatusPropagation(t *testing.T) {
	require.NoError(t, claims.Init())
	statuses := []grpb.Group_GroupStatus{
		grpb.Group_UNKNOWN_GROUP_STATUS,
		grpb.Group_FREE_TIER_GROUP_STATUS,
		grpb.Group_ENTERPRISE_GROUP_STATUS,
		grpb.Group_ENTERPRISE_TRIAL_GROUP_STATUS,
		grpb.Group_BLOCKED_GROUP_STATUS,
	}

	for _, status := range statuses {
		t.Run(grpb.Group_GroupStatus_name[int32(status)], func(t *testing.T) {
			ctx := context.Background()
			akg := &fakeAPIKeyGroup{
				groupID:      "GR1234",
				capabilities: capabilities.ToInt(capabilities.AnonymousUserCapabilities),
				groupStatus:  status,
			}

			c, err := claims.APIKeyGroupClaims(ctx, akg)
			require.NoError(t, err)
			require.Equal(t, status, c.GetGroupStatus())
		})
	}
}

func TestAssembleJWT_ES256(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	c := &claims.Claims{UserID: "US123", GroupID: "GR456"}
	tokenString, err := claims.AssembleJWT(c, jwt.SigningMethodES256)
	require.NoError(t, err)
	require.NotEmpty(t, tokenString)

	// Verify it's actually an ES256 token by parsing the header
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, &claims.Claims{})
	require.NoError(t, err)
	require.Equal(t, "ES256", token.Method.Alg())
}

func TestAssembleJWT_ES256_UsesNewKeyWhenSet(t *testing.T) {
	keyPair1 := testkeys.GenerateES256KeyPair(t)
	keyPair2 := testkeys.GenerateES256KeyPair(t)

	flags.Set(t, "auth.jwt_es256_private_key", keyPair1.PrivateKeyPEM)
	flags.Set(t, "auth.new_jwt_es256_private_key", keyPair2.PrivateKeyPEM)
	flags.Set(t, "auth.sign_using_new_jwt_key", true)
	require.NoError(t, claims.Init())

	c := &claims.Claims{UserID: "US123", GroupID: "GR456"}
	tokenString, err := claims.AssembleJWT(c, jwt.SigningMethodES256)
	require.NoError(t, err)
	require.NotEmpty(t, tokenString)

	// Verify the JWT was indeed signed with keyPair2
	pubKey, err := jwt.ParseECPublicKeyFromPEM([]byte(keyPair2.PublicKeyPEM))
	require.NoError(t, err)

	parsedClaims := &claims.Claims{}
	_, err = jwt.ParseWithClaims(tokenString, parsedClaims, func(token *jwt.Token) (interface{}, error) {
		return pubKey, nil
	})
	require.NoError(t, err)
	require.Equal(t, "US123", parsedClaims.UserID)
}

func TestAssembleJWT_ES256_NoPrivateKey(t *testing.T) {
	flags.Set(t, "auth.jwt_es256_private_key", "")
	flags.Set(t, "auth.new_jwt_es256_private_key", "")
	require.NoError(t, claims.Init())

	c := &claims.Claims{UserID: "US123"}
	_, err := claims.AssembleJWT(c, jwt.SigningMethodES256)
	require.Error(t, err)
}

func TestAssembleJWT_UnsupportedSigningMethod(t *testing.T) {
	c := &claims.Claims{UserID: "US123"}
	_, err := claims.AssembleJWT(c, jwt.SigningMethodRS256)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unsupported JWT signing method")
}

func TestGetES256PublicKeys_NoKeys(t *testing.T) {
	flags.Set(t, "auth.jwt_es256_private_key", "")
	flags.Set(t, "auth.new_jwt_es256_private_key", "")
	require.NoError(t, claims.Init())

	keys := claims.GetES256PublicKeys()
	require.Empty(t, keys)
}

func TestGetES256PublicKeys_OnlyOldKey(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	flags.Set(t, "auth.new_jwt_es256_private_key", "")
	require.NoError(t, claims.Init())

	keys := claims.GetES256PublicKeys()
	require.Len(t, keys, 1)
}

func TestGetES256PublicKeys_OnlyNewKey(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", "")
	flags.Set(t, "auth.new_jwt_es256_private_key", keyPair.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	keys := claims.GetES256PublicKeys()
	require.Len(t, keys, 1)
}

func TestGetES256PublicKeys_BothKeys(t *testing.T) {
	keyPair1 := testkeys.GenerateES256KeyPair(t)
	keyPair2 := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair1.PrivateKeyPEM)
	flags.Set(t, "auth.new_jwt_es256_private_key", keyPair2.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	keys := claims.GetES256PublicKeys()
	require.Len(t, keys, 2)
}

func TestParseClaims_ES256(t *testing.T) {
	keyPair := testkeys.GenerateES256KeyPair(t)
	flags.Set(t, "auth.jwt_es256_private_key", keyPair.PrivateKeyPEM)
	require.NoError(t, claims.Init())

	c := &claims.Claims{UserID: "US123", GroupID: "GR456"}
	tokenString, err := claims.AssembleJWT(c, jwt.SigningMethodES256)
	require.NoError(t, err)
	require.NotEmpty(t, tokenString)

	keyProvider := func(ctx context.Context) ([]string, error) {
		return []string{keyPair.PublicKeyPEM}, nil
	}
	parser, err := claims.NewClaimsParser(keyProvider)
	require.NoError(t, err)

	parsedClaims, err := parser.Parse(t.Context(), tokenString)
	require.NoError(t, err)
	require.Equal(t, "US123", parsedClaims.UserID)
	require.Equal(t, "GR456", parsedClaims.GroupID)
}
