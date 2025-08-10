package claims_test

import (
	"context"
	"slices"
	"testing"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
)

func contextWithUnverifiedJWT(c *claims.Claims) context.Context {
	authCtx := claims.AuthContextWithJWT(context.Background(), c, nil)
	jwt := authCtx.Value(authutil.ContextTokenStringKey).(string)
	return context.WithValue(context.Background(), authutil.ContextTokenStringKey, jwt)
}

func TestJWT(t *testing.T) {
	c := &claims.Claims{UserID: "US123"}
	testContext := contextWithUnverifiedJWT(c)

	parsedClaims, err := claims.ClaimsFromContext(testContext)
	require.NoError(t, err)
	require.Equal(t, c, parsedClaims)
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
	require.Equal(t, c, parsedClaims)

	// Get JWT signed using new key.
	testContext = contextWithUnverifiedJWT(c)
	// Validate with both keys in place.
	flags.Set(t, "auth.new_jwt_key", "new_jwt_key")
	parsedClaims, err = claims.ClaimsFromContext(testContext)
	require.NoError(t, err)
	require.Equal(t, c, parsedClaims)
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

func TestAPIKeyGroupClaimsWithRequestContext(t *testing.T) {
	ctx := context.Background()
	baseGroupID := "GR9000"
	caps := capabilities.AnonymousUserCapabilities
	akg := &fakeAPIKeyGroup{groupID: baseGroupID, capabilities: capabilities.ToInt(caps)}
	c, err := claims.APIKeyGroupClaims(ctx, akg)
	require.NoError(t, err)
	expectedBaseMembership := &interfaces.GroupMembership{
		GroupID:      baseGroupID,
		Capabilities: append(caps, cappb.Capability_GROUP_ACCESS),
		Role:         role.Default,
	}
	require.Equal(t, baseGroupID, c.GetGroupID())
	require.Equal(t, []string{baseGroupID}, c.GetAllowedGroups())
	require.Equal(t, []*interfaces.GroupMembership{expectedBaseMembership}, c.GetGroupMemberships())

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
	}

	// Regular call should return the parent group as the effective group.
	c, err = claims.APIKeyGroupClaims(ctx, akg)
	require.NoError(t, err)
	expectedChildMembership := &interfaces.GroupMembership{
		GroupID:      childGroupID,
		Capabilities: append(caps, cappb.Capability_GROUP_ACCESS),
		Role:         role.Default,
	}
	require.Equal(t, baseGroupID, c.GetGroupID())
	require.Equal(t, baseGroupID, c.GetAPIKeyInfo().OwnerGroupID)
	require.Equal(t, []string{baseGroupID, childGroupID}, c.GetAllowedGroups())
	require.Equal(t, []*interfaces.GroupMembership{expectedBaseMembership, expectedChildMembership}, c.GetGroupMemberships())

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

// This test checks whether a given capability implicitly grants the
// GROUP_ACCESS capability, which grants access to a broad range of access
// to various resources.
//
// When adding a new capability, decide whether that type of API key should
// have broad access to retrieve invocation information, artifacts, group membership, etc.
func TestAPIKeyImplicitGroupAccessCaps(t *testing.T) {
	implicitGroupAccess := map[cappb.Capability]bool{
		cappb.Capability_UNKNOWN_CAPABILITY: false,
		cappb.Capability_CACHE_WRITE:        true,
		cappb.Capability_REGISTER_EXECUTOR:  true,
		cappb.Capability_CAS_WRITE:          true,
		cappb.Capability_ORG_ADMIN:          true,
		cappb.Capability_AUDIT_LOG_READ:     false,
		cappb.Capability_GROUP_ACCESS:       true,
	}
	ctx := context.Background()
	for name, val := range cappb.Capability_value {
		baseGroupID := "GR9000"
		caps := []cappb.Capability{cappb.Capability(val)}
		akg := &fakeAPIKeyGroup{groupID: baseGroupID, capabilities: capabilities.ToInt(caps)}
		c, err := claims.APIKeyGroupClaims(ctx, akg)
		require.NoError(t, err)
		expected, ok := implicitGroupAccess[cappb.Capability(val)]
		require.True(t, ok, "expectation for %q needs to be specified", name)
		actual := slices.Contains(c.Capabilities, cappb.Capability_GROUP_ACCESS)
		if !expected && actual {
			require.FailNow(t, "capability should not grant implicit group access", name)
		} else if expected && !actual {
			require.FailNow(t, "capability should grant implicit group access", name)
		}
	}
}

// Tests that users always have implicit GROUP_ACCESS capability.
func TestUserImplicitGroupAccessCaps(t *testing.T) {
	for name, val := range grpb.Group_Role_value {
		if grpb.Group_Role(val) == grpb.Group_UNKNOWN_ROLE {
			continue
		}
		groupID := "GR9000"
		r, err := role.FromProto(grpb.Group_Role(val))
		require.NoError(t, err, "could not convert proto role %s", name)
		u := &tables.User{
			Groups: []*tables.GroupRole{
				{
					Group: tables.Group{GroupID: groupID},
					Role:  uint32(r),
				},
			},
		}
		c, err := claims.UserClaims(u, groupID)
		require.NoError(t, err)
		require.Contains(t, c.Capabilities, cappb.Capability_GROUP_ACCESS, "role %s should grant implicit group access", name)
	}
}
