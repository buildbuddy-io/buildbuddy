package permissions_test

import (
	"net/http"
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/permissionstest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func requestContext(c *permissionstest.Client, groupID string) *ctxpb.RequestContext {
	return &ctxpb.RequestContext{UserId: c.RequestContext.GetUserId(), GroupId: groupID}
}

func requireRPCCode(t *testing.T, err error, code codes.Code) *permissionstest.RPCError {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, status.Code(err))
	var rpcErr *permissionstest.RPCError
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, http.StatusInternalServerError, rpcErr.HTTPStatus)
	return rpcErr
}

func capabilitiesByGroup(rsp *uspb.GetUserResponse) map[string][]cappb.Capability {
	out := make(map[string][]cappb.Capability, len(rsp.GetUserGroup()))
	for _, g := range rsp.GetUserGroup() {
		out[g.GetId()] = g.GetCapabilities()
	}
	return out
}

func labels(rsp *akpb.GetApiKeysResponse) []string {
	out := make([]string, 0, len(rsp.GetApiKey()))
	for _, k := range rsp.GetApiKey() {
		out = append(out, k.GetLabel())
	}
	slices.Sort(out)
	return out
}

func TestGetUserReportsPerOrganizationCapabilitiesAndAllowedRPCs(t *testing.T) {
	f := permissionstest.New(t)
	tests := []struct {
		name          string
		selectedGroup string
		wantGroups    map[string][]cappb.Capability
		wantAdminRPCs bool
		wantMemberRPC bool
	}{
		{
			name:          permissionstest.AdminName,
			selectedGroup: f.OrgA,
			wantGroups: map[string][]cappb.Capability{
				f.OrgA: {cappb.Capability_CACHE_WRITE, cappb.Capability_CAS_WRITE, cappb.Capability_ORG_ADMIN},
			},
			wantAdminRPCs: true,
			wantMemberRPC: true,
		},
		{
			name:          permissionstest.DeveloperName,
			selectedGroup: f.OrgA,
			wantGroups:    map[string][]cappb.Capability{f.OrgA: {cappb.Capability_CAS_WRITE}},
			wantMemberRPC: true,
		},
		{
			name:          permissionstest.WriterName,
			selectedGroup: f.OrgA,
			wantGroups:    map[string][]cappb.Capability{f.OrgA: {cappb.Capability_CACHE_WRITE, cappb.Capability_CAS_WRITE}},
			wantMemberRPC: true,
		},
		{
			name:          permissionstest.ReaderName,
			selectedGroup: f.OrgA,
			wantGroups:    map[string][]cappb.Capability{f.OrgA: nil},
			wantMemberRPC: true,
		},
		{
			name:          permissionstest.OutsiderName,
			selectedGroup: f.OrgB,
			wantGroups: map[string][]cappb.Capability{
				f.OrgB: {cappb.Capability_CACHE_WRITE, cappb.Capability_CAS_WRITE, cappb.Capability_ORG_ADMIN},
			},
			wantAdminRPCs: true,
			wantMemberRPC: true,
		},
		{
			name:          permissionstest.DualName,
			selectedGroup: f.OrgA,
			wantGroups: map[string][]cappb.Capability{
				f.OrgA: nil,
				f.OrgB: {cappb.Capability_CACHE_WRITE, cappb.Capability_CAS_WRITE, cappb.Capability_ORG_ADMIN},
			},
			wantMemberRPC: true,
		},
		{
			name:          permissionstest.GrouplessName,
			selectedGroup: f.OrgA,
			wantGroups:    map[string][]cappb.Capability{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := f.Login(t, f.Users[tc.name])
			rsp := &uspb.GetUserResponse{}
			require.NoError(t, c.RPC("GetUser", &uspb.GetUserRequest{
				RequestContext: requestContext(c, tc.selectedGroup),
			}, rsp))
			require.Equal(t, f.Users[tc.name].ID, rsp.GetDisplayUser().GetUserId().GetId())
			require.Equal(t, tc.wantGroups, capabilitiesByGroup(rsp))

			if len(tc.wantGroups) == 0 {
				require.Empty(t, rsp.GetSelectedGroupId())
				require.Equal(t, uspb.SelectedGroup_DENIED, rsp.GetSelectedGroup().GetAccess())
			} else {
				require.Equal(t, tc.selectedGroup, rsp.GetSelectedGroupId())
				require.Equal(t, uspb.SelectedGroup_ALLOWED, rsp.GetSelectedGroup().GetAccess())
			}
			require.Equal(t, tc.wantAdminRPCs, slices.Contains(rsp.GetAllowedRpc(), "UpdateGroup"))
			require.Equal(t, tc.wantAdminRPCs, slices.Contains(rsp.GetAllowedRpc(), "UpdateGroupUsers"))
			require.Equal(t, tc.wantAdminRPCs, slices.Contains(rsp.GetAllowedRpc(), "CreateApiKey"))
			require.Equal(t, tc.wantMemberRPC, slices.Contains(rsp.GetAllowedRpc(), "GetApiKeys"))
			require.Equal(t, tc.wantMemberRPC, slices.Contains(rsp.GetAllowedRpc(), "CreateUserApiKey"))
		})
	}

	// The dual user is a reader in A but an admin in B; changing only the
	// request context must recompute the selected-org capabilities.
	dual := f.Login(t, f.Users[permissionstest.DualName])
	rsp := &uspb.GetUserResponse{}
	require.NoError(t, dual.RPC("GetUser", &uspb.GetUserRequest{RequestContext: requestContext(dual, f.OrgB)}, rsp))
	require.Equal(t, f.OrgB, rsp.GetSelectedGroupId())
	require.Contains(t, rsp.GetAllowedRpc(), "UpdateGroup")
	require.Contains(t, rsp.GetAllowedRpc(), "CreateApiKey")
}

func TestOrganizationSettingsAndMembershipRequireSelectedOrgAdmin(t *testing.T) {
	f := permissionstest.New(t)
	admin := f.Login(t, f.Users[permissionstest.AdminName])

	members := &grpb.GetGroupUsersResponse{}
	require.NoError(t, admin.RPC("GetGroupUsers", &grpb.GetGroupUsersRequest{
		RequestContext:        requestContext(admin, f.OrgA),
		GroupId:               f.OrgA,
		GroupMembershipStatus: []grpb.GroupMembershipStatus{grpb.GroupMembershipStatus_MEMBER},
	}, members))
	gotRoles := map[string]grpb.Group_Role{}
	for _, member := range members.GetUser() {
		gotRoles[member.GetUser().GetUserId().GetId()] = member.GetRole()
	}
	require.Equal(t, map[string]grpb.Group_Role{
		f.Users[permissionstest.AdminName].ID:     grpb.Group_ADMIN_ROLE,
		f.Users[permissionstest.DeveloperName].ID: grpb.Group_DEVELOPER_ROLE,
		f.Users[permissionstest.WriterName].ID:    grpb.Group_WRITER_ROLE,
		f.Users[permissionstest.ReaderName].ID:    grpb.Group_READER_ROLE,
		f.Users[permissionstest.DualName].ID:      grpb.Group_READER_ROLE,
	}, gotRoles)

	require.NoError(t, admin.RPC("UpdateGroup", &grpb.UpdateGroupRequest{
		RequestContext:              requestContext(admin, f.OrgA),
		Id:                          f.OrgA,
		Name:                        "Organization A updated by admin",
		UserOwnedKeysEnabled:        true,
		BotSuggestionsEnabled:       true,
		DeveloperOrgCreationEnabled: true,
	}, &grpb.UpdateGroupResponse{}))
	var orgA tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgA).Take(&orgA).Error)
	require.Equal(t, "Organization A updated by admin", orgA.Name)

	for _, name := range []string{
		permissionstest.DeveloperName,
		permissionstest.WriterName,
		permissionstest.ReaderName,
		permissionstest.DualName,
	} {
		t.Run("non_admin_"+name, func(t *testing.T) {
			c := f.Login(t, f.Users[name])
			err := c.RPC("UpdateGroup", &grpb.UpdateGroupRequest{
				RequestContext:              requestContext(c, f.OrgA),
				Id:                          f.OrgA,
				Name:                        "unauthorized-" + name,
				UserOwnedKeysEnabled:        true,
				BotSuggestionsEnabled:       true,
				DeveloperOrgCreationEnabled: true,
			}, &grpb.UpdateGroupResponse{})
			rpcErr := requireRPCCode(t, err, codes.PermissionDenied)
			require.Equal(t, "permission denied", rpcErr.Message)

			err = c.RPC("GetGroupUsers", &grpb.GetGroupUsersRequest{
				RequestContext:        requestContext(c, f.OrgA),
				GroupId:               f.OrgA,
				GroupMembershipStatus: []grpb.GroupMembershipStatus{grpb.GroupMembershipStatus_MEMBER},
			}, &grpb.GetGroupUsersResponse{})
			requireRPCCode(t, err, codes.PermissionDenied)

			err = c.RPC("UpdateGroupUsers", &grpb.UpdateGroupUsersRequest{
				RequestContext: requestContext(c, f.OrgA),
				GroupId:        f.OrgA,
				Update: []*grpb.UpdateGroupUsersRequest_Update{{
					UserId: &uidpb.UserId{Id: f.Users[permissionstest.ReaderName].ID},
					Role:   grpb.Group_ADMIN_ROLE,
				}},
			}, &grpb.UpdateGroupUsersResponse{})
			requireRPCCode(t, err, codes.PermissionDenied)
		})
	}

	// An Org A admin cannot use an Org A-authenticated request to substitute an
	// Org B resource ID, and an Org B admin cannot forge Org A as selected org.
	err := admin.RPC("GetGroupUsers", &grpb.GetGroupUsersRequest{
		RequestContext:        requestContext(admin, f.OrgA),
		GroupId:               f.OrgB,
		GroupMembershipStatus: []grpb.GroupMembershipStatus{grpb.GroupMembershipStatus_MEMBER},
	}, &grpb.GetGroupUsersResponse{})
	requireRPCCode(t, err, codes.PermissionDenied)

	outsider := f.Login(t, f.Users[permissionstest.OutsiderName])
	err = outsider.RPC("UpdateGroup", &grpb.UpdateGroupRequest{
		RequestContext:              requestContext(outsider, f.OrgA),
		Id:                          f.OrgA,
		Name:                        "forged cross-org update",
		UserOwnedKeysEnabled:        true,
		BotSuggestionsEnabled:       true,
		DeveloperOrgCreationEnabled: true,
	}, &grpb.UpdateGroupResponse{})
	requireRPCCode(t, err, codes.PermissionDenied)

	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgA).Take(&orgA).Error)
	require.Equal(t, "Organization A updated by admin", orgA.Name, "denied writes must not mutate the org")
	var orgB tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgB).Take(&orgB).Error)
	require.Equal(t, "Permissions organization B", orgB.Name, "cross-org attempts must not mutate the other org")
	var readerMembership tables.UserGroup
	require.NoError(t, f.App.DB().Where(
		"user_user_id = ? AND group_group_id = ?",
		f.Users[permissionstest.ReaderName].ID,
		f.OrgA,
	).Take(&readerMembership).Error)
	require.EqualValues(t, grpb.Group_READER_ROLE, readerMembership.Role, "denied membership writes must have no side effects")
}

func TestMembershipDowngradeAndRemovalAffectExistingSession(t *testing.T) {
	f := permissionstest.New(t)
	admin := f.Login(t, f.Users[permissionstest.AdminName])
	developer := f.Login(t, f.Users[permissionstest.DeveloperName])

	created := &akpb.CreateApiKeyResponse{}
	require.NoError(t, developer.RPC("CreateUserApiKey", &akpb.CreateApiKeyRequest{
		RequestContext: requestContext(developer, f.OrgA),
		UserId:         f.Users[permissionstest.DeveloperName].ID,
		Label:          "developer-before-downgrade",
		Capability:     []cappb.Capability{cappb.Capability_CAS_WRITE},
	}, created))
	require.NotEmpty(t, created.GetApiKey().GetId())

	require.NoError(t, admin.RPC("UpdateGroupUsers", &grpb.UpdateGroupUsersRequest{
		RequestContext: requestContext(admin, f.OrgA),
		GroupId:        f.OrgA,
		Update: []*grpb.UpdateGroupUsersRequest_Update{{
			UserId: &uidpb.UserId{Id: f.Users[permissionstest.DeveloperName].ID},
			Role:   grpb.Group_READER_ROLE,
		}},
	}, &grpb.UpdateGroupUsersResponse{}))

	userRsp := &uspb.GetUserResponse{}
	require.NoError(t, developer.RPC("GetUser", &uspb.GetUserRequest{RequestContext: requestContext(developer, f.OrgA)}, userRsp))
	require.Empty(t, capabilitiesByGroup(userRsp)[f.OrgA], "the existing session must observe the downgraded Reader capabilities")

	var countBefore int64
	require.NoError(t, f.App.DB().Model(&tables.APIKey{}).Count(&countBefore).Error)
	err := developer.RPC("CreateUserApiKey", &akpb.CreateApiKeyRequest{
		RequestContext: requestContext(developer, f.OrgA),
		UserId:         f.Users[permissionstest.DeveloperName].ID,
		Label:          "reader-cannot-request-cas-write",
		Capability:     []cappb.Capability{cappb.Capability_CAS_WRITE},
	}, &akpb.CreateApiKeyResponse{})
	rpcErr := requireRPCCode(t, err, codes.PermissionDenied)
	require.Equal(t, "user does not have permission to assign these API key capabilities", rpcErr.Message)
	var countAfter int64
	require.NoError(t, f.App.DB().Model(&tables.APIKey{}).Count(&countAfter).Error)
	require.Equal(t, countBefore, countAfter, "denied key creation must have no side effects")

	require.NoError(t, admin.RPC("UpdateGroupUsers", &grpb.UpdateGroupUsersRequest{
		RequestContext: requestContext(admin, f.OrgA),
		GroupId:        f.OrgA,
		Update: []*grpb.UpdateGroupUsersRequest_Update{{
			UserId:           &uidpb.UserId{Id: f.Users[permissionstest.DeveloperName].ID},
			MembershipAction: grpb.UpdateGroupUsersRequest_Update_REMOVE,
		}},
	}, &grpb.UpdateGroupUsersResponse{}))

	userRsp = &uspb.GetUserResponse{}
	require.NoError(t, developer.RPC("GetUser", &uspb.GetUserRequest{RequestContext: requestContext(developer, f.OrgA)}, userRsp))
	require.NotContains(t, capabilitiesByGroup(userRsp), f.OrgA)
	require.Empty(t, userRsp.GetSelectedGroupId())
	require.Equal(t, uspb.SelectedGroup_DENIED, userRsp.GetSelectedGroup().GetAccess())

	err = developer.RPC("GetUserApiKeys", &akpb.GetApiKeysRequest{
		RequestContext: requestContext(developer, f.OrgA),
		UserId:         f.Users[permissionstest.DeveloperName].ID,
	}, &akpb.GetApiKeysResponse{})
	requireRPCCode(t, err, codes.PermissionDenied)
}

func TestOrganizationAPIKeysAreRoleAndOrganizationScoped(t *testing.T) {
	f := permissionstest.New(t)
	admin := f.Login(t, f.Users[permissionstest.AdminName])
	outsider := f.Login(t, f.Users[permissionstest.OutsiderName])
	developer := f.Login(t, f.Users[permissionstest.DeveloperName])
	reader := f.Login(t, f.Users[permissionstest.ReaderName])

	createOrgKey := func(t *testing.T, c *permissionstest.Client, groupID, label string, visible bool) *akpb.ApiKey {
		t.Helper()
		rsp := &akpb.CreateApiKeyResponse{}
		require.NoError(t, c.RPC("CreateApiKey", &akpb.CreateApiKeyRequest{
			RequestContext:      requestContext(c, groupID),
			Label:               label,
			Capability:          []cappb.Capability{cappb.Capability_CAS_WRITE},
			VisibleToDevelopers: visible,
		}, rsp))
		return rsp.GetApiKey()
	}

	aVisible := createOrgKey(t, admin, f.OrgA, "a-visible", true)
	aHidden := createOrgKey(t, admin, f.OrgA, "a-hidden", false)
	bHidden := createOrgKey(t, outsider, f.OrgB, "b-hidden", false)

	adminAKeys := &akpb.GetApiKeysResponse{}
	require.NoError(t, admin.RPC("GetApiKeys", &akpb.GetApiKeysRequest{RequestContext: requestContext(admin, f.OrgA)}, adminAKeys))
	require.Equal(t, []string{"a-hidden", "a-visible"}, labels(adminAKeys))

	developerKeys := &akpb.GetApiKeysResponse{}
	require.NoError(t, developer.RPC("GetApiKeys", &akpb.GetApiKeysRequest{RequestContext: requestContext(developer, f.OrgA)}, developerKeys))
	require.Equal(t, []string{"a-visible"}, labels(developerKeys), "non-admin members only list developer-visible org keys")

	outsiderKeys := &akpb.GetApiKeysResponse{}
	require.NoError(t, outsider.RPC("GetApiKeys", &akpb.GetApiKeysRequest{RequestContext: requestContext(outsider, f.OrgB)}, outsiderKeys))
	require.Equal(t, []string{"b-hidden"}, labels(outsiderKeys))

	// The deprecated body group_id cannot override the authenticated request
	// context group. This must still return A keys, not B keys.
	forgedBody := &akpb.GetApiKeysResponse{}
	require.NoError(t, admin.RPC("GetApiKeys", &akpb.GetApiKeysRequest{
		RequestContext: requestContext(admin, f.OrgA),
		GroupId:        f.OrgB,
	}, forgedBody))
	require.Equal(t, []string{"a-hidden", "a-visible"}, labels(forgedBody))

	err := admin.RPC("GetApiKey", &akpb.GetApiKeyRequest{
		RequestContext: requestContext(admin, f.OrgA),
		ApiKeyId:       bHidden.GetId(),
	}, &akpb.GetApiKeyResponse{})
	requireRPCCode(t, err, codes.PermissionDenied)

	err = outsider.RPC("GetApiKey", &akpb.GetApiKeyRequest{
		RequestContext: requestContext(outsider, f.OrgB),
		ApiKeyId:       aHidden.GetId(),
	}, &akpb.GetApiKeyResponse{})
	requireRPCCode(t, err, codes.PermissionDenied)

	var countBefore int64
	require.NoError(t, f.App.DB().Model(&tables.APIKey{}).Count(&countBefore).Error)
	for _, tc := range []struct {
		name string
		c    *permissionstest.Client
	}{
		{name: "developer", c: developer},
		{name: "reader", c: reader},
	} {
		t.Run("denied_create_"+tc.name, func(t *testing.T) {
			err := tc.c.RPC("CreateApiKey", &akpb.CreateApiKeyRequest{
				RequestContext: requestContext(tc.c, f.OrgA),
				Label:          "unauthorized-" + tc.name,
			}, &akpb.CreateApiKeyResponse{})
			rpcErr := requireRPCCode(t, err, codes.PermissionDenied)
			require.Equal(t, "permission denied", rpcErr.Message)
		})
	}
	var countAfter int64
	require.NoError(t, f.App.DB().Model(&tables.APIKey{}).Count(&countAfter).Error)
	require.Equal(t, countBefore, countAfter, "denied org-key writes must not insert rows")
	require.NotEmpty(t, aVisible.GetId()) // Positive control for successful writes.
}

func TestPersonalAPIKeysAreOwnerAndOrganizationScoped(t *testing.T) {
	f := permissionstest.New(t)
	admin := f.Login(t, f.Users[permissionstest.AdminName])
	developer := f.Login(t, f.Users[permissionstest.DeveloperName])
	reader := f.Login(t, f.Users[permissionstest.ReaderName])
	outsider := f.Login(t, f.Users[permissionstest.OutsiderName])
	dual := f.Login(t, f.Users[permissionstest.DualName])

	createPersonalKey := func(t *testing.T, c *permissionstest.Client, groupID, userID, label string, caps ...cappb.Capability) *akpb.ApiKey {
		t.Helper()
		rsp := &akpb.CreateApiKeyResponse{}
		require.NoError(t, c.RPC("CreateUserApiKey", &akpb.CreateApiKeyRequest{
			RequestContext: requestContext(c, groupID),
			UserId:         userID,
			Label:          label,
			Capability:     caps,
		}, rsp))
		return rsp.GetApiKey()
	}

	developerKey := createPersonalKey(t, developer, f.OrgA, f.Users[permissionstest.DeveloperName].ID, "developer-a", cappb.Capability_CAS_WRITE)
	readerKey := createPersonalKey(t, reader, f.OrgA, f.Users[permissionstest.ReaderName].ID, "reader-a")
	createPersonalKey(t, outsider, f.OrgB, f.Users[permissionstest.OutsiderName].ID, "outsider-b", cappb.Capability_CAS_WRITE)
	createPersonalKey(t, dual, f.OrgA, f.Users[permissionstest.DualName].ID, "dual-a")
	createPersonalKey(t, dual, f.OrgB, f.Users[permissionstest.DualName].ID, "dual-b", cappb.Capability_CAS_WRITE)

	developerKeys := &akpb.GetApiKeysResponse{}
	require.NoError(t, developer.RPC("GetUserApiKeys", &akpb.GetApiKeysRequest{
		RequestContext: requestContext(developer, f.OrgA),
		UserId:         f.Users[permissionstest.DeveloperName].ID,
	}, developerKeys))
	require.Equal(t, []string{"developer-a"}, labels(developerKeys))

	adminView := &akpb.GetApiKeysResponse{}
	require.NoError(t, admin.RPC("GetUserApiKeys", &akpb.GetApiKeysRequest{
		RequestContext: requestContext(admin, f.OrgA),
		UserId:         f.Users[permissionstest.DeveloperName].ID,
	}, adminView))
	require.Equal(t, []string{"developer-a"}, labels(adminView), "org admins can manage a member's personal keys")

	dualA := &akpb.GetApiKeysResponse{}
	require.NoError(t, dual.RPC("GetUserApiKeys", &akpb.GetApiKeysRequest{
		RequestContext: requestContext(dual, f.OrgA),
		UserId:         f.Users[permissionstest.DualName].ID,
	}, dualA))
	require.Equal(t, []string{"dual-a"}, labels(dualA))
	dualB := &akpb.GetApiKeysResponse{}
	require.NoError(t, dual.RPC("GetUserApiKeys", &akpb.GetApiKeysRequest{
		RequestContext: requestContext(dual, f.OrgB),
		UserId:         f.Users[permissionstest.DualName].ID,
	}, dualB))
	require.Equal(t, []string{"dual-b"}, labels(dualB), "the same user's keys remain isolated by org")

	err := developer.RPC("GetUserApiKey", &akpb.GetApiKeyRequest{
		RequestContext: requestContext(developer, f.OrgA),
		ApiKeyId:       readerKey.GetId(),
	}, &akpb.GetApiKeyResponse{})
	requireRPCCode(t, err, codes.PermissionDenied)

	adminRead := &akpb.GetApiKeyResponse{}
	require.NoError(t, admin.RPC("GetUserApiKey", &akpb.GetApiKeyRequest{
		RequestContext: requestContext(admin, f.OrgA),
		ApiKeyId:       developerKey.GetId(),
	}, adminRead))
	require.Equal(t, developerKey.GetId(), adminRead.GetApiKey().GetId())

	for _, tc := range []struct {
		name    string
		client  *permissionstest.Client
		groupID string
		userID  string
	}{
		{
			name:    "org_a_admin_cannot_target_org_b_user",
			client:  admin,
			groupID: f.OrgA,
			userID:  f.Users[permissionstest.OutsiderName].ID,
		},
		{
			name:    "org_b_admin_cannot_target_org_a_user",
			client:  outsider,
			groupID: f.OrgB,
			userID:  f.Users[permissionstest.DeveloperName].ID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.client.RPC("GetUserApiKeys", &akpb.GetApiKeysRequest{
				RequestContext: requestContext(tc.client, tc.groupID),
				UserId:         tc.userID,
			}, &akpb.GetApiKeysResponse{})
			rpcErr := requireRPCCode(t, err, codes.PermissionDenied)
			require.Equal(t, "user is not a member of the requested group", rpcErr.Message)
		})
	}

	var countBefore int64
	require.NoError(t, f.App.DB().Model(&tables.APIKey{}).Count(&countBefore).Error)
	err = developer.RPC("CreateUserApiKey", &akpb.CreateApiKeyRequest{
		RequestContext: requestContext(developer, f.OrgA),
		UserId:         f.Users[permissionstest.ReaderName].ID,
		Label:          "developer-forged-owner",
	}, &akpb.CreateApiKeyResponse{})
	rpcErr := requireRPCCode(t, err, codes.PermissionDenied)
	require.Equal(t, "org admin permission is required to create an API key for the requested user", rpcErr.Message)
	var countAfter int64
	require.NoError(t, f.App.DB().Model(&tables.APIKey{}).Count(&countAfter).Error)
	require.Equal(t, countBefore, countAfter, "denied personal-key writes must not insert rows")
}
