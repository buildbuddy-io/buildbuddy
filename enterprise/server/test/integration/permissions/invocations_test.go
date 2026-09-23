package permissions_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/permissionstest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func invocationACL(userID, groupID string, permissions int32) *aclpb.ACL {
	return perms.ToACLProto(&uidpb.UserId{Id: userID}, groupID, permissions)
}

func seedInvocation(t *testing.T, f *permissionstest.Fixture, id, ownerID, groupID string, permissions int32) {
	t.Helper()
	require.NoError(t, f.App.DB().Create(&tables.Invocation{
		InvocationID: id,
		UserID:       ownerID,
		GroupID:      groupID,
		Perms:        permissions,
	}).Error)
}

func readInvocation(t *testing.T, f *permissionstest.Fixture, id string) (tables.Invocation, bool) {
	t.Helper()
	var count int64
	require.NoError(t, f.App.DB().Model(&tables.Invocation{}).Where("invocation_id = ?", id).Count(&count).Error)
	if count == 0 {
		return tables.Invocation{}, false
	}
	var inv tables.Invocation
	require.NoError(t, f.App.DB().Where("invocation_id = ?", id).Take(&inv).Error)
	return inv, true
}

func enableInvocationSharing(t *testing.T, f *permissionstest.Fixture, groupID, adminName string) {
	t.Helper()
	var org tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", groupID).Take(&org).Error)
	admin := f.Login(t, f.Users[adminName])
	require.NoError(t, admin.RPC("UpdateGroup", &grpb.UpdateGroupRequest{
		RequestContext:                    &ctxpb.RequestContext{GroupId: groupID},
		Id:                                groupID,
		Name:                              org.Name,
		UrlIdentifier:                     org.URLIdentifier,
		SharingEnabled:                    true,
		UseGroupOwnedExecutors:            org.UseGroupOwnedExecutors,
		SuggestionPreference:              org.SuggestionPreference,
		UserOwnedKeysEnabled:              org.UserOwnedKeysEnabled,
		RestrictCleanWorkflowRunsToAdmins: org.RestrictCleanWorkflowRunsToAdmins,
		BotSuggestionsEnabled:             org.BotSuggestionsEnabled,
		DeveloperOrgCreationEnabled:       org.DeveloperOrgCreationEnabled,
		CodeSearchEnabled:                 org.CodeSearchEnabled,
	}, &grpb.UpdateGroupResponse{}))
	var updated tables.Group
	require.NoError(t, f.App.DB().Where("group_id = ?", groupID).Take(&updated).Error)
	require.True(t, updated.SharingEnabled)
}

func TestUpdateInvocationOwnerGroupAndCrossOrgPermissions(t *testing.T) {
	f := permissionstest.New(t)
	enableInvocationSharing(t, f, f.OrgA, permissionstest.AdminName)

	const (
		ownerOnly = perms.OWNER_READ | perms.OWNER_WRITE
		groupACL  = perms.OWNER_READ | perms.OWNER_WRITE | perms.GROUP_READ | perms.GROUP_WRITE
		publicACL = groupACL | perms.OTHERS_READ
	)
	tests := []struct {
		name          string
		actor         string
		owner         string
		resourceGroup func(*permissionstest.Fixture) string
		requestGroup  func(*permissionstest.Fixture) string
		initial       int32
		wantCode      codes.Code
		wantUpdated   bool
	}{
		{name: "owner", actor: permissionstest.AdminName, owner: permissionstest.AdminName, resourceGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: ownerOnly, wantUpdated: true},
		{name: "same_org_non_owner_developer", actor: permissionstest.DeveloperName, owner: permissionstest.AdminName, resourceGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: groupACL, wantUpdated: true},
		{name: "same_org_non_owner_writer", actor: permissionstest.WriterName, owner: permissionstest.AdminName, resourceGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: groupACL, wantUpdated: true},
		{name: "same_org_non_owner_reader", actor: permissionstest.ReaderName, owner: permissionstest.AdminName, resourceGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: groupACL, wantUpdated: true},
		{name: "same_org_non_owner_without_group_write", actor: permissionstest.ReaderName, owner: permissionstest.AdminName, resourceGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: ownerOnly, wantCode: codes.PermissionDenied},
		{name: "same_org_read_only", actor: permissionstest.ReaderName, owner: permissionstest.AdminName, resourceGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: perms.OWNER_READ | perms.OWNER_WRITE | perms.GROUP_READ, wantCode: codes.PermissionDenied},
		{name: "foreign_org_public", actor: permissionstest.OutsiderName, owner: permissionstest.AdminName, resourceGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgB }, initial: publicACL, wantCode: codes.PermissionDenied},
		{name: "foreign_org_admin", actor: permissionstest.OutsiderName, owner: permissionstest.AdminName, resourceGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgB }, initial: groupACL, wantCode: codes.PermissionDenied},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := "update-permissions-" + tc.name
			groupID := tc.resourceGroup(f)
			seedInvocation(t, f, id, f.Users[tc.owner].ID, groupID, tc.initial)
			c := f.Login(t, f.Users[tc.actor])
			err := c.RPC("UpdateInvocation", &inpb.UpdateInvocationRequest{
				RequestContext: &ctxpb.RequestContext{GroupId: tc.requestGroup(f)},
				InvocationId:   id,
				Acl:            invocationACL(f.Users[tc.owner].ID, groupID, publicACL),
			}, &inpb.UpdateInvocationResponse{})
			if tc.wantUpdated {
				require.NoError(t, err, "case %d", i)
				got, ok := readInvocation(t, f, id)
				require.True(t, ok)
				require.EqualValues(t, publicACL, got.Perms)
				return
			}
			requireRPCCode(t, err, tc.wantCode)
			got, ok := readInvocation(t, f, id)
			require.True(t, ok)
			require.EqualValues(t, tc.initial, got.Perms, "denied update must leave ACL unchanged")
		})
	}
}

func TestDeleteInvocationOwnerGroupAndCrossOrgPermissions(t *testing.T) {
	f := permissionstest.New(t)
	const (
		ownerOnly = perms.OWNER_READ | perms.OWNER_WRITE
		groupACL  = perms.OWNER_READ | perms.OWNER_WRITE | perms.GROUP_READ | perms.GROUP_WRITE
	)
	tests := []struct {
		name         string
		actor        string
		requestGroup func(*permissionstest.Fixture) string
		initial      int32
		wantCode     codes.Code
		wantDeleted  bool
	}{
		{name: "owner", actor: permissionstest.AdminName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: ownerOnly, wantDeleted: true},
		{name: "same_org_non_owner_developer", actor: permissionstest.DeveloperName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: groupACL, wantDeleted: true},
		{name: "same_org_non_owner_writer", actor: permissionstest.WriterName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: groupACL, wantDeleted: true},
		{name: "same_org_non_owner_reader", actor: permissionstest.ReaderName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: groupACL, wantDeleted: true},
		{name: "same_org_non_owner_without_group_access", actor: permissionstest.ReaderName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: ownerOnly, wantCode: codes.NotFound},
		{name: "owner_read_only", actor: permissionstest.AdminName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: perms.OWNER_READ, wantCode: codes.NotFound},
		{name: "same_org_read_only", actor: permissionstest.ReaderName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgA }, initial: perms.OWNER_READ | perms.OWNER_WRITE | perms.GROUP_READ, wantCode: codes.NotFound},
		{name: "foreign_org_public", actor: permissionstest.OutsiderName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgB }, initial: groupACL | perms.OTHERS_READ, wantCode: codes.NotFound},
		{name: "foreign_org_admin", actor: permissionstest.OutsiderName, requestGroup: func(f *permissionstest.Fixture) string { return f.OrgB }, initial: groupACL, wantCode: codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := "delete-permissions-" + tc.name
			seedInvocation(t, f, id, f.Users[permissionstest.AdminName].ID, f.OrgA, tc.initial)
			c := f.Login(t, f.Users[tc.actor])
			err := c.RPC("DeleteInvocation", &inpb.DeleteInvocationRequest{
				RequestContext: &ctxpb.RequestContext{GroupId: tc.requestGroup(f)},
				InvocationId:   id,
			}, &inpb.DeleteInvocationResponse{})
			if tc.wantDeleted {
				require.NoError(t, err)
				_, ok := readInvocation(t, f, id)
				require.False(t, ok)
				return
			}
			requireRPCCode(t, err, tc.wantCode)
			got, ok := readInvocation(t, f, id)
			require.True(t, ok)
			require.EqualValues(t, tc.initial, got.Perms, "denied delete must leave invocation unchanged")
		})
	}
}
