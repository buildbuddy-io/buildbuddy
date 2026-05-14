package perms_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func TestUnixPermMapping(t *testing.T) {
	for _, test := range []struct {
		unix int32
		perm int32
	}{
		{
			perm: perms.NONE,
			unix: 0000,
		},
		{
			perm: perms.ALL,
			unix: 0777,
		},
		{
			perm: perms.OWNER_READ | perms.OWNER_WRITE | perms.GROUP_READ | perms.OTHERS_READ,
			unix: 0644,
		},
		{
			perm: perms.OWNER_READ | perms.OWNER_WRITE | perms.OWNER_EXEC | perms.GROUP_READ | perms.GROUP_EXEC | perms.OTHERS_READ | perms.OTHERS_EXEC,
			unix: 0755,
		},
	} {
		t.Run(fmt.Sprintf("%o", test.unix), func(t *testing.T) {
			require.Equal(t, test.unix, test.perm, "want 0%o, got 0%o", test.unix, test.perm)
		})
	}
}

func TestACLConversions(t *testing.T) {
	for _, test := range []struct {
		name string
		p    *perms.UserGroupPerm
	}{
		{
			p: &perms.UserGroupPerm{
				UserID:  "US1",
				GroupID: "GR1",
				Perms:   0000,
			},
		},
		{
			p: &perms.UserGroupPerm{
				UserID:  "US1",
				GroupID: "GR1",
				Perms:   0644,
			},
		},
		{
			p: &perms.UserGroupPerm{
				UserID:  "US1",
				GroupID: "GR1",
				Perms:   0666,
			},
		},
		{
			p: &perms.UserGroupPerm{
				UserID:  "US1",
				GroupID: "GR1",
				Perms:   0660,
			},
		},
		// TODO: support EXEC perms in ACL. These are currently dropped because
		// they are never used.
	} {
		t.Run(fmt.Sprintf("%o", test.p.Perms), func(t *testing.T) {
			acl := perms.ToACLProto(&uidpb.UserId{Id: test.p.UserID}, test.p.GroupID, test.p.Perms)
			perms, err := perms.FromACL(acl)
			require.NoError(t, err)
			require.Equal(t, test.p.Perms, perms, "want 0o%3o, got 0o%3o", test.p.Perms, perms)
		})
	}
}

func TestFromACL_InvalidACL(t *testing.T) {
	for _, test := range []struct {
		name    string
		acl     *aclpb.ACL
		wantErr error
	}{
		{
			name:    "nil ACL",
			acl:     nil,
			wantErr: status.InvalidArgumentError("ACL is nil."),
		},
		{
			name: "missing owner perms",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  nil,
				GroupPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: status.InvalidArgumentError("ACL is missing one or more required permissions fields."),
		},
		{
			name: "missing group perms",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				GroupPermissions:  nil,
				OthersPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: status.InvalidArgumentError("ACL is missing one or more required permissions fields."),
		},
		{
			name: "missing others perms",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				GroupPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: nil,
			},
			wantErr: status.InvalidArgumentError("ACL is missing one or more required permissions fields."),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := perms.FromACL(test.acl)
			require.Equal(t, test.wantErr, err)
		})
	}
}

func TestAuthorizeRead(t *testing.T) {
	for _, test := range []struct {
		name                string
		authenticatedUserID string
		acl                 *aclpb.ACL
		anonAuthDisabled    bool
		wantErr             error
		wantAnonymousErr    bool
	}{
		{
			name:    "nil user",
			acl:     &aclpb.ACL{},
			wantErr: status.InvalidArgumentError("user cannot be nil."),
		},
		{
			name:                "invalid ACL",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: status.InvalidArgumentError("ACL is missing one or more required permissions fields."),
		},
		{
			name:                "authenticated user with owner read perms",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				GroupPermissions:  &aclpb.ACL_Permissions{},
				OthersPermissions: &aclpb.ACL_Permissions{},
			},
			wantErr: nil,
		},
		{
			name:                "authenticated user in the right group but group bits are unset",
			authenticatedUserID: "US2",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				GroupPermissions:  &aclpb.ACL_Permissions{},
				OthersPermissions: &aclpb.ACL_Permissions{},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
		{
			name:                "authenticated user with group read perms",
			authenticatedUserID: "US2",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				GroupPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: &aclpb.ACL_Permissions{},
			},
			wantErr: nil,
		},
		{
			name:                "authenticated user in the right group but trying to read a write-only object",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  &aclpb.ACL_Permissions{Write: true},
				GroupPermissions:  &aclpb.ACL_Permissions{Write: true},
				OthersPermissions: &aclpb.ACL_Permissions{Write: true},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
		{
			name:                "authenticated user not in the right group",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US3"},
				GroupId:           "GR3",
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				GroupPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: &aclpb.ACL_Permissions{},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := real_environment.NewRealEnv(nil)
			ta := testauth.NewTestAuthenticator(t, testauth.TestUsers(
				// US1 and US2 are both in GR1.
				"US1", "GR1",
				"US2", "GR1",
				// US3 is in GR3.
				"US3", "GR3",
			))
			env.SetAuthenticator(ta)
			ctx := context.Background()

			var u interfaces.UserInfo
			if test.authenticatedUserID != "" {
				ctx, err := ta.WithAuthenticatedUser(ctx, test.authenticatedUserID)
				require.NoError(t, err)
				u, err = ta.AuthenticatedUser(ctx)
				require.NoError(t, err)
			}

			err := perms.AuthorizeRead(u, test.acl)

			require.Equal(t, test.wantErr, err)
		})
	}
}

func TestAuthorizeWrite(t *testing.T) {
	for _, test := range []struct {
		name                string
		authenticatedUserID string
		acl                 *aclpb.ACL
		anonAuthDisabled    bool
		wantErr             error
		wantAnonymousErr    bool
	}{
		{
			name:    "nil user",
			acl:     &aclpb.ACL{},
			wantErr: status.InvalidArgumentError("authenticatedUser cannot be nil."),
		},
		{
			name:                "invalid ACL",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: status.InvalidArgumentError("ACL is missing one or more required permissions fields."),
		},
		{
			name:                "authenticated user with owner write perms",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				OwnerPermissions:  &aclpb.ACL_Permissions{Write: true},
				GroupPermissions:  &aclpb.ACL_Permissions{},
				OthersPermissions: &aclpb.ACL_Permissions{},
			},
			wantErr: nil,
		},
		{
			name:                "authenticated user in the right group but group bits are unset",
			authenticatedUserID: "US2",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  &aclpb.ACL_Permissions{Write: true},
				GroupPermissions:  &aclpb.ACL_Permissions{},
				OthersPermissions: &aclpb.ACL_Permissions{},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
		{
			name:                "authenticated user with group write perms",
			authenticatedUserID: "US2",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  &aclpb.ACL_Permissions{Write: true},
				GroupPermissions:  &aclpb.ACL_Permissions{Write: true},
				OthersPermissions: &aclpb.ACL_Permissions{},
			},
			wantErr: nil,
		},
		{
			name:                "authenticated user in the right group but trying to write a readonly object",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				GroupId:           "GR1",
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				GroupPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
		{
			name:                "authenticated user not in the right group",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US3"},
				GroupId:           "GR3",
				OwnerPermissions:  &aclpb.ACL_Permissions{Write: true},
				GroupPermissions:  &aclpb.ACL_Permissions{Write: true},
				OthersPermissions: &aclpb.ACL_Permissions{},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ta := testauth.NewTestAuthenticator(t, testauth.TestUsers(
				"US1", "GR1",
				"US2", "GR1",
				"US3", "GR3",
			))
			ctx := context.Background()

			var u *interfaces.UserInfo
			if test.authenticatedUserID != "" {
				ctx, err := ta.WithAuthenticatedUser(ctx, test.authenticatedUserID)
				require.NoError(t, err)
				user, err := ta.AuthenticatedUser(ctx)
				require.NoError(t, err)
				u = &user
			}

			err := perms.AuthorizeWrite(u, test.acl)

			require.Equal(t, test.wantErr, err)
		})
	}
}
