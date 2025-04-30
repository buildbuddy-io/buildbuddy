package authutil_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func TestParseAPIKeyFromString(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:    "non-empty key",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=abc123'",
			want:    "abc123",
			wantErr: false,
		},
		{
			name:    "empty key",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key='",
			want:    "",
			wantErr: true,
		},
		{
			name:    "empty key in the middle",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=' --bes_backend=grpc://localhost:1985",
			want:    "",
			wantErr: true,
		},
		{
			name:    "key not set",
			input:   "--bes_results_url=http://localhost:8080/invocation/",
			want:    "",
			wantErr: false,
		},
		{
			name:    "multiple API keys",
			input:   "--bes_results_url=http://localhost:8080/invocation/ --remote_header='x-buildbuddy-api-key=abc123' --remote_header='x-buildbuddy-api-key=abc456",
			want:    "abc456",
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := authutil.ParseAPIKeyFromString(tc.input)
			assert.Equal(t, tc.want, output)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
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
			name:                "authenticated user with owner read perms",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:           &uidpb.UserId{Id: "US1"},
				OwnerPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: nil,
		},
		{
			name:                "authenticated user with group read perms",
			authenticatedUserID: "US2",
			acl: &aclpb.ACL{
				UserId:           &uidpb.UserId{Id: "US1"},
				GroupId:          "GR1",
				OwnerPermissions: &aclpb.ACL_Permissions{Read: true},
				GroupPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: nil,
		},
		{
			name:                "authenticated user without owner or group read perms",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:           &uidpb.UserId{Id: "US3"},
				GroupId:          "GR3",
				OwnerPermissions: &aclpb.ACL_Permissions{Read: true},
				GroupPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
		{
			name:                "anonymous user with others read perms",
			authenticatedUserID: "",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: nil,
		},
		{
			name:                "anonymous user with others read perms but anonymous auth disabled",
			authenticatedUserID: "",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				OwnerPermissions:  &aclpb.ACL_Permissions{Read: true},
				OthersPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			anonAuthDisabled: true,
			// TODO: if anonymous usage is disabled, we should always return an
			// error. This test just verifies the old behavior of
			// perms.AuthorizeRead, before we migrated it to authutil.
			wantErr:          nil,
			wantAnonymousErr: false,
		},
		{
			name:                "anonymous user without others read perms",
			authenticatedUserID: "",
			acl: &aclpb.ACL{
				UserId:           &uidpb.UserId{Id: "US1"},
				OwnerPermissions: &aclpb.ACL_Permissions{Read: true},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := real_environment.NewRealEnv(nil)
			ta := testauth.NewTestAuthenticator(testauth.TestUsers(
				// US1 and US2 are both in GR1.
				"US1", "GR1",
				"US2", "GR1",
				// US3 is in GR3.
				"US3", "GR3",
			))
			env.SetAuthenticator(ta)
			ctx := context.Background()
			if test.authenticatedUserID != "" {
				c, err := ta.WithAuthenticatedUser(ctx, test.authenticatedUserID)
				require.NoError(t, err)
				ctx = c
			}
			if test.anonAuthDisabled {
				ta.SetAnonymousUsageEnabled(false)
			}

			err := authutil.AuthorizeRead(ctx, env.GetAuthenticator(), test.acl)

			if test.wantAnonymousErr {
				require.True(t, authutil.IsAnonymousUserError(err), "want anonymous user error, got %v", err)
			} else if test.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, status.Code(test.wantErr).String(), status.Code(err).String())
				require.Equal(t, status.Message(test.wantErr), status.Message(err))
			}
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
			name:                "authenticated user with owner write perms",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:           &uidpb.UserId{Id: "US1"},
				OwnerPermissions: &aclpb.ACL_Permissions{Write: true},
			},
			wantErr: nil,
		},
		{
			name:                "authenticated user with group write perms",
			authenticatedUserID: "US2",
			acl: &aclpb.ACL{
				UserId:           &uidpb.UserId{Id: "US1"},
				GroupId:          "GR1",
				OwnerPermissions: &aclpb.ACL_Permissions{Write: true},
				GroupPermissions: &aclpb.ACL_Permissions{Write: true},
			},
			wantErr: nil,
		},
		{
			name:                "authenticated user without owner or group write perms",
			authenticatedUserID: "US1",
			acl: &aclpb.ACL{
				UserId:           &uidpb.UserId{Id: "US3"},
				GroupId:          "GR3",
				OwnerPermissions: &aclpb.ACL_Permissions{Write: true},
				GroupPermissions: &aclpb.ACL_Permissions{Write: true},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
		{
			name:                "anonymous user with others write perms",
			authenticatedUserID: "",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				OwnerPermissions:  &aclpb.ACL_Permissions{Write: true},
				OthersPermissions: &aclpb.ACL_Permissions{Write: true},
			},
			// OTHERS_WRITE should be ignored (a warning will be logged)
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
		{
			name:                "anonymous user with others write perms but anonymous auth disabled",
			authenticatedUserID: "",
			acl: &aclpb.ACL{
				UserId:            &uidpb.UserId{Id: "US1"},
				OwnerPermissions:  &aclpb.ACL_Permissions{Write: true},
				OthersPermissions: &aclpb.ACL_Permissions{Write: true},
			},
			anonAuthDisabled: true,
			wantAnonymousErr: true,
		},
		{
			name:                "anonymous user without others write perms",
			authenticatedUserID: "",
			acl: &aclpb.ACL{
				UserId:           &uidpb.UserId{Id: "US1"},
				OwnerPermissions: &aclpb.ACL_Permissions{Write: true},
			},
			wantErr: status.PermissionDeniedError("You do not have permission to perform this action."),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := real_environment.NewRealEnv(nil)
			ta := testauth.NewTestAuthenticator(testauth.TestUsers(
				"US1", "GR1",
				"US2", "GR1",
				"US3", "GR3",
			))
			env.SetAuthenticator(ta)
			ctx := context.Background()
			if test.authenticatedUserID != "" {
				c, err := ta.WithAuthenticatedUser(ctx, test.authenticatedUserID)
				require.NoError(t, err)
				ctx = c
			}
			if test.anonAuthDisabled {
				ta.SetAnonymousUsageEnabled(false)
			}

			err := authutil.AuthorizeWrite(ctx, env.GetAuthenticator(), test.acl)

			if test.wantAnonymousErr {
				require.True(t, authutil.IsAnonymousUserError(err), "want anonymous user error, got %v", err)
			} else if test.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, status.Code(test.wantErr).String(), status.Code(err).String())
				require.Equal(t, status.Message(test.wantErr), status.Message(err))
			}
		})
	}
}
