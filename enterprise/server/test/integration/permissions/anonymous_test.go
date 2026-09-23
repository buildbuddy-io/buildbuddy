package permissions_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/permissionstest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	uspb "github.com/buildbuddy-io/buildbuddy/proto/user"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func TestAnonymousRPCsCannotAccessOrganizationData(t *testing.T) {
	for _, allowAnonymous := range []bool{true, false} {
		t.Run(fmt.Sprintf("anonymous_usage_%t", allowAnonymous), func(t *testing.T) {
			f := permissionstest.New(t, fmt.Sprintf("--auth.enable_anonymous_usage=%t", allowAnonymous))
			admin := f.Login(t, f.Users[permissionstest.AdminName])
			created := &akpb.CreateApiKeyResponse{}
			require.NoError(t, admin.RPC("CreateApiKey", &akpb.CreateApiKeyRequest{
				RequestContext: requestContext(admin, f.OrgA), Label: "private-organization-key",
			}, created))
			knownKey := &akpb.GetApiKeyResponse{}
			require.NoError(t, admin.RPC("GetApiKey", &akpb.GetApiKeyRequest{
				RequestContext: requestContext(admin, f.OrgA), ApiKeyId: created.GetApiKey().GetId(),
			}, knownKey))
			require.NotEmpty(t, knownKey.GetApiKey().GetValue())
			var before tables.Group
			require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgA).Take(&before).Error)
			var countBefore int64
			require.NoError(t, f.App.DB().Model(&tables.APIKey{}).Count(&countBefore).Error)

			anonymous := f.AnonymousClient()
			require.Nil(t, anonymous.HTTPClient.Jar)
			require.Empty(t, anonymous.APIKey)
			// Claimed identity and org IDs in the body are not credentials.
			forged := &ctxpb.RequestContext{GroupId: f.OrgA, UserId: &uidpb.UserId{Id: f.Users[permissionstest.AdminName].ID}}
			identity := &uspb.GetUserResponse{}
			err := anonymous.RPC("GetUser", &uspb.GetUserRequest{RequestContext: forged}, identity)
			requireRPCCode(t, err, codes.PermissionDenied)
			require.Nil(t, identity.GetDisplayUser())

			tests := []struct {
				method   string
				request  proto.Message
				response proto.Message
			}{
				{"GetApiKeys", &akpb.GetApiKeysRequest{RequestContext: forged}, &akpb.GetApiKeysResponse{}},
				{"GetApiKey", &akpb.GetApiKeyRequest{RequestContext: forged, ApiKeyId: created.GetApiKey().GetId()}, &akpb.GetApiKeyResponse{}},
				{"GetUserApiKey", &akpb.GetApiKeyRequest{RequestContext: forged, ApiKeyId: created.GetApiKey().GetId()}, &akpb.GetApiKeyResponse{}},
				{"GetGroupUsers", &grpb.GetGroupUsersRequest{RequestContext: forged, GroupId: f.OrgA}, &grpb.GetGroupUsersResponse{}},
				{"SearchInvocation", &inpb.SearchInvocationRequest{RequestContext: forged, Query: &inpb.InvocationQuery{GroupId: f.OrgA}}, &inpb.SearchInvocationResponse{}},
				{"UpdateGroup", &grpb.UpdateGroupRequest{RequestContext: forged, Id: f.OrgA, Name: "anonymous mutation"}, &grpb.UpdateGroupResponse{}},
				{"CreateApiKey", &akpb.CreateApiKeyRequest{RequestContext: forged, Label: "anonymous key"}, &akpb.CreateApiKeyResponse{}},
				{"CreateUserApiKey", &akpb.CreateApiKeyRequest{RequestContext: forged, UserId: f.Users[permissionstest.AdminName].ID, Label: "anonymous personal key"}, &akpb.CreateApiKeyResponse{}},
			}
			for _, tc := range tests {
				t.Run(tc.method, func(t *testing.T) {
					err := anonymous.RPC(tc.method, tc.request, tc.response)
					requireRPCCodeHTTP(t, err, codes.PermissionDenied, http.StatusForbidden)
					require.NotContains(t, err.Error(), knownKey.GetApiKey().GetValue())
					require.Zero(t, proto.Size(tc.response), "denied responses must contain no data")
				})
			}
			var after tables.Group
			require.NoError(t, f.App.DB().Where("group_id = ?", f.OrgA).Take(&after).Error)
			require.Equal(t, before, after, "denied mutation must not change the org")
			var countAfter int64
			require.NoError(t, f.App.DB().Model(&tables.APIKey{}).Count(&countAfter).Error)
			require.Equal(t, countBefore, countAfter, "anonymous mutations must not create API keys")
		})
	}
}
