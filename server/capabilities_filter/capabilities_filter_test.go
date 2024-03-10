package capabilities_filter_test

import (
	"context"
	"path"
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/capabilities_filter"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/assert"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
)

func TestAllRPCsHaveExplicitCapabilitiesSpecified(t *testing.T) {
	serviceMethodNames := []string{}
	buildbuddyServiceType := reflect.TypeOf((*bbspb.BuildBuddyServiceServer)(nil)).Elem()
	for i := 0; i < buildbuddyServiceType.NumMethod(); i++ {
		serviceMethodNames = append(serviceMethodNames, buildbuddyServiceType.Method(i).Name)
	}
	apiServiceType := reflect.TypeOf((*apipb.ApiServiceServer)(nil)).Elem()
	for i := 0; i < apiServiceType.NumMethod(); i++ {
		serviceMethodNames = append(serviceMethodNames, apiServiceType.Method(i).Name)
	}

	allDefinedMethods := capabilities_filter.AllRPCsForTestOnly()

	assert.Subset(
		t, allDefinedMethods, serviceMethodNames,
		"All BuildBuddyService RPCs should be added to one of the lists in capabilities_filter.go",
	)
	assert.Subset(
		t, serviceMethodNames, allDefinedMethods,
		"All BuildBuddyService RPCs listed in capabilities_filter.go should be valid BuildBuddy service RPCs. "+
			"(check for typos, or if you deleted an RPC, remove it from capabilities_filter.go)",
	)
}

func TestBuildBuddyServiceRPCsHaveRequestAndResponseContextFields(t *testing.T) {
	type Req interface{ GetRequestContext() *ctxpb.RequestContext }
	type Res interface{ GetResponseContext() *ctxpb.ResponseContext }
	reqType := reflect.TypeOf((*Req)(nil)).Elem()
	resType := reflect.TypeOf((*Res)(nil)).Elem()

	buildbuddyServiceType := reflect.TypeOf((*bbspb.BuildBuddyServiceServer)(nil)).Elem()
	for i := 0; i < buildbuddyServiceType.NumMethod(); i++ {
		methodFunc := buildbuddyServiceType.Method(i).Type
		methodName := buildbuddyServiceType.Method(i).Name
		reqMsg := methodFunc.In(1)
		if !reqMsg.Implements(reqType) {
			assert.Failf(t, "missing request_context field", "BuildBuddyService/%s request message %s must have a field 'context.RequestContext request_context'", methodName, reqMsg)
		}
		resMsg := methodFunc.Out(0)
		if !resMsg.Implements(resType) {
			assert.Failf(t, "missing response_context field", "BuildBuddyService/%s response message %s must have a field 'context.ResponseContext response_context'", methodName, resMsg)
		}
	}
}

func TestAllowedRPCs(t *testing.T) {
	for _, test := range []struct {
		Name               string
		RPC                string
		Anonymous          bool
		Capabilities       []akpb.ApiKey_Capability
		ServerAdminGroupID string
		Allowed            bool
	}{
		{
			Name:      "UnrestrictedRPC_Anonymous_Allowed",
			RPC:       "/buildbuddy.service.BuildBuddyService/GetInvocation",
			Anonymous: true,
			Allowed:   true,
		},
		{
			Name:         "UnrestrictedRPC_NonAdmin_Allowed",
			RPC:          "/buildbuddy.service.BuildBuddyService/GetInvocation",
			Capabilities: []akpb.ApiKey_Capability{},
			Allowed:      true,
		},
		{
			Name:         "UnrestrictedRPC_Admin_Allowed",
			RPC:          "/buildbuddy.service.BuildBuddyService/GetInvocation",
			Capabilities: []akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY},
			Allowed:      true,
		},
		{
			Name:      "OrgMemberRPC_Anonymous_NotAllowed",
			RPC:       "/buildbuddy.service.BuildBuddyService/SearchInvocation",
			Anonymous: true,
			Allowed:   false,
		},
		{
			Name:         "OrgMemberRPC_OrgMember_Allowed",
			RPC:          "/buildbuddy.service.BuildBuddyService/SearchInvocation",
			Capabilities: []akpb.ApiKey_Capability{},
			Allowed:      true,
		},
		{
			Name:         "AdminOnlyRPC_NonAdmin_NotAllowed",
			RPC:          "/buildbuddy.service.BuildBuddyService/UpdateGroup",
			Capabilities: []akpb.ApiKey_Capability{},
			Allowed:      false,
		},
		{
			Name:         "AdminOnlyRPC_Admin_Allowed",
			RPC:          "/buildbuddy.service.BuildBuddyService/UpdateGroup",
			Capabilities: []akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY},
			Allowed:      true,
		},
		{
			Name: "ServerAdminOnly_NonServerAdmin_NotAllowed",
			RPC:  "/buildbuddy.service.BuildBuddyService/ApplyBucket",
			// Note: this user is an org admin but not a server admin.
			Capabilities: []akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY},
			Allowed:      false,
		},
		{
			Name:               "ServerAdminOnly_ServerAdmin_Allowed",
			RPC:                "/buildbuddy.service.BuildBuddyService/ApplyBucket",
			ServerAdminGroupID: "GR1",
			Capabilities:       []akpb.ApiKey_Capability{akpb.ApiKey_ORG_ADMIN_CAPABILITY},
			Allowed:            true,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			ctx := context.Background()
			env := testenv.GetTestEnv(t)
			users := testauth.TestUsers("US1", "GR1")
			ta := testauth.NewTestAuthenticator(users)
			ta.ServerAdminGroupID = test.ServerAdminGroupID
			env.SetAuthenticator(ta)
			u := users["US1"].(*testauth.TestUser)
			u.Capabilities = test.Capabilities
			u.GroupMemberships[0].Capabilities = test.Capabilities
			if !test.Anonymous {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, u)
			}

			authErr := capabilities_filter.AuthorizeRPC(ctx, env, test.RPC)
			allowedRPCs := capabilities_filter.AllowedRPCs(ctx, env, "GR1")

			if test.Allowed {
				assert.NoError(t, authErr)
				assert.Contains(t, allowedRPCs, path.Base(test.RPC))
			} else {
				assert.Error(t, authErr)
				assert.NotContains(t, allowedRPCs, path.Base(test.RPC))
			}
		})
	}
}
