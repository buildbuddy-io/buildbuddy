package capabilities_filter_test

import (
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/capabilities_filter"
	"github.com/stretchr/testify/assert"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
)

func TestAllRPCsHaveExplicitRolesSpecified(t *testing.T) {
	serviceMethodNames := []string{}
	buildbuddyServiceType := reflect.TypeOf((*bbspb.BuildBuddyServiceServer)(nil)).Elem()
	for i := 0; i < buildbuddyServiceType.NumMethod(); i++ {
		serviceMethodNames = append(serviceMethodNames, buildbuddyServiceType.Method(i).Name)
	}
	apiServiceType := reflect.TypeOf((*apipb.ApiServiceServer)(nil)).Elem()
	for i := 0; i < apiServiceType.NumMethod(); i++ {
		serviceMethodNames = append(serviceMethodNames, apiServiceType.Method(i).Name)
	}

	allDefinedMethods := []string{}
	allDefinedMethods = append(allDefinedMethods, capabilities_filter.RoleIndependentRPCs()...)
	allDefinedMethods = append(allDefinedMethods, capabilities_filter.GroupAdminOnlyRPCs()...)
	allDefinedMethods = append(allDefinedMethods, capabilities_filter.GroupDeveloperRPCs()...)
	allDefinedMethods = append(allDefinedMethods, capabilities_filter.ServerAdminOnlyRPCs()...)

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
