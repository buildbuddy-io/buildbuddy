package role_filter_test

import (
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/http/role_filter"
	"github.com/stretchr/testify/assert"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
)

func TestAllRPCsHaveExplicitRolesSpecified(t *testing.T) {
	serviceMethodNames := []string{}
	serviceType := reflect.TypeOf((*bbspb.BuildBuddyServiceServer)(nil)).Elem()
	for i := 0; i < serviceType.NumMethod(); i++ {
		serviceMethodNames = append(serviceMethodNames, serviceType.Method(i).Name)
	}

	allDefinedMethods := []string{}
	allDefinedMethods = append(allDefinedMethods, role_filter.RoleIndependentRPCs...)
	allDefinedMethods = append(allDefinedMethods, role_filter.GroupAdminOnlyRPCs...)
	allDefinedMethods = append(allDefinedMethods, role_filter.GroupDeveloperRPCs...)

	assert.Subset(
		t, allDefinedMethods, serviceMethodNames,
		"All BuildBuddyService RPCs should be added to one of the lists in role_filter.go",
	)
	assert.Subset(
		t, serviceMethodNames, allDefinedMethods,
		"All BuildBuddyService RPCs listed in role_filter.go should be valid BuildBuddy service RPCs. "+
			"(check for typos, or if you deleted an RPC, remove it from role_filter.go)",
	)
}
