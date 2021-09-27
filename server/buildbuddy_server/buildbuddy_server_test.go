package buildbuddy_server_test

import (
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/stretchr/testify/assert"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
)

func TestAdminOnlyMethodsReferToBuildBuddyServiceMethods(t *testing.T) {
	serviceMethodNames := []string{}
	serviceType := reflect.TypeOf((*bbspb.BuildBuddyServiceServer)(nil)).Elem()
	for i := 0; i < serviceType.NumMethod(); i++ {
		serviceMethodNames = append(serviceMethodNames, serviceType.Method(i).Name)
	}

	assert.Subset(
		t, serviceMethodNames, buildbuddy_server.AdminAPIMethods,
		"AdminAPIMethods contains a method not defined by BuildBuddyService.")
}
