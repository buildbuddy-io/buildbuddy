package capabilities_test

import (
	"context"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	testauth "github.com/buildbuddy-io/buildbuddy/server/testutil/auth"
	testenv "github.com/buildbuddy-io/buildbuddy/server/testutil/environment"
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t *testing.T, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(users))
	return te
}

func TestToInt_NoCapabilities(t *testing.T) {
	c := capabilities.ToInt([]akpb.ApiKey_Capability{})

	assert.Equal(t, int32(0), c)
}

func TestToInt_OneCapability(t *testing.T) {
	c := capabilities.ToInt([]akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY})

	assert.Equal(t, int32(akpb.ApiKey_CACHE_WRITE_CAPABILITY), c)
}

func TestToInt_MultipleCapabilities(t *testing.T) {
	// TODO: we don't have multiple capabilities yet but should def test it when we do
	t.Skip()
}

func TestFromInt_NoCapabilities(t *testing.T) {
	caps := capabilities.FromInt(0)

	assert.Equal(t, []akpb.ApiKey_Capability{}, caps)
}

func TestFromInt_OneCapability(t *testing.T) {
	caps := capabilities.FromInt(int32(akpb.ApiKey_CACHE_WRITE_CAPABILITY))

	assert.Equal(t, []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY}, caps)
}

func TestFromInt_MultipleCapabilities(t *testing.T) {
	// TODO: we don't have multiple capabilities yet but should def test it when we do
	t.Skip()
}

func TestIsGranted_AnonymousUsageDisabled_AnonymousUser_False(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	flags.Set(t, "auth.enable_anonymous_usage", "false")
	anonCtx := context.Background()

	canWrite, err := capabilities.IsGranted(anonCtx, te, akpb.ApiKey_CACHE_WRITE_CAPABILITY)

	assert.False(t, canWrite)
	assert.Nil(t, err)
}

func TestIsGranted_AnonymousUsageEnabled_AnonymousUser_True(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	flags.Set(t, "auth.enable_anonymous_usage", "true")
	anonCtx := context.Background()

	canWrite, err := capabilities.IsGranted(anonCtx, te, akpb.ApiKey_CACHE_WRITE_CAPABILITY)

	assert.True(t, canWrite)
	assert.Nil(t, err)
}

func TestIsGranted_TestUserWithCapability_True(t *testing.T) {
	user := &auth.Claims{
		UserID:       "US1",
		GroupID:      "GR1",
		Capabilities: []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY},
	}
	te := getTestEnv(t, map[string]interfaces.UserInfo{user.UserID: user})
	flags.Set(t, "auth.enable_anonymous_usage", "false")
	authCtx := testauth.WithAuthenticatedUser(context.Background(), user)

	canWrite, err := capabilities.IsGranted(authCtx, te, akpb.ApiKey_CACHE_WRITE_CAPABILITY)

	assert.True(t, canWrite)
	assert.Nil(t, err)
}

func TestIsGranted_TestUserWithoutCapability_False(t *testing.T) {
	user := &auth.Claims{
		UserID:       "US1",
		GroupID:      "GR1",
		Capabilities: []akpb.ApiKey_Capability{},
	}
	te := getTestEnv(t, map[string]interfaces.UserInfo{user.UserID: user})
	flags.Set(t, "auth.enable_anonymous_usage", "false")
	authCtx := testauth.WithAuthenticatedUser(context.Background(), user)

	canWrite, err := capabilities.IsGranted(authCtx, te, akpb.ApiKey_CACHE_WRITE_CAPABILITY)

	assert.False(t, canWrite)
	assert.Nil(t, err)
}
