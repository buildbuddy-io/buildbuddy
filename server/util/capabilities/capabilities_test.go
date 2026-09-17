package capabilities_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/capabilities"
	"github.com/stretchr/testify/assert"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
)

var (
	emptyUserMap = testauth.TestUsers()
)

func getTestEnv(t *testing.T, users map[string]interfaces.UserInfo) *testenv.TestEnv {
	te := testenv.GetTestEnv(t)
	te.SetAuthenticator(testauth.NewTestAuthenticator(t, users))
	return te
}

func TestToInt_NoCapabilities(t *testing.T) {
	c := capabilities.ToInt([]cappb.Capability{})

	assert.Equal(t, int32(0), c)
}

func TestToInt_OneCapability(t *testing.T) {
	c := capabilities.ToInt([]cappb.Capability{cappb.Capability_CACHE_WRITE})

	assert.Equal(t, int32(cappb.Capability_CACHE_WRITE), c)
}

func TestToInt_MultipleCapabilities(t *testing.T) {
	// TODO: we don't have multiple capabilities yet but should def test it when we do
	t.Skip()
}

func TestFromInt_NoCapabilities(t *testing.T) {
	caps := capabilities.FromInt(0)

	assert.Equal(t, []cappb.Capability{}, caps)
}

func TestFromInt_OneCapability(t *testing.T) {
	caps := capabilities.FromInt(int32(cappb.Capability_CACHE_WRITE))

	assert.Equal(t, []cappb.Capability{cappb.Capability_CACHE_WRITE}, caps)
}

func TestFromInt_MultipleCapabilities(t *testing.T) {
	// TODO: we don't have multiple capabilities yet but should def test it when we do
	t.Skip()
}

func TestIsGranted_AnonymousUsageEnabled_AnonymousUser_True(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	anonCtx := context.Background()

	canWrite, err := capabilities.IsGranted(anonCtx, te.GetAuthenticator(), cappb.Capability_CACHE_WRITE)

	assert.True(t, canWrite)
	assert.Nil(t, err)
}

func TestIsGranted_NullAuthenticator(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	te.SetAuthenticator(&nullauth.NullAuthenticator{})
	anonCtx := context.Background()

	canWrite, err := capabilities.IsGranted(anonCtx, te.GetAuthenticator(), cappb.Capability_CACHE_WRITE)

	assert.True(t, canWrite)
	assert.Nil(t, err)
}

func TestNotGranted_NullAuthenticator_AnonymousUsage_Disabled(t *testing.T) {
	te := getTestEnv(t, emptyUserMap)
	te.SetAuthenticator(nullauth.NewNullAuthenticator(false))

	anonCtx := context.Background()

	canWrite, err := capabilities.IsGranted(anonCtx, te.GetAuthenticator(), cappb.Capability_CACHE_WRITE)

	assert.False(t, canWrite)
	assert.Nil(t, err)
}

func TestIsGranted_TestUserWithCapability_True(t *testing.T) {
	user := &testauth.TestUser{
		UserID:       "US1",
		GroupID:      "GR1",
		Capabilities: []cappb.Capability{cappb.Capability_CACHE_WRITE},
	}
	te := getTestEnv(t, map[string]interfaces.UserInfo{user.UserID: user})
	authCtx := testauth.WithAuthenticatedUserInfo(context.Background(), user)

	canWrite, err := capabilities.IsGranted(authCtx, te.GetAuthenticator(), cappb.Capability_CACHE_WRITE)

	assert.True(t, canWrite)
	assert.Nil(t, err)
}

func TestIsGranted_TestUserWithoutCapability_False(t *testing.T) {
	user := &testauth.TestUser{
		UserID:       "US1",
		GroupID:      "GR1",
		Capabilities: []cappb.Capability{},
	}
	te := getTestEnv(t, map[string]interfaces.UserInfo{user.UserID: user})
	authCtx := testauth.WithAuthenticatedUserInfo(context.Background(), user)

	canWrite, err := capabilities.IsGranted(authCtx, te.GetAuthenticator(), cappb.Capability_CACHE_WRITE)

	assert.False(t, canWrite)
	assert.Nil(t, err)
}

func TestCanWriteCache(t *testing.T) {
	for _, instance := range []struct {
		name  string
		image bool
	}{
		{name: ""},
		{name: "regular-instance"},
		{name: interfaces.OCIImageInstanceNamePrefix, image: true},
		{name: interfaces.OCIImageInstanceNamePrefix + "_manifest_content_", image: true},
		{name: interfaces.OCIImageInstanceNamePrefix + "/nested", image: true},
		{name: interfaces.OCIImageInstanceNamePrefix + "/../regular-instance"},
		{name: interfaces.OCIImageInstanceNamePrefix + "/nested/../../regular-instance"},
		{name: interfaces.OCIImageInstanceNamePrefix + "/.."},
		{name: "other/" + interfaces.OCIImageInstanceNamePrefix},
	} {
		instanceName := instance.name
		for _, test := range []struct {
			name    string
			caps    []cappb.Capability
			cas, ac bool
		}{
			{name: "read only"},
			{name: "cache write", caps: []cappb.Capability{cappb.Capability_CACHE_WRITE}, cas: true, ac: true},
			{name: "CAS write", caps: []cappb.Capability{cappb.Capability_CAS_WRITE}, cas: true},
			{name: "unrelated capability", caps: []cappb.Capability{cappb.Capability_REGISTER_EXECUTOR}},
			{name: "image write", caps: []cappb.Capability{cappb.Capability_IMAGE_CACHE_WRITE},
				cas: instance.image,
				ac:  instance.image},
		} {
			t.Run(instanceName+"/"+test.name, func(t *testing.T) {
				user := &testauth.TestUser{UserID: "US1", GroupID: "GR1", Capabilities: test.caps}
				te := getTestEnv(t, map[string]interfaces.UserInfo{user.UserID: user})
				ctx := testauth.WithAuthenticatedUserInfo(t.Context(), user)
				cas, err := capabilities.CanWriteCAS(ctx, te.GetAuthenticator(), instanceName)
				assert.NoError(t, err)
				assert.Equal(t, test.cas, cas)
				ac, err := capabilities.CanWriteActionCache(ctx, te.GetAuthenticator(), instanceName)
				assert.NoError(t, err)
				assert.Equal(t, test.ac, ac)
			})
		}
	}
}

func TestCanWriteCache_Anonymous(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, instanceName := range []string{"regular-instance", interfaces.OCIImageInstanceNamePrefix} {
			auth := nullauth.NewNullAuthenticator(enabled)
			cas, err := capabilities.CanWriteCAS(t.Context(), auth, instanceName)
			assert.NoError(t, err)
			assert.Equal(t, enabled, cas)
			ac, err := capabilities.CanWriteActionCache(t.Context(), auth, instanceName)
			assert.NoError(t, err)
			assert.Equal(t, enabled, ac)
		}
	}
}
