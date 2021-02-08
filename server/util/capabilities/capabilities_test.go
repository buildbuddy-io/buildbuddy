package capabilities

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/auth"
	testenv "github.com/buildbuddy-io/buildbuddy/server/testutil/environment"
	"github.com/stretchr/testify/assert"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

func newTestEnv(t *testing.T) testenv.TestEnv {
	te, err := testenv.GetTestEnv()
	if err != nil {
		t.Fatal(err)
	}
	te.SetAuthenticator(auth.NewTestAuthenticator())
	return te
}

func TestToInt_NoCapabilities(t *testing.T) {
	c := ToInt([]akpb.ApiKey_Capability{})

	assert.Equal(t, int32(0), c)
}

func TestToInt_OneCapability(t *testing.T) {
	c := ToInt([]akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY})

	assert.Equal(t, int32(akpb.ApiKey_CACHE_WRITE_CAPABILITY), c)
}

func TestToInt_MultipleCapabilities(t *testing.T) {
	// TODO: we don't have multiple capabilities yet but should def test it when we do
	t.Skip()
}

func TestFromInt_NoCapabilities(t *testing.T) {
	caps := FromInt(0)

	assert.Equal(t, []akpb.ApiKey_Capability{}, caps)
}

func TestFromInt_OneCapability(t *testing.T) {
	caps := FromInt(int32(akpb.ApiKey_CACHE_WRITE_CAPABILITY))

	assert.Equal(t, []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY}, caps)
}

func TestFromInt_MultipleCapabilities(t *testing.T) {
	// TODO: we don't have multiple capabilities yet but should def test it when we do
	t.Skip()
}

func TestIsGranted_AnonymousUsageEnabled_AnonymousUser_True(t *testing.T) {
	te := newTestEnv()

	granted, err := IsGranted(ctx, te, akpb.ApiKey_CACHE_WRITE_CAPABILITY)

	assert.Nil(t, err)
	assert.True(t, granted)
}
