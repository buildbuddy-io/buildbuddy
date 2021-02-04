package capabilities

import (
	"testing"

	"github.com/stretchr/testify/assert"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

func TestToIntNoCapabilities(t *testing.T) {
	c := ToInt([]akpb.ApiKey_Capability{})

	assert.Equal(t, int32(0), c)
}

func TestToIntOneCapability(t *testing.T) {
	c := ToInt([]akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY})

	assert.Equal(t, int32(akpb.ApiKey_CACHE_WRITE_CAPABILITY), c)
}

func TestToIntMultipleCapabilities(t *testing.T) {
	// TODO: we don't have multiple capabilities yet but should def test it when we do
	t.Skip()
}

func TestFromIntNoCapabilities(t *testing.T) {
	caps := FromInt(0)

	assert.Equal(t, []akpb.ApiKey_Capability{}, caps)
}

func TestFromIntOneCapability(t *testing.T) {
	caps := FromInt(int32(akpb.ApiKey_CACHE_WRITE_CAPABILITY))

	assert.Equal(t, []akpb.ApiKey_Capability{akpb.ApiKey_CACHE_WRITE_CAPABILITY}, caps)
}

func TestFromIntMultipleCapabilities(t *testing.T) {
	// TODO: we don't have multiple capabilities yet but should def test it when we do
	t.Skip()
}
