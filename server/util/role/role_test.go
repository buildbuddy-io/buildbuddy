package role_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/stretchr/testify/require"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

func TestRoleProtoAndStringConversions(t *testing.T) {
	for enumValue, enumName := range grpb.Group_Role_name {
		if enumValue == int32(grpb.Group_UNKNOWN_ROLE) {
			continue
		}
		r, err := role.FromProto(grpb.Group_Role(enumValue))
		require.NoError(t, err, "role.FromProto should work for all proto values")
		require.Equal(t, int32(enumValue), int32(r), "role.FromProto should be the same as casting")
		s := r.String()
		expectedString := strings.ToLower(strings.TrimSuffix(enumName, "_ROLE"))
		require.Equal(t, expectedString, s, "role.String()")
		parsed, err := role.Parse(s)
		require.NoError(t, err, "role.Parse(r.String()) should succeed")
		require.Equal(t, r, parsed, "role.Parse(r.String()) should return r")
	}

	require.Equal(t, "", role.Role(0xFFFF).String(), "invalid role.String() should return empty")
	require.Equal(t, "", role.None.String(), "role.None.String() should return empty")

	_, err := role.Parse("invalid-role")
	require.Error(t, err)
	_, err = role.Parse("")
	require.Error(t, err)
}
