package api_key_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/api_key"
	"github.com/stretchr/testify/require"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
)

const (
	devs   = akpb.Visibility_VISIBLE_TO_DEVELOPERS
	admins = akpb.Visibility_VISIBLE_TO_GROUP_ADMINS
)

func TestVisibilityRoundTrip(t *testing.T) {
	require.Equal(t, int32(0), api_key.VisibilitiesToInt(nil))
	require.Empty(t, api_key.VisibilitiesFromInt(0))
	require.Equal(t, int32(3), api_key.VisibilitiesToInt([]akpb.Visibility{devs, admins}))
	require.Equal(t, []akpb.Visibility{devs, admins}, api_key.VisibilitiesFromInt(3))
	require.Equal(t, []akpb.Visibility{admins}, api_key.VisibilitiesFromInt(int32(admins)))
}

func TestFixRequestVisibility(t *testing.T) {
	// Clients that only set the deprecated flag get the equivalent list.
	create := &akpb.CreateApiKeyRequest{}
	api_key.FixRequestVisibility(create)
	require.Equal(t, []akpb.Visibility{admins}, create.GetVisibility())
	require.False(t, create.GetVisibleToDevelopers())

	create = &akpb.CreateApiKeyRequest{VisibleToDevelopers: true}
	api_key.FixRequestVisibility(create)
	require.Equal(t, []akpb.Visibility{devs, admins}, create.GetVisibility())
	require.True(t, create.GetVisibleToDevelopers())

	// An explicit list wins over the deprecated flag, and the flag is updated
	// to match.
	update := &akpb.UpdateApiKeyRequest{Visibility: []akpb.Visibility{admins}, VisibleToDevelopers: true}
	api_key.FixRequestVisibility(update)
	require.Equal(t, []akpb.Visibility{admins}, update.GetVisibility())
	require.False(t, update.GetVisibleToDevelopers())

	update = &akpb.UpdateApiKeyRequest{Visibility: []akpb.Visibility{devs}}
	api_key.FixRequestVisibility(update)
	require.Equal(t, []akpb.Visibility{devs}, update.GetVisibility())
	require.True(t, update.GetVisibleToDevelopers())
}

func TestVisibilityOfKey(t *testing.T) {
	// Unmigrated keys fall back to the deprecated flag.
	require.Equal(t, int32(admins), api_key.VisibilityOfKey(&tables.APIKey{}))
	require.Equal(t, int32(admins|devs), api_key.VisibilityOfKey(&tables.APIKey{VisibleToDevelopers: true}))
	// Migrated keys use the bitmask, even if the deprecated flag disagrees.
	require.Equal(t, int32(admins), api_key.VisibilityOfKey(&tables.APIKey{Visibility: int32(admins), VisibleToDevelopers: true}))
	require.True(t, api_key.IsVisibleToDevelopers(int32(devs)))
	require.False(t, api_key.IsVisibleToDevelopers(int32(admins)))
}
