package hit_tracker_service_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
)

func TestHitTrackerService(t *testing.T) {
	env := testenv.GetTestEnv(t)
	hit_tracker.Register(env)
	require.NotNil(t, env.GetHitTrackerService())
}
