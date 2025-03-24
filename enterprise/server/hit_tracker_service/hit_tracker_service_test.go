package hit_tracker_service_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hit_tracker_service"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
)

func TestHitTrackerService_DetailedStats(t *testing.T) {
	env := testenv.GetTestEnv(t)
	hit_tracker_service.Register(env)
	require.NotNil(t, env.GetHitTrackerServiceServer())
}
