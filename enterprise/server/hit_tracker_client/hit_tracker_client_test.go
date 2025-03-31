package hit_tracker_client_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/hit_tracker_client"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
)

func TestHitTrackerClient(t *testing.T) {
	te := testenv.GetTestEnv(t)
	hit_tracker_client.Register(te)
	require.NotNil(t, te.GetHitTrackerFactory())
}
