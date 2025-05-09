package invocation_stat_service

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/stats"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestGetStatDrilldown(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "app.trends_heatmap_enabled", true)
	te := testenv.GetTestEnv(t)

	ta := testauth.NewTestAuthenticator(testauth.TestUsers("USER1", "GROUP1"))
	te.SetAuthenticator(ta)

	ctx, err := ta.WithAuthenticatedUser(context.Background(), "USER1")
	require.NoError(t, err)

	reqCtx := testauth.RequestContext("USER1", "GROUP1")

	iss := NewInvocationStatService(te, te.GetDBHandle(), te.GetOLAPDBHandle())
	_, err = iss.GetStatDrilldown(ctx, &stats.GetStatDrilldownRequest{
		RequestContext: reqCtx,
	})
	require.True(t, status.IsInvalidArgumentError(err))
	require.Error(t, err, "Empty filter for drilldown.")
}
