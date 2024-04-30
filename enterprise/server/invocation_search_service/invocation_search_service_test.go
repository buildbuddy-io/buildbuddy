package invocation_search_service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	sfpb "github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
)

func getUUIDBytes(id byte) []byte {
	return []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, id}
}

func getUUIDString(id byte) string {
	return fmt.Sprintf("00000000-0000-0000-0000-0000000000%02x", id)
}

func setUpFakeData(ctx context.Context, env *real_environment.RealEnv, t *testing.T) {
	for _, row := range []*tables.Invocation{
		{
			InvocationID:     getUUIDString(0),
			InvocationUUID:   getUUIDBytes(0),
			User:             "jdhollen",
			Perms:            perms.GROUP_READ,
			InvocationStatus: 1, /* COMPLETE */
			GroupID:          "GR1",
		},
		{
			InvocationID:     getUUIDString(1),
			InvocationUUID:   getUUIDBytes(1),
			User:             "jdhollen",
			Perms:            perms.GROUP_READ,
			InvocationStatus: 3, /* DISCONNECTED */
			GroupID:          "GR1",
		},
		{
			InvocationID:     getUUIDString(2),
			InvocationUUID:   getUUIDBytes(2),
			User:             "jdhollen",
			Perms:            perms.GROUP_READ,
			InvocationStatus: 2, /* PARTIAL */
			GroupID:          "GR1",
		},
		{
			InvocationID:     getUUIDString(3),
			InvocationUUID:   getUUIDBytes(3),
			User:             "sluongng",
			Perms:            perms.GROUP_READ,
			InvocationStatus: 1, /* COMPLETE */
			GroupID:          "GR1",
		},
		{
			InvocationID:     getUUIDString(4),
			InvocationUUID:   getUUIDBytes(4),
			User:             "octocat",
			Perms:            perms.GROUP_READ,
			InvocationStatus: 1, /* COMPLETE */
			GroupID:          "GR2",
		},
		{
			InvocationID:     getUUIDString(5),
			InvocationUUID:   getUUIDBytes(5),
			User:             "jdhollen",
			Perms:            perms.GROUP_READ,
			InvocationStatus: 1, /* COMPLETE */
			GroupID:          "GR1",
		},
		{
			InvocationID:     getUUIDString(6),
			InvocationUUID:   getUUIDBytes(6),
			User:             "jdhollen",
			Perms:            perms.GROUP_READ,
			InvocationStatus: 1, /* COMPLETE */
			GroupID:          "GR1",
		},
		{
			InvocationID:     getUUIDString(7),
			InvocationUUID:   getUUIDBytes(7),
			User:             "jdhollen",
			Perms:            perms.GROUP_READ,
			InvocationStatus: 1, /* COMPLETE */
			GroupID:          "GR1",
		},
	} {
		var asCreated tables.Invocation
		err := env.GetDBHandle().NewQuery(ctx, "test").Create(row)
		require.NoError(t, err)
		err = env.GetDBHandle().GORM(ctx, "test").Where("invocation_id = ?", row.InvocationID).First(&asCreated).Error
		require.NoError(t, err)

		// Don't mirror inv. 6 over to OLAP, for fun.  Otherwise, don't sync partial / disconnected invocations to clickhouse.
		if row.InvocationID == getUUIDString(6) || row.InvocationStatus == 2 || row.InvocationStatus == 3 {
			continue
		}
		if env.GetOLAPDBHandle() != nil {
			err := env.GetOLAPDBHandle().FlushInvocationStats(ctx, &asCreated)
			require.NoError(t, err)
		}
	}
}

func setUpDB(ctx context.Context, env *real_environment.RealEnv, t *testing.T) *testauth.TestAuthenticator {
	ta := testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	setUpFakeData(ctx, env, t)
	return ta
}

func getInvocationIDSlice(rsp *inpb.SearchInvocationResponse) []string {
	out := make([]string, 0)
	for _, inv := range rsp.Invocation {
		out = append(out, inv.InvocationId)
	}
	return out
}

func TestNonOLAPQuery(t *testing.T) {
	bgCtx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := setUpDB(bgCtx, env, t)

	testCtx, err := ta.WithAuthenticatedUser(bgCtx, "US1")
	require.NoError(t, err)

	service := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	rsp, err := service.QueryInvocations(testCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Query: &inpb.InvocationQuery{
			User: "jdhollen",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{getUUIDString(7), getUUIDString(6), getUUIDString(5), getUUIDString(2), getUUIDString(1), getUUIDString(0)}, getInvocationIDSlice(rsp))

	rsp, err = service.QueryInvocations(testCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Query: &inpb.InvocationQuery{
			User: "sluongng",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{getUUIDString(3)}, getInvocationIDSlice(rsp))
}

func TestBlendedOLAPQuery(t *testing.T) {
	flags.Set(t, "app.blended_invocation_search_enabled", true)
	flags.Set(t, "testenv.use_clickhouse", true)
	bgCtx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := setUpDB(bgCtx, env, t)

	testCtx, err := ta.WithAuthenticatedUser(bgCtx, "US1")
	require.NoError(t, err)

	service := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	rsp, err := service.QueryInvocations(testCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Query: &inpb.InvocationQuery{
			User: "jdhollen",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{getUUIDString(7), getUUIDString(5), getUUIDString(2), getUUIDString(1), getUUIDString(0)}, getInvocationIDSlice(rsp))

	rsp, err = service.QueryInvocations(testCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Query: &inpb.InvocationQuery{
			User: "sluongng",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{getUUIDString(3)}, getInvocationIDSlice(rsp))
}

func TestUnblendedOLAPQuery(t *testing.T) {
	flags.Set(t, "app.olap_invocation_search_enabled", true)
	flags.Set(t, "testenv.use_clickhouse", true)
	bgCtx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := setUpDB(bgCtx, env, t)

	testCtx, err := ta.WithAuthenticatedUser(bgCtx, "US1")
	require.NoError(t, err)

	service := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	metric := sfpb.InvocationMetricType_UPDATED_AT_USEC_INVOCATION_METRIC
	bogusMetricMin := int64(0)
	rsp, err := service.QueryInvocations(testCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Query: &inpb.InvocationQuery{
			User: "jdhollen",
			// Including this filter will cause this request path to hit Clickhouse.
			Filter: []*sfpb.StatFilter{&sfpb.StatFilter{
				Metric: &sfpb.Metric{Invocation: &metric},
				Min:    &bogusMetricMin,
			}},
		},
	})
	require.NoError(t, err)
	// Pending / disconnected builds should be missing.
	assert.Equal(t, []string{getUUIDString(7), getUUIDString(5), getUUIDString(0)}, getInvocationIDSlice(rsp))

	rsp, err = service.QueryInvocations(testCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Query: &inpb.InvocationQuery{
			User: "sluongng",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{getUUIDString(3)}, getInvocationIDSlice(rsp))
}
