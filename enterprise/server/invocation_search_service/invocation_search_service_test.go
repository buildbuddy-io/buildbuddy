package invocation_search_service_test

import (
	"context"
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
)

func setUpFakeData(ctx context.Context, env *real_environment.RealEnv, t *testing.T) {
	for _, row := range []*tables.Invocation{
		{
			InvocationID:    "INV1",
			User:            "jdhollen",
			Perms:			 perms.GROUP_READ,
			GroupID:         "GR1",
		},
		{
			InvocationID:    "INV2",
			User:            "jdhollen",
			Perms:			 perms.GROUP_READ,
			GroupID:         "GR1",
		},
		{
			InvocationID:    "INV3",
			User:            "sluongng",
			Perms:			 perms.GROUP_READ,
			GroupID:         "GR1",
		},
		{
			InvocationID:    "INV4",
			User:            "octocat",
			Perms:			 perms.GROUP_READ,
			GroupID:         "GR2",
		},
	} {
		err := env.GetDBHandle().NewQuery(ctx, "test").Create(row)
		require.NoError(t, err)
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
	for _, inv := range(rsp.Invocation) {
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
	assert.Equal(t, []string{"INV2", "INV1"}, getInvocationIDSlice(rsp))

	rsp, err = service.QueryInvocations(testCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Query: &inpb.InvocationQuery{
			User: "sluongng",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"INV3"}, getInvocationIDSlice(rsp))
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
	assert.Equal(t, []string{"INV2", "INV1"}, getInvocationIDSlice(rsp))

	rsp, err = service.QueryInvocations(testCtx, &inpb.SearchInvocationRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Query: &inpb.InvocationQuery{
			User: "sluongng",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"INV3"}, getInvocationIDSlice(rsp))
}
