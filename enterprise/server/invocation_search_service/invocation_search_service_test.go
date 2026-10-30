package invocation_search_service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
	// Disable async invocation batch inserts.
	flags.Set(t, "olap_database.invocation_batch_insert_interval", 0)
	flags.Set(t, "olap_database.async_insert", true)

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1", "US2", "GR2"))
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

// Agent queries compose with role and user tags across primary and OLAP search.
func TestAgentQueries(t *testing.T) {
	for _, backend := range []string{"primary", "OLAP"} {
		t.Run(backend, func(t *testing.T) {
			// Configure the backend and authenticated user.
			flags.Set(t, "app.olap_invocation_search_enabled", true)
			flags.Set(t, "olap_database.invocation_batch_insert_interval", 0)
			flags.Set(t, "olap_database.async_insert", true)
			ctx := context.Background()
			env := testenv.GetTestEnv(t)
			if backend == "OLAP" {
				testclickhouse.Configure(t, env)
			}
			ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
			env.SetAuthenticator(ta)
			ctx, err := ta.WithAuthenticatedUser(ctx, "US1")
			require.NoError(t, err)
			// Seed providers, roles, ordinary tags, and an unfinished invocation.
			for _, row := range []struct {
				id     byte
				role   string
				tags   string
				agent  string
				status int64
			}{
				{id: 0, role: "CI", agent: "codex", tags: "team=build", status: 1},
				{id: 1, role: "CI", agent: "claude", status: 1},
				{id: 2, role: "HOSTED_BAZEL", agent: "codex", status: 1},
				{id: 4, role: "CI", status: 1},
				{id: 6, role: "CI", agent: "codex", status: 2},
			} {
				inv := &tables.Invocation{
					InvocationID:     getUUIDString(row.id),
					InvocationUUID:   getUUIDBytes(row.id),
					GroupID:          "GR1",
					User:             "developer",
					Perms:            perms.GROUP_READ,
					Role:             row.role,
					Tags:             "control," + row.tags,
					Agent:            new(row.agent),
					InvocationStatus: row.status,
				}
				require.NoError(t, env.GetDBHandle().NewQuery(ctx, "test").Create(inv))
				if backend == "OLAP" && row.status == 1 {
					require.NoError(t, env.GetOLAPDBHandle().FlushInvocationStats(ctx, inv))
				}
			}
			// Exercise provider filters together with role and ordinary-tag predicates.
			service := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())
			for _, tc := range []struct {
				name               string
				roles              []string
				legacyTags         []string
				values             []string
				expectedComplete   []byte
				expectedUnfinished []byte
			}{
				{
					name:               "CI and exact provider",
					roles:              []string{"CI"},
					values:             []string{"codex"},
					expectedComplete:   []byte{0},
					expectedUnfinished: []byte{6},
				},
				{
					name:               "provider alternatives",
					roles:              []string{"CI"},
					values:             []string{"codex", "claude"},
					expectedComplete:   []byte{0, 1},
					expectedUnfinished: []byte{6},
				},
				{
					name:             "provider and ordinary tag",
					legacyTags:       []string{"team=build"},
					values:           []string{"codex"},
					expectedComplete: []byte{0},
				},
				{
					name:   "unknown provider matches no invocations",
					values: []string{"future-agent"},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					// Build and execute the request for this filter combination.
					query := &inpb.InvocationQuery{
						Role: tc.roles,
						Tags: tc.legacyTags,
						GenericFilters: []*sfpb.GenericFilter{{
							Type:    sfpb.FilterType_AGENT_FILTER_TYPE,
							Operand: sfpb.FilterOperand_IN_OPERAND,
							Value:   &sfpb.FilterValue{StringValue: tc.values},
						}},
					}
					if backend == "OLAP" {
						// Use the existing ordinary-tag routing rule to exercise OLAP.
						query.Tags = append(query.Tags, "control")
					}
					rsp, err := service.QueryInvocations(ctx, &inpb.SearchInvocationRequest{
						RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
						Query:          query,
					})
					require.NoError(t, err)
					// Only primary search includes invocations that have not been flushed.
					var expectedIDs []string
					for _, id := range tc.expectedComplete {
						expectedIDs = append(expectedIDs, getUUIDString(id))
					}
					if backend == "primary" {
						for _, id := range tc.expectedUnfinished {
							expectedIDs = append(expectedIDs, getUUIDString(id))
						}
					}
					assert.ElementsMatch(t, expectedIDs, getInvocationIDSlice(rsp))
				})
			}
		})
	}
}

func TestGroupIDDefaultsToAuthenticatedGroupID(t *testing.T) {
	env := testenv.GetTestEnv(t)
	ta := setUpDB(t.Context(), env, t)

	service := invocation_search_service.NewInvocationSearchService(env, env.GetDBHandle(), env.GetOLAPDBHandle())

	for _, tc := range []struct {
		name                  string
		user                  string
		query                 *inpb.InvocationQuery
		expectedInvocationIDs []string
		expectedError         func(err error) bool
	}{
		{
			name:  "nil query",
			user:  "US1",
			query: nil,
			expectedInvocationIDs: []string{
				getUUIDString(0),
				getUUIDString(1),
				getUUIDString(2),
				getUUIDString(3),
				// Note: invocation 00..04 is not owned by US1's group
				getUUIDString(5),
				getUUIDString(6),
				getUUIDString(7),
			},
		},
		{
			name:  "empty query",
			user:  "US1",
			query: &inpb.InvocationQuery{},
			expectedInvocationIDs: []string{
				getUUIDString(0),
				getUUIDString(1),
				getUUIDString(2),
				getUUIDString(3),
				getUUIDString(5),
				getUUIDString(6),
				getUUIDString(7),
			},
		},
		{
			name:  "nonempty query",
			user:  "US1",
			query: &inpb.InvocationQuery{User: "jdhollen"},
			expectedInvocationIDs: []string{
				getUUIDString(0),
				getUUIDString(1),
				getUUIDString(2),
				getUUIDString(5),
				getUUIDString(6),
				getUUIDString(7),
			},
		},
		{
			name:          "unauthenticated",
			user:          "",
			query:         nil,
			expectedError: status.IsInternalError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			if tc.user != "" {
				var err error
				ctx, err = ta.WithAuthenticatedUser(t.Context(), tc.user)
				require.NoError(t, err)
			}

			req := &inpb.SearchInvocationRequest{
				RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
				Query:          tc.query,
			}
			rsp, err := service.QueryInvocations(ctx, req)

			if tc.expectedError != nil {
				require.True(t, tc.expectedError(err))
			} else {
				require.NoError(t, err)
				assert.ElementsMatch(t, tc.expectedInvocationIDs, getInvocationIDSlice(rsp))
			}
		})
	}
}

func TestOLAPQuery(t *testing.T) {
	flags.Set(t, "app.olap_invocation_search_enabled", true)
	bgCtx := context.Background()
	env := testenv.GetTestEnv(t)
	testclickhouse.Configure(t, env)
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
