package usage_service_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

func testUsageUser(userID, groupID string, caps ...cappb.Capability) *testauth.TestUser {
	return &testauth.TestUser{
		UserID:        userID,
		GroupID:       groupID,
		AllowedGroups: []string{groupID},
		Capabilities:  caps,
		GroupMemberships: []*interfaces.GroupMembership{
			{
				GroupID:      groupID,
				Capabilities: caps,
			},
		},
	}
}

func TestGetUsage(t *testing.T) {
	group := &tables.Group{
		GroupID:       "GR1",
		CreatedAtUsec: time.Date(2023, 7, 9, 0, 0, 0, 0, time.UTC).UnixMicro(),
	}
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	// Current time for test: 2024-02-22, noon UTC
	now := time.Date(2024, 2, 22, 12, 0, 0, 0, time.UTC)
	clock := clockwork.NewFakeClockAt(now)
	service, err := usage_service.New(env, clock)
	require.NoError(t, err)
	// Insert some usage data:
	for _, row := range []*tables.Usage{
		// GR1, current usage period
		{
			UsageID:         "UG1",
			GroupID:         "GR1",
			PeriodStartUsec: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			Invocations:     13,
			CASCacheHits:    10_000,
		},
		{
			UsageID:         "UG1",
			GroupID:         "GR1",
			PeriodStartUsec: time.Date(2024, 2, 4, 1, 0, 0, 0, time.UTC).UnixMicro(),
			Invocations:     15,
			CASCacheHits:    12_000,
		},
		// GR1, previous usage period
		{
			UsageID:         "UG2",
			GroupID:         "GR1",
			PeriodStartUsec: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			CASCacheHits:    77,
		},
		// GR2, current usage period
		{
			UsageID:         "UG3",
			GroupID:         "GR2",
			PeriodStartUsec: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			Invocations:     107,
		},
	} {
		err = env.GetDBHandle().NewQuery(ctx, "test").Create(row)
		require.NoError(t, err)
	}

	rsp, err := service.GetUsageInternal(ctx1, group, &usagepb.GetUsageRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		UsagePeriod:    "2024-02",
	})
	require.NoError(t, err)

	expectedResponse := &usagepb.GetUsageResponse{
		Usage: &usagepb.Usage{
			Period:       "2024-02",
			Invocations:  28,
			CasCacheHits: 22_000,
		},
		DailyUsage: []*usagepb.Usage{
			&usagepb.Usage{
				Period:       "2024-02-03",
				Invocations:  13,
				CasCacheHits: 10_000,
			},
			&usagepb.Usage{
				Period:       "2024-02-04",
				Invocations:  15,
				CasCacheHits: 12_000,
			},
		},
		AvailableUsagePeriods: []string{
			"2024-02",
			"2024-01",
			"2023-12",
			"2023-11",
			"2023-10",
			"2023-09",
			"2023-08",
			"2023-07",
		},
	}
	assert.Empty(t, cmp.Diff(expectedResponse, rsp, protocmp.Transform()))
}

func TestGetCurrentBill(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/v1/customers":
			if r.URL.Query().Get("ingest_alias") == "GR1" {
				fmt.Fprint(w, `{"data":[{"id":"cust-1","ingest_aliases":["GR1"]}]}`)
			} else {
				fmt.Fprint(w, `{"data":[]}`)
			}
		case "/v1/customers/cust-1/invoices":
			fmt.Fprint(w, `{"data":[{"id":"inv-1","type":"USAGE","status":"DRAFT","start_timestamp":"2024-02-01T00:00:00Z","end_timestamp":"2024-03-01T00:00:00Z","total":2396000,"line_items":[{"name":"Action cache hits","type":"usage","quantity":2000,"unit_price":1200,"total":2400000,"starting_at":"2024-02-01T00:00:00+00:00","ending_before":"2024-03-01T00:00:00+00:00"},{"name":"Monthly credit applied","type":"applied_commit_or_credit","quantity":null,"unit_price":null,"total":-4000,"starting_at":"2024-02-01T00:00:00+00:00","ending_before":"2024-03-01T00:00:00+00:00"}]}]}`)
		case "/v1/contracts/customerBalances/list":
			fmt.Fprint(w, `{"data":[{"type":"CREDIT","balance":1000,"access_schedule":{"schedule_items":[{"amount":5000,"starting_at":"2024-02-01T00:00:00Z","ending_before":"2024-03-01T00:00:00Z"}]}}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	flags.Set(t, "http.client.allow_localhost", true)
	flags.Set(t, "billing.metronome.read_only_api_key", "test-key")
	flags.Set(t, "billing.metronome.api_url", server.URL)

	ctx := context.Background()
	env := enterprise_testenv.New(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1", "US2", "GR2"))
	env.SetAuthenticator(ta)
	for id, groupStatus := range map[string]grpb.Group_GroupStatus{
		"GR1": grpb.Group_USAGE_BASED_GROUP_STATUS,
		"GR2": grpb.Group_FREE_TIER_GROUP_STATUS,
	} {
		require.NoError(t, env.GetDBHandle().NewQuery(ctx, "test").Create(&tables.Group{GroupID: id, Status: groupStatus}))
	}
	now := time.Date(2024, 2, 22, 12, 0, 0, 0, time.UTC)
	clock := clockwork.NewFakeClockAt(now)
	service, err := usage_service.New(env, clock)
	require.NoError(t, err)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	ctx2, err := ta.WithAuthenticatedUser(ctx, "US2")
	require.NoError(t, err)

	_, err = service.GetCurrentBill(ctx1, &usagepb.GetCurrentBillRequest{})
	require.True(t, status.IsUnimplementedError(err), "unexpected error: %v", err)
	assert.EqualValues(t, 0, requests.Load())
	flags.Set(t, "app.usage_bill_enabled", true)

	periodStart := timestamppb.New(time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC))
	periodEnd := timestamppb.New(time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC))
	expected := &usagepb.GetCurrentBillResponse{Bill: &usagepb.Bill{
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		TotalCents:  2396000,
		LineItems: []*usagepb.BillLineItem{
			{Name: "Action cache hits", Quantity: 2000, UnitPriceCents: 1200, TotalCents: 2400000, PeriodStart: periodStart, PeriodEnd: periodEnd},
		},
		FetchedAt:          timestamppb.New(now),
		CreditGrantedCents: 5000,
		CreditUsedCents:    4000,
	}}
	rsp, err := service.GetCurrentBill(ctx1, &usagepb.GetCurrentBillRequest{})
	require.NoError(t, err)
	assert.Empty(t, cmp.Diff(expected, rsp, protocmp.Transform()))
	assert.EqualValues(t, 3, requests.Load())

	rsp, err = service.GetCurrentBill(ctx1, &usagepb.GetCurrentBillRequest{})
	require.NoError(t, err)
	assert.Empty(t, cmp.Diff(expected, rsp, protocmp.Transform()))
	assert.EqualValues(t, 3, requests.Load())

	clock.Advance(16 * time.Minute)
	_, err = service.GetCurrentBill(ctx1, &usagepb.GetCurrentBillRequest{})
	require.NoError(t, err)
	assert.EqualValues(t, 6, requests.Load())

	rsp, err = service.GetCurrentBill(ctx2, &usagepb.GetCurrentBillRequest{})
	require.NoError(t, err)
	assert.Nil(t, rsp.GetBill())
	assert.EqualValues(t, 6, requests.Load())
}

func TestUsageFields_CoverEveryUsageFieldAndAlertingMetric(t *testing.T) {
	usageFieldNames := map[string]struct{}{}
	fields := (&usagepb.Usage{}).ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		// Repeated fields are per-dimension breakdowns that are only available
		// from the OLAP DB, so they have no UsageFields entry.
		if field.Name() == "period" || field.Cardinality() == protoreflect.Repeated {
			continue
		}
		usageFieldNames[string(field.Name())] = struct{}{}
	}

	alertingMetrics := map[usagepb.UsageAlertingMetric_Value]struct{}{}
	for value := range usagepb.UsageAlertingMetric_Value_name {
		metric := usagepb.UsageAlertingMetric_Value(value)
		if metric == usagepb.UsageAlertingMetric_UNKNOWN {
			continue
		}
		alertingMetrics[metric] = struct{}{}
	}

	seenUsageFields := map[string]struct{}{}
	seenAlertingMetrics := map[usagepb.UsageAlertingMetric_Value]struct{}{}
	for _, field := range usage_service.UsageFields {
		require.NotEmpty(t, field.PrimaryDBExpression)
		require.NotEmpty(t, field.OLAPExpression)
		require.NotEmpty(t, field.Name)

		usageFieldName := field.Name
		assert.Contains(t, usageFieldNames, usageFieldName)
		assert.NotContains(t, seenUsageFields, usageFieldName)
		seenUsageFields[usageFieldName] = struct{}{}

		assert.NotEqual(t, usagepb.UsageAlertingMetric_UNKNOWN, field.AlertingMetric)
		assert.Contains(t, alertingMetrics, field.AlertingMetric)
		assert.Equal(t, strings.ToUpper(field.Name), field.AlertingMetric.String())
		assert.NotContains(t, seenAlertingMetrics, field.AlertingMetric)
		seenAlertingMetrics[field.AlertingMetric] = struct{}{}
	}

	assert.Equal(t, usageFieldNames, seenUsageFields)
	assert.Equal(t, alertingMetrics, seenAlertingMetrics)
}

func TestNew_ReadUsageFromOLAPDBRequiresOLAPDB(t *testing.T) {
	flags.Set(t, "app.read_usage_from_olap_db", true)
	env := testenv.GetTestEnv(t)

	// Enabling OLAP reads without configuring an OLAP DB should fail during
	// service setup instead of falling back to primary DB reads later.
	service, err := usage_service.New(env, clockwork.NewFakeClock())

	require.Nil(t, service)
	assert.True(t, status.IsFailedPreconditionError(err))
}

func TestUsageAlertingRules_CreateListDelete(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"US1": testUsageUser("US1", "GR1", cappb.Capability_ORG_ADMIN),
	})
	env.SetAuthenticator(ta)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	// Freeze DB timestamps so the created timestamp is deterministic.
	now := time.Date(2024, 2, 22, 12, 0, 0, 0, time.UTC)
	clock := clockwork.NewFakeClockAt(now)
	env.GetDBHandle().SetNowFunc(clock.Now)
	service, err := usage_service.New(env, clock)
	require.NoError(t, err)

	// Create a usage alerting rule for the authenticated org.
	createRsp, err := service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		Configuration: &usagepb.UsageAlertingRuleConfiguration{
			Metric:            usagepb.UsageAlertingMetric_CLOUD_RBE_CPU_NANOS,
			AbsoluteThreshold: 123,
			Window:            usagepb.UsageAlertingWindow_WEEK,
		},
	})
	require.NoError(t, err)
	ruleID := createRsp.GetUsageAlertingRule().GetMetadata().GetUsageAlertingRuleId()
	require.NotEmpty(t, ruleID)

	// The created rule includes server-controlled metadata and the saved configuration.
	expectedRule := &usagepb.UsageAlertingRule{
		Metadata: &usagepb.UsageAlertingRuleMetadata{
			UsageAlertingRuleId: ruleID,
			CreatedByUser: &uidpb.DisplayUser{
				UserId: &uidpb.UserId{Id: "US1"},
			},
			CreatedTimestamp: timestamppb.New(now),
		},
		Configuration: &usagepb.UsageAlertingRuleConfiguration{
			Metric:            usagepb.UsageAlertingMetric_CLOUD_RBE_CPU_NANOS,
			AbsoluteThreshold: 123,
			Window:            usagepb.UsageAlertingWindow_WEEK,
		},
		Status: &usagepb.UsageAlertingRuleStatus{},
	}
	assert.Empty(t, cmp.Diff(expectedRule, createRsp.GetUsageAlertingRule(), protocmp.Transform()))

	// Simulate evaluator-controlled status updates and verify listing returns them.
	lastFired := now.Add(20 * time.Minute)
	err = env.GetDBHandle().NewQuery(ctx, "test_update_usage_alerting_rule_status").Raw(`
		UPDATE "UsageAlertingRules"
		SET last_fired_usec = ?
		WHERE usage_alerting_rule_id = ?
	`, lastFired.UnixMicro(), ruleID).Exec().Error
	require.NoError(t, err)

	listRsp, err := service.GetUsageAlertingRules(ctx1, &usagepb.GetUsageAlertingRulesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
	})
	require.NoError(t, err)
	expectedRule.Status = &usagepb.UsageAlertingRuleStatus{
		LastFiredTimestamp: timestamppb.New(lastFired),
	}
	assert.Empty(t, cmp.Diff(
		&usagepb.GetUsageAlertingRulesResponse{UsageAlertingRule: []*usagepb.UsageAlertingRule{expectedRule}},
		listRsp,
		protocmp.Transform(),
	))

	// Deleting the rule removes it from subsequent list responses.
	_, err = service.DeleteUsageAlertingRule(ctx1, &usagepb.DeleteUsageAlertingRuleRequest{
		RequestContext:      &ctxpb.RequestContext{GroupId: "GR1"},
		UsageAlertingRuleId: ruleID,
	})
	require.NoError(t, err)
	listRsp, err = service.GetUsageAlertingRules(ctx1, &usagepb.GetUsageAlertingRulesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
	})
	require.NoError(t, err)
	assert.Empty(t, listRsp.GetUsageAlertingRule())
}

func TestUsageAlertingRules_AreScopedToAuthenticatedGroup(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"US1": testUsageUser("US1", "GR1", cappb.Capability_ORG_ADMIN),
		"US2": testUsageUser("US2", "GR2", cappb.Capability_ORG_ADMIN),
	})
	env.SetAuthenticator(ta)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	ctx2, err := ta.WithAuthenticatedUser(ctx, "US2")
	require.NoError(t, err)

	// Create a rule as a user in GR1.
	now := time.Date(2024, 2, 22, 12, 0, 0, 0, time.UTC)
	clock := clockwork.NewFakeClockAt(now)
	env.GetDBHandle().SetNowFunc(clock.Now)
	service, err := usage_service.New(env, clock)
	require.NoError(t, err)
	createRsp, err := service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
		Configuration: &usagepb.UsageAlertingRuleConfiguration{
			Metric:            usagepb.UsageAlertingMetric_TOTAL_WORKFLOW_DOWNLOAD_SIZE_BYTES,
			AbsoluteThreshold: 1,
			Window:            usagepb.UsageAlertingWindow_DAY,
		},
	})
	require.NoError(t, err)

	// A user in GR2 cannot list or delete GR1's rule.
	listRsp, err := service.GetUsageAlertingRules(ctx2, &usagepb.GetUsageAlertingRulesRequest{})
	require.NoError(t, err)
	assert.Empty(t, listRsp.GetUsageAlertingRule())
	_, err = service.DeleteUsageAlertingRule(ctx2, &usagepb.DeleteUsageAlertingRuleRequest{
		UsageAlertingRuleId: createRsp.GetUsageAlertingRule().GetMetadata().GetUsageAlertingRuleId(),
	})
	assert.True(t, status.IsNotFoundError(err))

	// The original GR1 user can still see the rule after the failed GR2 delete.
	listRsp, err = service.GetUsageAlertingRules(ctx1, &usagepb.GetUsageAlertingRulesRequest{})
	require.NoError(t, err)
	require.Len(t, listRsp.GetUsageAlertingRule(), 1)
}

func TestUsageAlertingRules_RequiresOrgAdmin(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"US1": testUsageUser("US1", "GR1"),
	})
	env.SetAuthenticator(ta)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	service, err := usage_service.New(env, clockwork.NewFakeClock())
	require.NoError(t, err)

	// A group member without ORG_ADMIN cannot list usage alerting rules.
	_, err = service.GetUsageAlertingRules(ctx1, &usagepb.GetUsageAlertingRulesRequest{})
	assert.True(t, status.IsPermissionDeniedError(err))

	// The same non-admin member cannot create usage alerting rules, even with a valid configuration.
	_, err = service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
		Configuration: &usagepb.UsageAlertingRuleConfiguration{
			Metric:            usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
			AbsoluteThreshold: 1,
			Window:            usagepb.UsageAlertingWindow_DAY,
		},
	})
	assert.True(t, status.IsPermissionDeniedError(err))

	// The same non-admin member cannot delete usage alerting rules.
	_, err = service.DeleteUsageAlertingRule(ctx1, &usagepb.DeleteUsageAlertingRuleRequest{
		UsageAlertingRuleId: "UAR1",
	})
	assert.True(t, status.IsPermissionDeniedError(err))
}

func TestUsageAlertingRules_ValidateConfiguration(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"US1": testUsageUser("US1", "GR1", cappb.Capability_ORG_ADMIN),
	})
	env.SetAuthenticator(ta)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	service, err := usage_service.New(env, clockwork.NewFakeClock())
	require.NoError(t, err)

	for _, testCase := range []struct {
		name   string
		config *usagepb.UsageAlertingRuleConfiguration
	}{
		{
			name: "MissingConfiguration",
		},
		{
			name: "MissingUsageAlertingMetric",
			config: &usagepb.UsageAlertingRuleConfiguration{
				AbsoluteThreshold: 1,
				Window:            usagepb.UsageAlertingWindow_DAY,
			},
		},
		{
			name: "NegativeThreshold",
			config: &usagepb.UsageAlertingRuleConfiguration{
				Metric:            usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
				AbsoluteThreshold: -1,
				Window:            usagepb.UsageAlertingWindow_DAY,
			},
		},
		{
			name: "ZeroThreshold",
			config: &usagepb.UsageAlertingRuleConfiguration{
				Metric:            usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
				AbsoluteThreshold: 0,
				Window:            usagepb.UsageAlertingWindow_DAY,
			},
		},
		{
			name: "MissingWindow",
			config: &usagepb.UsageAlertingRuleConfiguration{
				Metric:            usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
				AbsoluteThreshold: 1,
			},
		},
		{
			name: "UnsupportedUsageAlertingMetric",
			config: &usagepb.UsageAlertingRuleConfiguration{
				Metric:            usagepb.UsageAlertingMetric_Value(999),
				AbsoluteThreshold: 1,
				Window:            usagepb.UsageAlertingWindow_DAY,
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// Invalid user-controlled configuration is rejected before inserting a row.
			_, err := service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
				Configuration: testCase.config,
			})
			assert.True(t, status.IsInvalidArgumentError(err))
		})
	}

	// None of the invalid create requests should have persisted a rule.
	listRsp, err := service.GetUsageAlertingRules(ctx1, &usagepb.GetUsageAlertingRulesRequest{})
	require.NoError(t, err)
	assert.Empty(t, listRsp.GetUsageAlertingRule())
}

func TestUsageAlertingRules_DuplicateConfigurationRejected(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"US1": testUsageUser("US1", "GR1", cappb.Capability_ORG_ADMIN),
	})
	env.SetAuthenticator(ta)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	service, err := usage_service.New(env, clockwork.NewFakeClock())
	require.NoError(t, err)

	configuration := &usagepb.UsageAlertingRuleConfiguration{
		Metric:            usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
		AbsoluteThreshold: 123,
		Window:            usagepb.UsageAlertingWindow_DAY,
	}

	// Creating a rule with a new configuration succeeds.
	_, err = service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
		Configuration: configuration,
	})
	require.NoError(t, err)

	// Creating another rule with the exact same configuration is rejected.
	_, err = service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
		Configuration: configuration,
	})
	assert.True(t, status.IsAlreadyExistsError(err))

	// A rule that changes any part of the configuration is still allowed.
	_, err = service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
		Configuration: &usagepb.UsageAlertingRuleConfiguration{
			Metric:            usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
			AbsoluteThreshold: 456,
			Window:            usagepb.UsageAlertingWindow_DAY,
		},
	})
	require.NoError(t, err)
}

func TestUsageAlertingRules_MaxRulesPerGroup(t *testing.T) {
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, map[string]interfaces.UserInfo{
		"US1": testUsageUser("US1", "GR1", cappb.Capability_ORG_ADMIN),
	})
	env.SetAuthenticator(ta)
	ctx1, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	service, err := usage_service.New(env, clockwork.NewFakeClock())
	require.NoError(t, err)

	// Rules up to the per-group cap are accepted.
	for i := range usage_service.MaxUsageAlertingRulesPerGroup {
		_, err := service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
			Configuration: &usagepb.UsageAlertingRuleConfiguration{
				Metric:            usagepb.UsageAlertingMetric_TOTAL_CACHED_ACTION_EXEC_USEC,
				AbsoluteThreshold: int64(i + 1),
				Window:            usagepb.UsageAlertingWindow_DAY,
			},
		})
		require.NoError(t, err)
	}

	// The next rule would exceed the per-group cap, so it is rejected.
	_, err = service.CreateUsageAlertingRule(ctx1, &usagepb.CreateUsageAlertingRuleRequest{
		Configuration: &usagepb.UsageAlertingRuleConfiguration{
			Metric:            usagepb.UsageAlertingMetric_TOTAL_CACHED_ACTION_EXEC_USEC,
			AbsoluteThreshold: int64(usage_service.MaxUsageAlertingRulesPerGroup + 1),
			Window:            usagepb.UsageAlertingWindow_DAY,
		},
	})
	assert.True(t, status.IsResourceExhaustedError(err))
}
