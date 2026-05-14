package usage_service_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
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
		GroupID: "GR1",
		Model: tables.Model{
			CreatedAtUsec: time.Date(2023, 7, 9, 0, 0, 0, 0, time.UTC).UnixMicro(),
		},
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
	service := usage_service.New(env, clock)
	// Insert some usage data:
	for _, row := range []*tables.Usage{
		// GR1, current usage period
		{
			UsageID:         "UG1",
			GroupID:         "GR1",
			PeriodStartUsec: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			UsageCounts:     tables.UsageCounts{Invocations: 13, CASCacheHits: 10_000},
		},
		{
			UsageID:         "UG1",
			GroupID:         "GR1",
			PeriodStartUsec: time.Date(2024, 2, 4, 1, 0, 0, 0, time.UTC).UnixMicro(),
			UsageCounts:     tables.UsageCounts{Invocations: 15, CASCacheHits: 12_000},
		},
		// GR1, previous usage period
		{
			UsageID:         "UG2",
			GroupID:         "GR1",
			PeriodStartUsec: time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			UsageCounts:     tables.UsageCounts{CASCacheHits: 77},
		},
		// GR2, current usage period
		{
			UsageID:         "UG3",
			GroupID:         "GR2",
			PeriodStartUsec: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
			UsageCounts:     tables.UsageCounts{Invocations: 107},
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

func TestUsageFields_CoverEveryUsageFieldAndAlertingMetric(t *testing.T) {
	usageFieldNames := map[string]struct{}{}
	fields := (&usagepb.Usage{}).ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		fieldName := string(fields.Get(i).Name())
		if fieldName == "period" {
			continue
		}
		usageFieldNames[fieldName] = struct{}{}
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
		require.NotEmpty(t, field.Name)

		usageFieldName := field.Name
		assert.Contains(t, usageFieldNames, usageFieldName)
		assert.NotContains(t, seenUsageFields, usageFieldName)
		seenUsageFields[usageFieldName] = struct{}{}

		assert.NotEqual(t, usagepb.UsageAlertingMetric_UNKNOWN, field.AlertingMetric)
		assert.Contains(t, alertingMetrics, field.AlertingMetric)
		assert.NotContains(t, seenAlertingMetrics, field.AlertingMetric)
		seenAlertingMetrics[field.AlertingMetric] = struct{}{}
	}

	assert.Equal(t, usageFieldNames, seenUsageFields)
	assert.Equal(t, alertingMetrics, seenAlertingMetrics)
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
	service := usage_service.New(env, clock)

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
	lastEvaluation := now.Add(10 * time.Minute)
	lastFired := now.Add(20 * time.Minute)
	err = env.GetDBHandle().NewQuery(ctx, "test_update_usage_alerting_rule_status").Raw(`
		UPDATE "UsageAlertingRules"
		SET last_evaluation_usec = ?, last_fired_usec = ?
		WHERE usage_alerting_rule_id = ?
	`, lastEvaluation.UnixMicro(), lastFired.UnixMicro(), ruleID).Exec().Error
	require.NoError(t, err)

	listRsp, err := service.GetUsageAlertingRules(ctx1, &usagepb.GetUsageAlertingRulesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
	})
	require.NoError(t, err)
	expectedRule.Status = &usagepb.UsageAlertingRuleStatus{
		LastEvaluationTimestamp: timestamppb.New(lastEvaluation),
		LastFiredTimestamp:      timestamppb.New(lastFired),
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
	service := usage_service.New(env, clock)
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
	service := usage_service.New(env, clockwork.NewFakeClock())

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
	service := usage_service.New(env, clockwork.NewFakeClock())

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
	service := usage_service.New(env, clockwork.NewFakeClock())

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
	service := usage_service.New(env, clockwork.NewFakeClock())

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
