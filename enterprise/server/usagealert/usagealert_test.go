package main

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

func TestEvaluatorSendsAlertsToAllAdminAccounts(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// Create an over-threshold rule with three admin accounts, including two
	// accounts that share the same email address.
	sender := &fakeEmailSender{}
	metrics := testEvaluatorMetrics(t)
	evaluator, dbh, olapDBH := newTestEvaluator(t, now, sender, metrics)
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_CAS_CACHE_HITS,
		AbsoluteThreshold:   100,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "One", "admin1@example.com")
	createAdminRecipient(t, ctx, dbh, "GR1", "UA2", "Admin", "One GitHub", "admin1@example.com")
	createAdminRecipient(t, ctx, dbh, "GR1", "UA3", "Admin", "Two", "admin2@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.RemoteCacheCASHits, nil, 101)

	result, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// Admin accounts should receive email, and the rule should record that it
	// fired for the day window that was queried.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 3}, result)
	require.Len(t, sender.messages, 3)
	assert.Equal(t, "admin1@example.com", sender.messages[0].To.Email)
	assert.Equal(t, "admin1@example.com", sender.messages[1].To.Email)
	assert.Equal(t, "admin2@example.com", sender.messages[2].To.Email)
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Equal(t, now.UnixMicro(), rule.LastFiredUsec)
}

func TestEvaluatorDoesNotRefireWithinSameWindow(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)
	lastFiredUsec := time.Date(2026, 5, 11, 1, 0, 0, 0, time.UTC).UnixMicro()

	// The rule is still over threshold, but it already fired inside the current
	// week window.
	sender := &fakeEmailSender{}
	evaluator, dbh, olapDBH := newTestEvaluator(t, now, sender, testEvaluatorMetrics(t))
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
		AbsoluteThreshold:   10,
		Window:              usagepb.UsageAlertingWindow_WEEK,
		LastFiredUsec:       lastFiredUsec,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.BuildEventsBESCount, nil, 11)

	result, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// The evaluator should not send a duplicate alert or move the existing fire
	// timestamp.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 0}, result)
	assert.Empty(t, sender.messages)
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Equal(t, lastFiredUsec, rule.LastFiredUsec)
}

func TestEvaluatorFormatsDurationAlertsAsMinutes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// Cached build duration thresholds are stored as nanoseconds, but the UI
	// presents them as minutes.
	sender := &fakeEmailSender{}
	evaluator, dbh, olapDBH := newTestEvaluator(t, now, sender, testEvaluatorMetrics(t))
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_CACHED_BUILD_DURATION,
		AbsoluteThreshold:   60e9,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.RemoteCacheACCachedExecDurationNanos, nil, 2*60e9)

	result, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// The email should use the same label and minute formatting as the UI, with
	// no raw nanosecond values leaked to the recipient.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 1}, result)
	require.Len(t, sender.messages, 1)
	assert.Equal(t, "[BuildBuddy Usage Alert] [GR1] Daily Cached build minutes threshold reached", sender.messages[0].Subject)
	assert.Contains(t, sender.messages[0].Body, "Cached build minutes")
	assert.Contains(t, sender.messages[0].Body, "2 minutes")
	assert.Contains(t, sender.messages[0].Body, "1 minute")
	assert.NotContains(t, sender.messages[0].Body, "120000000000")
	assert.NotContains(t, sender.messages[0].Body, "60000000000")
}

func TestEvaluatorBelowThresholdDoesNotUpdateRule(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// Usage equal to the threshold is not a firing condition; alerts fire only
	// when usage exceeds the configured value.
	sender := &fakeEmailSender{}
	evaluator, dbh, olapDBH := newTestEvaluator(t, now, sender, testEvaluatorMetrics(t))
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
		AbsoluteThreshold:   100,
		Window:              usagepb.UsageAlertingWindow_MONTH,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.RemoteCacheCASDownloadedBytes, nil, 100)

	result, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// The rule should not send email or update its fire timestamp.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 0}, result)
	assert.Empty(t, sender.messages)
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Zero(t, rule.LastFiredUsec)
}

func TestEvaluatorQueriesLabeledUsageFromClickHouse(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// External download alerts should count only RawUsage rows whose origin
	// label is external, even when the same SKU has larger internal usage.
	sender := &fakeEmailSender{}
	evaluator, dbh, olapDBH := newTestEvaluator(t, now, sender, testEvaluatorMetrics(t))
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_EXTERNAL_DOWNLOAD_SIZE_BYTES,
		AbsoluteThreshold:   100,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.RemoteCacheCASDownloadedBytes, map[sku.LabelName]sku.LabelValue{
		sku.Origin: sku.OriginExternal,
	}, 101)
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.RemoteCacheCASDownloadedBytes, map[sku.LabelName]sku.LabelValue{
		sku.Origin: sku.OriginInternal,
	}, 1000)

	result, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// The alert should fire based on the external value, not the total SKU count.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 1}, result)
	require.Len(t, sender.messages, 1)
	assert.Contains(t, sender.messages[0].Body, "101B")
	assert.NotContains(t, sender.messages[0].Body, "1.101KB")
}

func TestAlertEmailUsesGroupNameAndSubdomainUsageURL(t *testing.T) {
	u, err := url.Parse("https://app.buildbuddy.io")
	require.NoError(t, err)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "app.enable_subdomain_matching", true)
	start := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)

	// URL-identified groups should get their own subdomain link when
	// subdomains are enabled.
	msg, err := buildAlertEmail(&alertRule{
		rule: &tables.UsageAlertingRule{
			UsageAlertingRuleID: "UR1",
			GroupID:             "GR1",
			AbsoluteThreshold:   1,
			Window:              usagepb.UsageAlertingWindow_WEEK,
		},
		groupName:          "Acme <Corp>",
		groupURLIdentifier: "acme",
	}, metricDefinitions[usagepb.UsageAlertingMetric_INVOCATIONS], email.Address{Email: "admin@example.com"}, 2, start, end)
	require.NoError(t, err)

	assert.Equal(t, "[BuildBuddy Usage Alert] [Acme <Corp>] Weekly Invocations threshold reached", msg.Subject)
	assert.Equal(t, `<p><strong>Weekly Invocations</strong> usage alert fired for organization <strong>Acme &lt;Corp&gt;</strong>.</p>
<p>Usage for <strong>Invocations</strong> is <strong>2</strong>, which exceeds the configured threshold of <strong>1</strong>.</p>
<p>Window: 2026-05-11T00:00:00Z to 2026-05-18T00:00:00Z UTC</p>
<p>See usage at <a href="https://acme.buildbuddy.io/usage">https://acme.buildbuddy.io/usage</a></p>`, msg.Body)
}

func TestAlertEmailUsesGroupSlugAndBaseUsageURL(t *testing.T) {
	u, err := url.Parse("http://localhost:8080")
	require.NoError(t, err)
	flags.Set(t, "app.build_buddy_url", *u)
	flags.Set(t, "app.enable_subdomain_matching", false)
	start := time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC)

	// Without a group name, the subject should fall back to the group slug. With
	// subdomains disabled, the email should still link to the base usage page.
	msg, err := buildAlertEmail(&alertRule{
		rule: &tables.UsageAlertingRule{
			UsageAlertingRuleID: "UR1",
			GroupID:             "GR1",
			AbsoluteThreshold:   1,
			Window:              usagepb.UsageAlertingWindow_MONTH,
		},
		groupURLIdentifier: "acme",
	}, metricDefinitions[usagepb.UsageAlertingMetric_INVOCATIONS], email.Address{Email: "admin@example.com"}, 2, start, end)
	require.NoError(t, err)

	assert.Equal(t, "[BuildBuddy Usage Alert] [acme] Monthly Invocations threshold reached", msg.Subject)
	assert.Equal(t, `<p><strong>Monthly Invocations</strong> usage alert fired for organization <strong>GR1</strong>.</p>
<p>Usage for <strong>Invocations</strong> is <strong>2</strong>, which exceeds the configured threshold of <strong>1</strong>.</p>
<p>Window: 2026-05-11T00:00:00Z to 2026-05-18T00:00:00Z UTC</p>
<p>See usage at <a href="http://localhost:8080/usage">http://localhost:8080/usage</a></p>`, msg.Body)
}

func TestEvaluatorRecordsErrorMetric(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)
	metrics := testEvaluatorMetrics(t)

	// Break the metric SQL after the rule and recipient rows have been loaded
	// from SQL.
	evaluator, dbh, _ := newTestEvaluator(t, now, &fakeEmailSender{}, metrics)
	setOLAPExpressionForMetric(t, usagepb.UsageAlertingMetric_INVOCATIONS, "invalid_clickhouse_expression(")
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
		AbsoluteThreshold:   10,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")

	result, err := evaluator.Run(ctx)
	require.Error(t, err)

	// The query failure should increment the query-stage metric and leave the
	// rule status unchanged because evaluation did not complete.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 0}, result)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.errors.WithLabelValues(errorStageQueryUsage)))
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Zero(t, rule.LastFiredUsec)
}

func TestRunEvaluatorPushesMetricsAfterEvaluationError(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// The wrapped runner should push metrics even when evaluation itself
	// returns an error.
	evaluator, dbh, _ := newTestEvaluator(t, now, &fakeEmailSender{}, testEvaluatorMetrics(t))
	setOLAPExpressionForMetric(t, usagepb.UsageAlertingMetric_INVOCATIONS, "invalid_clickhouse_expression(")
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
		AbsoluteThreshold:   10,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	pusher := &fakeMetricsPusher{}

	result, err := runEvaluator(ctx, evaluator, pusher)
	require.Error(t, err)

	// A failed run still returns the partial evaluation result and attempts to
	// push metrics.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 0}, result)
	assert.Equal(t, 1, pusher.calls)
}

func TestEvaluatorQueryRulesAndRecipientsReturnsAllAdminAccounts(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	dbh := te.GetDBHandle()
	evaluator := newDBOnlyEvaluator(dbh)

	// Seed a group with multiple active admins, plus users that should not
	// receive alerts because they have no email, wrong role, or pending status.
	require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_group").Create(&tables.Group{GroupID: "GR1", Name: "Acme Corp", URLIdentifier: "acme"}))
	for _, row := range []*tables.User{
		{UserID: "UA1", FirstName: "Admin", LastName: "One", Email: "admin@example.com"},
		{UserID: "UA2", FirstName: "Admin", LastName: "Two", Email: "admin@example.com"},
		{UserID: "UA3", FirstName: "Blank", LastName: "Email"},
		{UserID: "UC1", FirstName: "Role", LastName: "Combo", Email: "combo@example.com"},
		{UserID: "UD1", FirstName: "Developer", Email: "developer@example.com"},
		{UserID: "UI1", FirstName: "Invited", Email: "invited@example.com"},
		{UserID: "UO1", FirstName: "Other", Email: "other@example.com"},
	} {
		require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_user").Create(row))
	}
	for _, row := range []*tables.UserGroup{
		{UserUserID: "UA1", GroupGroupID: "GR1", Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: "UA2", GroupGroupID: "GR1", Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: "UA3", GroupGroupID: "GR1", Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: "UC1", GroupGroupID: "GR1", Role: uint32(role.Admin | role.Developer), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: "UD1", GroupGroupID: "GR1", Role: uint32(role.Developer), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: "UI1", GroupGroupID: "GR1", Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_REQUESTED)},
		{UserUserID: "UO1", GroupGroupID: "GR2", Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
	} {
		require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_user_group").Create(row))
	}
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
		AbsoluteThreshold:   1,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})

	rules, err := evaluator.queryRulesAndRecipients(ctx)
	require.NoError(t, err)

	// Only active admins with non-empty email addresses in the rule's group
	// should be attached as recipients.
	require.Len(t, rules, 1)
	assert.Equal(t, "UR1", rules[0].rule.UsageAlertingRuleID)
	assert.Equal(t, "Acme Corp", rules[0].groupName)
	assert.Equal(t, "acme", rules[0].groupURLIdentifier)
	assert.Equal(t, []email.Address{
		{Name: "Admin One", Email: "admin@example.com"},
		{Name: "Admin Two", Email: "admin@example.com"},
		{Name: "Role Combo", Email: "combo@example.com"},
	}, rules[0].recipients)
}

func TestEvaluatorQueryRulesAndRecipientsOnlyReturnsGroupsWithAdminRecipients(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	dbh := te.GetDBHandle()
	evaluator := newDBOnlyEvaluator(dbh)

	// Create rules across groups where only GR1 has a member admin with a
	// deliverable email address.
	for _, row := range []*tables.User{
		{UserID: "UA1", Email: "admin@example.com"},
		{UserID: "UB1"},
		{UserID: "UD1", Email: "developer@example.com"},
		{UserID: "UR1", Email: "requested@example.com"},
	} {
		require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_user").Create(row))
	}
	for _, row := range []*tables.UserGroup{
		{UserUserID: "UA1", GroupGroupID: "GR1", Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: "UB1", GroupGroupID: "GR2", Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: "UD1", GroupGroupID: "GR3", Role: uint32(role.Developer), MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER)},
		{UserUserID: "UR1", GroupGroupID: "GR4", Role: uint32(role.Admin), MembershipStatus: int32(grpb.GroupMembershipStatus_REQUESTED)},
	} {
		require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_user_group").Create(row))
	}
	for _, row := range []*tables.UsageAlertingRule{
		{
			UsageAlertingRuleID: "UR1",
			GroupID:             "GR1",
			UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
			AbsoluteThreshold:   1,
			Window:              usagepb.UsageAlertingWindow_DAY,
		},
		{
			UsageAlertingRuleID: "UR2",
			GroupID:             "GR2",
			UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
			AbsoluteThreshold:   1,
			Window:              usagepb.UsageAlertingWindow_DAY,
		},
		{
			UsageAlertingRuleID: "UR3",
			GroupID:             "GR3",
			UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
			AbsoluteThreshold:   1,
			Window:              usagepb.UsageAlertingWindow_DAY,
		},
		{
			UsageAlertingRuleID: "UR4",
			GroupID:             "GR4",
			UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
			AbsoluteThreshold:   1,
			Window:              usagepb.UsageAlertingWindow_DAY,
		},
		{
			UsageAlertingRuleID: "UR5",
			GroupID:             "GR5",
			UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
			AbsoluteThreshold:   1,
			Window:              usagepb.UsageAlertingWindow_DAY,
		},
	} {
		createUsageAlertingRule(t, ctx, dbh, row)
	}

	rules, err := evaluator.queryRulesAndRecipients(ctx)
	require.NoError(t, err)

	// Rules without recipients should not be evaluated.
	require.Len(t, rules, 1)
	assert.Equal(t, "UR1", rules[0].rule.UsageAlertingRuleID)
}

func TestMetricDefinitionsCoverEveryAlertingMetric(t *testing.T) {
	for _, metric := range alertingMetricValues() {
		// Every user-selectable alerting metric should have email display
		// metadata.
		_, ok := metricDefinitions[metric]
		assert.True(t, ok, "missing metric definition for %s", metric)
	}
	assert.Len(t, metricDefinitions, len(alertingMetricValues()))
}

func TestWindowRange(t *testing.T) {
	// Wednesday, May 13, 2026 should fall in the Monday-start UTC week
	// beginning May 11 and ending May 18.
	now := time.Date(2026, 5, 13, 15, 30, 0, 0, time.UTC)

	start, end, err := windowRange(now, usagepb.UsageAlertingWindow_WEEK)
	require.NoError(t, err)

	// The evaluator uses half-open ranges, so the end is the first instant
	// after the active week.
	assert.Equal(t, time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC), end)
}

func newTestEvaluator(t testing.TB, now time.Time, sender emailSender, metrics *evaluatorMetrics) (*Evaluator, interfaces.DBHandle, interfaces.OLAPDBHandle) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)
	te := testenv.GetTestEnv(t)
	return NewEvaluator(te.GetDBHandle(), te.GetOLAPDBHandle(), sender, clockwork.NewFakeClockAt(now), metrics), te.GetDBHandle(), te.GetOLAPDBHandle()
}

func testEvaluatorMetrics(t testing.TB) *evaluatorMetrics {
	metrics, err := newEvaluatorMetrics()
	require.NoError(t, err)
	return metrics
}

func setOLAPExpressionForMetric(t testing.TB, metric usagepb.UsageAlertingMetric_Value, expression string) {
	field, ok := usage_service.UsageFieldForAlertingMetric(metric)
	require.True(t, ok)
	originalExpression := field.OLAPExpression
	field.OLAPExpression = expression
	t.Cleanup(func() {
		field.OLAPExpression = originalExpression
	})
}

func newDBOnlyEvaluator(dbh interfaces.DBHandle) *Evaluator {
	return NewEvaluator(dbh, nil, nil, clockwork.NewFakeClockAt(time.Unix(0, 0).UTC()), nil)
}

func createUsageAlertingRule(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, rule *tables.UsageAlertingRule) {
	require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_usage_alerting_rule").Create(rule))
}

func createRawUsage(t testing.TB, ctx context.Context, olapDBH interfaces.OLAPDBHandle, groupID string, periodStart time.Time, usageSKU sku.SKU, labels map[sku.LabelName]sku.LabelValue, count int64) {
	require.NoError(t, olapDBH.FlushUsages(ctx, []*schema.RawUsage{
		{
			GroupID:     groupID,
			SKU:         usageSKU,
			Labels:      labels,
			PeriodStart: periodStart,
			Count:       count,
		},
	}))
}

func createAdminRecipient(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, groupID, userID, firstName, lastName, emailAddress string) {
	require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_user").Create(&tables.User{
		UserID:    userID,
		FirstName: firstName,
		LastName:  lastName,
		Email:     emailAddress,
	}))
	require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_user_group").Create(&tables.UserGroup{
		UserUserID:       userID,
		GroupGroupID:     groupID,
		Role:             uint32(role.Admin),
		MembershipStatus: int32(grpb.GroupMembershipStatus_MEMBER),
	}))
}

func getUsageAlertingRule(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, ruleID string) *tables.UsageAlertingRule {
	rule := &tables.UsageAlertingRule{}
	require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_get_usage_alerting_rule").Raw(`
		SELECT *
		FROM "UsageAlertingRules"
		WHERE usage_alerting_rule_id = ?
	`, ruleID).Take(rule))
	return rule
}

type fakeEmailSender struct {
	mu       sync.Mutex
	messages []*email.Message
	err      error
}

func (s *fakeEmailSender) Send(ctx context.Context, msg *email.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.messages = append(s.messages, msg)
	return nil
}

type fakeMetricsPusher struct {
	calls int
	err   error
}

func (p *fakeMetricsPusher) Push() error {
	p.calls++
	return p.err
}
