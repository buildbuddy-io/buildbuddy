package main

import (
	"context"
	"errors"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

func TestEvaluatorSendsAlertsToUniqueAdminEmailRecipients(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// Create a rule whose usage exceeds the threshold, with three admin
	// accounts. Two admins share an email address (this can happen e.g. if a
	// gmail address is registered through both Google OIDC and GitHub OIDC).
	// Also create some users that shouldn't receive emails (non-admins /
	// missing email).
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
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "One", " admin1@example.com ")
	createAdminRecipient(t, ctx, dbh, "GR1", "UA2", "Admin", "One GitHub", "admin1@example.com")
	createAdminRecipient(t, ctx, dbh, "GR1", "UA3", "Admin", "Two", "admin2@example.com")
	createAdminRecipient(t, ctx, dbh, "GR1", "UA4", "Admin", "No Email", "")
	createUserAndMembership(t, ctx, dbh, "GR1", "UD1", "Developer", "", "developer@example.com", uint32(role.Developer), int32(grpb.GroupMembershipStatus_MEMBER))
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.RemoteCacheCASHits, nil, 101)

	result, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// Unique admin email addresses should receive a shared email, and the rule
	// should record that it fired for the day window that was queried.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 2}, result)
	require.Len(t, sender.messages, 1)
	assert.Equal(t, []email.Address{
		{Name: "Admin One", Email: "admin1@example.com"},
		{Name: "Admin Two", Email: "admin2@example.com"},
	}, sender.messages[0].ToAddresses)
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Equal(t, now.UnixMicro(), rule.LastFiredUsec)
}

func TestEvaluatorDoesNotRefireWithinSameWindow(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)
	lastFiredUsec := time.Date(2026, 5, 11, 1, 0, 0, 0, time.UTC).UnixMicro()

	// The rule usage still exceeds the threshold, but the rule already fired
	// inside the current week window.
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

	// The evaluator should not send a duplicate alert or change the existing
	// fire timestamp.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 0}, result)
	assert.Empty(t, sender.messages)
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Equal(t, lastFiredUsec, rule.LastFiredUsec)
}

func TestEvaluatorFiresAgainInNextWindow(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// Run the evaluator once for a daily rule, then advance the clock into the
	// next UTC day with fresh usage that exceeds the threshold.
	sender := &fakeEmailSender{}
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)
	te := testenv.GetTestEnv(t)
	clock := clockwork.NewFakeClockAt(now)
	evaluator := NewEvaluator(te.GetDBHandle(), te.GetOLAPDBHandle(), sender, clock, testEvaluatorMetrics(t))
	dbh := te.GetDBHandle()
	olapDBH := te.GetOLAPDBHandle()
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
		AbsoluteThreshold:   10,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.BuildEventsBESCount, nil, 11)

	firstResult, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// The first run should send the initial alert and mark the current day as
	// fired.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 1}, firstResult)
	require.Len(t, sender.messages, 1)

	clock.Advance(24 * time.Hour)
	nextNow := clock.Now()
	createRawUsage(t, ctx, olapDBH, "GR1", nextNow, sku.BuildEventsBESCount, nil, 11)

	secondResult, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// Once the clock reaches the next window, the daily rule can fire again.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 1}, secondResult)
	require.Len(t, sender.messages, 2)
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Equal(t, nextNow.UnixMicro(), rule.LastFiredUsec)
}

func TestEvaluatorFormatsDurationAlertsAsMinutes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// Cached build duration is stored in RawUsage as nanoseconds, but alert
	// thresholds and emails should use the same microsecond unit as the Usage
	// API.
	sender := &fakeEmailSender{}
	evaluator, dbh, olapDBH := newTestEvaluator(t, now, sender, testEvaluatorMetrics(t))
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_TOTAL_CACHED_ACTION_EXEC_USEC,
		AbsoluteThreshold:   60e6,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.RemoteCacheACCachedExecDurationNanos, nil, 2*60e9)

	result, err := evaluator.Run(ctx)
	require.NoError(t, err)

	// The email should use the same label and minute formatting as the UI, and
	// it should not show raw nanosecond values to the recipient.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 1}, result)
	require.Len(t, sender.messages, 1)
	assert.Equal(t, "[BuildBuddy Usage Alert] [GR1] Daily Cached build minutes threshold reached", sender.messages[0].Subject)
	assert.Contains(t, sender.messages[0].Body, "Cached build minutes")
	assert.Contains(t, sender.messages[0].Body, "2 minutes")
	assert.Contains(t, sender.messages[0].Body, "1 minute")
	assert.NotContains(t, sender.messages[0].Body, "120000000000")
	assert.NotContains(t, sender.messages[0].Body, "60000000")
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

func TestEvaluatorDoesNotRecordRuleAfterSendFailure(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)
	previousLastFiredUsec := now.Add(-48 * time.Hour).UnixMicro()

	// The rule usage exceeds the threshold, but the email backend reports a
	// send failure. The evaluator should leave the previous fire timestamp so a
	// later cron run can retry the alert.
	sender := &fakeEmailSender{err: errors.New("test send failure")}
	evaluator, dbh, olapDBH := newTestEvaluator(t, now, sender, testEvaluatorMetrics(t))
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
		AbsoluteThreshold:   10,
		Window:              usagepb.UsageAlertingWindow_DAY,
		LastFiredUsec:       previousLastFiredUsec,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.BuildEventsBESCount, nil, 11)

	result, err := evaluator.Run(ctx)
	require.ErrorContains(t, err, "test send failure")

	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 0}, result)
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Equal(t, previousLastFiredUsec, rule.LastFiredUsec)
}

func TestEvaluatorSendsWhenRuleRecordedAfterQuery(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// Simulate a second evaluator run loading the rule before the first run
	// records that it fired. The second run should still send the alert so a
	// process exit between send and record does not drop alerts.
	sender := &fakeEmailSender{}
	evaluator, dbh, olapDBH := newTestEvaluator(t, now, sender, testEvaluatorMetrics(t))
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
		AbsoluteThreshold:   10,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")
	createRawUsage(t, ctx, olapDBH, "GR1", now, sku.BuildEventsBESCount, nil, 11)
	rules, err := evaluator.queryRulesAndRecipients(ctx)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	start, end, err := windowRange(now, usagepb.UsageAlertingWindow_DAY)
	require.NoError(t, err)
	recorded, err := evaluator.recordRuleFired(ctx, "UR1", now.UnixMicro(), start, end)
	require.NoError(t, err)
	require.True(t, recorded)

	alertsSent, err := evaluator.evaluateRule(ctx, now, rules[0])
	require.NoError(t, err)

	// The stale rule loaded in memory has usage above the threshold, so the
	// evaluator should send the alert even though recording the fire timestamp
	// sees that another evaluator already recorded this window.
	assert.Equal(t, 1, alertsSent)
	require.Len(t, sender.messages, 1)
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
		UsageAlertingMetric: usagepb.UsageAlertingMetric_TOTAL_EXTERNAL_DOWNLOAD_SIZE_BYTES,
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

	// Groups with URL identifiers should get their own subdomain link when
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
	}, metricDefinitions[usagepb.UsageAlertingMetric_INVOCATIONS], []email.Address{{Email: "admin@example.com"}}, 2, start, end)
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
	}, metricDefinitions[usagepb.UsageAlertingMetric_INVOCATIONS], []email.Address{{Email: "admin@example.com"}}, 2, start, end)
	require.NoError(t, err)

	assert.Equal(t, "[BuildBuddy Usage Alert] [acme] Monthly Invocations threshold reached", msg.Subject)
	assert.Equal(t, `<p><strong>Monthly Invocations</strong> usage alert fired for organization <strong>acme</strong>.</p>
<p>Usage for <strong>Invocations</strong> is <strong>2</strong>, which exceeds the configured threshold of <strong>1</strong>.</p>
<p>Window: 2026-05-11T00:00:00Z to 2026-05-18T00:00:00Z UTC</p>
<p>See usage at <a href="http://localhost:8080/usage">http://localhost:8080/usage</a></p>`, msg.Body)
}

func TestEvaluatorRecordsErrorMetric(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)
	metrics := testEvaluatorMetrics(t)

	// Make the usage query fail after the rule and recipient rows have been
	// loaded from SQL.
	evaluator, dbh := newTestEvaluatorWithUsage(t, now, &fakeEmailSender{}, metrics, &fakeUsageReader{
		errors: map[usagepb.UsageAlertingMetric_Value]error{
			usagepb.UsageAlertingMetric_INVOCATIONS: errors.New("test usage query"),
		},
	})
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

	// The query failure should increment the metric for the query stage and
	// leave the rule status unchanged because evaluation did not complete.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 1, AlertsSent: 0}, result)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.errors.WithLabelValues(errorStageQueryUsage)))
	rule := getUsageAlertingRule(t, ctx, dbh, "UR1")
	assert.Zero(t, rule.LastFiredUsec)
}

func TestEvaluatorContinuesAfterRuleEvaluationError(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// Make a metric query fail, then create another valid rule with usage above
	// the threshold. The evaluator should report the failure and keep evaluating
	// the rest of the run.
	sender := &fakeEmailSender{}
	metrics := testEvaluatorMetrics(t)
	evaluator, dbh := newTestEvaluatorWithUsage(t, now, sender, metrics, &fakeUsageReader{
		values: map[usagepb.UsageAlertingMetric_Value]int64{
			usagepb.UsageAlertingMetric_CAS_CACHE_HITS: 101,
		},
		errors: map[usagepb.UsageAlertingMetric_Value]error{
			usagepb.UsageAlertingMetric_INVOCATIONS: errors.New("test usage query"),
		},
	})
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR1",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_INVOCATIONS,
		AbsoluteThreshold:   10,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createUsageAlertingRule(t, ctx, dbh, &tables.UsageAlertingRule{
		UsageAlertingRuleID: "UR2",
		GroupID:             "GR1",
		UsageAlertingMetric: usagepb.UsageAlertingMetric_CAS_CACHE_HITS,
		AbsoluteThreshold:   100,
		Window:              usagepb.UsageAlertingWindow_DAY,
	})
	createAdminRecipient(t, ctx, dbh, "GR1", "UA1", "Admin", "", "admin@example.com")

	result, err := evaluator.Run(ctx)
	require.Error(t, err)

	// The rule whose query failed should return an error, while the valid rule
	// should still send and record its fire timestamp.
	require.Equal(t, &EvaluationResult{RulesEvaluated: 2, AlertsSent: 1}, result)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.errors.WithLabelValues(errorStageQueryUsage)))
	assert.Len(t, sender.messages, 1)
	assert.Zero(t, getUsageAlertingRule(t, ctx, dbh, "UR1").LastFiredUsec)
	assert.Equal(t, now.UnixMicro(), getUsageAlertingRule(t, ctx, dbh, "UR2").LastFiredUsec)
}

func TestRunEvaluatorPushesMetricsAfterEvaluationError(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 11, 15, 30, 0, 0, time.UTC)

	// The wrapped runner should push metrics even when evaluation itself
	// returns an error.
	evaluator, dbh := newTestEvaluatorWithUsage(t, now, &fakeEmailSender{}, testEvaluatorMetrics(t), &fakeUsageReader{
		errors: map[usagepb.UsageAlertingMetric_Value]error{
			usagepb.UsageAlertingMetric_INVOCATIONS: errors.New("test usage query"),
		},
	})
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

func TestEvaluatorQueryRulesAndRecipientsReturnsUniqueAdminEmails(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	dbh := te.GetDBHandle()
	evaluator := newDBOnlyEvaluator(dbh)

	// Seed a group with multiple active admins, plus users that should not
	// receive alerts because they duplicate another email, have no email, have
	// the wrong role, or have a pending status.
	require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_group").Create(&tables.Group{GroupID: "GR1", Name: "Acme Corp", URLIdentifier: "acme"}))
	for _, row := range []*tables.User{
		{UserID: "UA1", FirstName: "Admin", LastName: "One", Email: " admin@example.com "},
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

	// Only active admins with unique, non-empty email addresses in the rule's
	// group should be attached as recipients.
	require.Len(t, rules, 1)
	assert.Equal(t, "UR1", rules[0].rule.UsageAlertingRuleID)
	assert.Equal(t, "Acme Corp", rules[0].groupName)
	assert.Equal(t, "acme", rules[0].groupURLIdentifier)
	assert.Equal(t, []email.Address{
		{Name: "Admin One", Email: "admin@example.com"},
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

	// Only rules with deliverable admin recipients should be evaluated.
	require.Len(t, rules, 1)
	assert.Equal(t, "UR1", rules[0].rule.UsageAlertingRuleID)
}

func TestMetricDefinitionsCoverEveryAlertingMetric(t *testing.T) {
	for _, metric := range alertingMetricValues() {
		// Every alerting metric users can select should have email display
		// metadata.
		_, ok := metricDefinitions[metric]
		assert.True(t, ok, "missing metric definition for %s", metric)
	}
	assert.Len(t, metricDefinitions, len(alertingMetricValues()))
}

func TestPushGatewayGathererIncludesCommonAndEvaluatorMetrics(t *testing.T) {
	metrics := testEvaluatorMetrics(t)
	metrics.errors.WithLabelValues(errorStageQueryUsage).Inc()

	names := gathererMetricNames(t, newPushGatewayGatherer(metrics))

	assert.Contains(t, names, "go_goroutines")
	assert.Contains(t, names, "buildbuddy_usage_alert_errors_total")
}

func TestWindowRange(t *testing.T) {
	// Wednesday, May 13, 2026 should fall in the UTC week that starts on Monday,
	// beginning May 11 and ending May 18.
	now := time.Date(2026, 5, 13, 15, 30, 0, 0, time.UTC)

	start, end, err := windowRange(now, usagepb.UsageAlertingWindow_WEEK)
	require.NoError(t, err)

	// The evaluator includes the start and excludes the end, so the end is the
	// first instant after the active week.
	assert.Equal(t, time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, time.Date(2026, 5, 18, 0, 0, 0, 0, time.UTC), end)
}

func newTestEvaluator(t testing.TB, now time.Time, sender emailSender, metrics *evaluatorMetrics) (*Evaluator, interfaces.DBHandle, interfaces.OLAPDBHandle) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)
	te := testenv.GetTestEnv(t)
	return NewEvaluator(te.GetDBHandle(), te.GetOLAPDBHandle(), sender, clockwork.NewFakeClockAt(now), metrics), te.GetDBHandle(), te.GetOLAPDBHandle()
}

func newTestEvaluatorWithUsage(t testing.TB, now time.Time, sender emailSender, metrics *evaluatorMetrics, usage usageReader) (*Evaluator, interfaces.DBHandle) {
	t.Helper()
	te := testenv.GetTestEnv(t)
	evaluator := NewEvaluator(te.GetDBHandle(), nil, sender, clockwork.NewFakeClockAt(now), metrics)
	evaluator.usage = usage
	return evaluator, te.GetDBHandle()
}

func testEvaluatorMetrics(t testing.TB) *evaluatorMetrics {
	metrics, err := newEvaluatorMetrics()
	require.NoError(t, err)
	return metrics
}

func gathererMetricNames(t testing.TB, gatherer prometheus.Gatherer) map[string]struct{} {
	t.Helper()
	metricFamilies, err := gatherer.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(metricFamilies))
	for _, metricFamily := range metricFamilies {
		names[metricFamily.GetName()] = struct{}{}
	}
	return names
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
	createUserAndMembership(t, ctx, dbh, groupID, userID, firstName, lastName, emailAddress, uint32(role.Admin), int32(grpb.GroupMembershipStatus_MEMBER))
}

func createUserAndMembership(t testing.TB, ctx context.Context, dbh interfaces.DBHandle, groupID, userID, firstName, lastName, emailAddress string, userRole uint32, membershipStatus int32) {
	require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_user").Create(&tables.User{
		UserID:    userID,
		FirstName: firstName,
		LastName:  lastName,
		Email:     emailAddress,
	}))
	require.NoError(t, dbh.NewQuery(ctx, "usagealert_test_create_user_group").Create(&tables.UserGroup{
		UserUserID:       userID,
		GroupGroupID:     groupID,
		Role:             userRole,
		MembershipStatus: membershipStatus,
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

type fakeUsageReader struct {
	values map[usagepb.UsageAlertingMetric_Value]int64
	errors map[usagepb.UsageAlertingMetric_Value]error
}

func (r *fakeUsageReader) QueryUsage(ctx context.Context, field *usage_service.UsageField, groupID string, start, end time.Time) (int64, error) {
	if err := r.errors[field.AlertingMetric]; err != nil {
		return 0, err
	}
	return r.values[field.AlertingMetric], nil
}
