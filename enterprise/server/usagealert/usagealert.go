// Usage alerting rule evaluator and notifier.
//
// This should run as a cronjob. It queries usage alerting rules and recipients,
// then compares each rule's configured threshold against the current usage data
// in ClickHouse. For any alerts that haven't already fired for the current
// window, it sends email alerts to all org admins.
//
// Example run:
//
//	bb run -- //enterprise/server/usagealert:usagealert \
//	  --app.build_buddy_url=https://app.buildbuddy.example.com \
//	  --app.enable_subdomain_matching=true \
//	  --database.data_source='mysql://user:password@tcp(mysql:3306)/buildbuddy' \
//	  --olap_database.data_source='clickhouse://default:password@clickhouse:9000/buildbuddy' \
//	  --notifications.email.sendgrid.api_key=$SENDGRID_API_KEY \
//	  --notifications.email.default_from_address='{"name":"BuildBuddy","email":"notifications@buildbuddy.example.com"}' \
//	  --usagealert.push_gateway=http://pushgateway.example.com:9091
//
// Usage alert metrics and the common default metrics are pushed to a configured
// prometheus push gateway.
package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"math"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/email"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/jonboulle/clockwork"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

const (
	errorStageQueryRulesAndRecipients = "query_rules_and_recipients"
	errorStageMissingMetric           = "missing_metric"
	errorStageUnsupportedWindow       = "unsupported_window"
	errorStageQueryUsage              = "query_usage"
	errorStageSendEmail               = "send_email"
	errorStageUpdateRule              = "update_rule"
)

var (
	evaluatorConcurrency = flag.Int("usagealert.concurrency", 4, "Maximum number of usage alerting rules to evaluate concurrently.")
	pushGateway          = flag.String("usagealert.push_gateway", "", "Prometheus Pushgateway address used to publish evaluator error metrics.")

	alertEmailBodyTemplate = sync.OnceValues(func() (*template.Template, error) {
		return template.New("usage_alert_email").Parse(`<p><strong>{{.WindowLabel}} {{.MetricDisplay}}</strong> usage alert fired for organization <strong>{{.GroupDisplayName}}</strong>.</p>
<p>Usage for <strong>{{.MetricDisplay}}</strong> is <strong>{{.Value}}</strong>, which exceeds the configured threshold of <strong>{{.Threshold}}</strong>.</p>
<p>Window: {{.Start}} to {{.End}} UTC</p>
<p>See usage at <a href="{{.UsageURL}}">{{.UsageURL}}</a></p>`)
	})
)

func main() {
	if err := disableAutoMigration(); err != nil {
		log.Fatalf("disable auto-migration: %s", err)
	}
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := configsecrets.Configure(); err != nil {
		return fmt.Errorf("prepare config secrets provider: %w", err)
	}
	if err := config.Load(); err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := log.Configure(); err != nil {
		return fmt.Errorf("configure log: %w", err)
	}
	if !email.IsConfigured() {
		return fmt.Errorf("email is not configured")
	}

	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("usagealert"))
	dbh, err := db.GetConfiguredDatabase(ctx, env)
	if err != nil {
		return fmt.Errorf("configure SQL database: %w", err)
	}
	env.SetDBHandle(dbh)
	if err := clickhouse.Register(env); err != nil {
		return fmt.Errorf("configure ClickHouse: %w", err)
	}
	if env.GetOLAPDBHandle() == nil {
		return fmt.Errorf("clickhouse database is required")
	}

	metrics, err := newEvaluatorMetrics()
	if err != nil {
		return fmt.Errorf("configure metrics: %w", err)
	}
	evaluator := NewEvaluator(
		env.GetDBHandle(),
		env.GetOLAPDBHandle(),
		email.NewClient(email.ClientConfig{}),
		clockwork.NewRealClock(),
		metrics,
	)
	pusher := &pushGatewayPusher{gatherer: newPushGatewayGatherer(metrics)}
	result, err := runEvaluator(ctx, evaluator, pusher)
	if result != nil {
		log.Infof("Evaluated %d usage alerting rules and sent %d alert emails.", result.RulesEvaluated, result.AlertsSent)
	}
	return err
}

// disableAutoMigration disables schema migration for the evaluator process.
func disableAutoMigration() error {
	if err := flagutil.SetValueForFlagName("auto_migrate_db", false, nil, false); err != nil {
		return err
	}
	if err := flagutil.SetValueForFlagName("olap_database.auto_migrate_db", false, nil, false); err != nil {
		return err
	}
	return nil
}

// emailSender sends alert emails.
type emailSender interface {
	Send(ctx context.Context, msg *email.Message) error
}

// metricsPusher publishes evaluator metrics after a run.
type metricsPusher interface {
	Push() error
}

// usageReader queries aggregate usage values for alert evaluation.
type usageReader interface {
	QueryUsage(ctx context.Context, field *usage_service.UsageField, groupID string, start, end time.Time) (int64, error)
}

// runEvaluator runs the evaluator and then pushes metrics even if evaluation fails.
func runEvaluator(ctx context.Context, evaluator *Evaluator, pusher metricsPusher) (*EvaluationResult, error) {
	result, evalErr := evaluator.Run(ctx)
	pushErr := pusher.Push()
	if pushErr != nil {
		pushErr = fmt.Errorf("push evaluator metrics: %w", pushErr)
	}
	return result, errors.Join(evalErr, pushErr)
}

// Evaluator checks usage alerting rules and emails admins for newly fired alerts.
type Evaluator struct {
	dbh     interfaces.DBHandle
	usage   usageReader
	email   emailSender
	clock   clockwork.Clock
	metrics *evaluatorMetrics
}

// EvaluationResult summarizes an evaluator run.
type EvaluationResult struct {
	// RulesEvaluated is the number of attempted alert rules.
	RulesEvaluated int
	// AlertsSent is the number of alert emails sent.
	AlertsSent int
}

// alertRule pairs a rule row with its current admin recipients.
type alertRule struct {
	rule               *tables.UsageAlertingRule
	groupName          string
	groupURLIdentifier string
	recipients         []email.Address
	recipientEmails    map[string]struct{}
}

// NewEvaluator constructs an evaluator using explicit dependencies for tests.
func NewEvaluator(dbh interfaces.DBHandle, olapDBH interfaces.OLAPDBHandle, email emailSender, clock clockwork.Clock, metrics *evaluatorMetrics) *Evaluator {
	return &Evaluator{
		dbh:     dbh,
		usage:   &usageQuerier{dbh: olapDBH},
		email:   email,
		clock:   clock,
		metrics: metrics,
	}
}

// Run evaluates alert rules in the database.
func (e *Evaluator) Run(ctx context.Context) (*EvaluationResult, error) {
	rules, err := e.queryRulesAndRecipients(ctx)
	if err != nil {
		e.recordError(errorStageQueryRulesAndRecipients)
		return nil, fmt.Errorf("query usage alerting rules and recipients: %w", err)
	}

	now := e.clock.Now().UTC()
	result := &EvaluationResult{RulesEvaluated: len(rules)}
	if len(rules) == 0 {
		return result, nil
	}

	concurrency := min(*evaluatorConcurrency, len(rules))
	if concurrency < 1 {
		return nil, fmt.Errorf("evaluator concurrency must be >= 1")
	}

	jobs := make(chan *alertRule)
	results := make(chan ruleEvaluationResult, len(rules))
	var wg sync.WaitGroup
	for range concurrency {
		wg.Go(func() {
			for rule := range jobs {
				alertsSent, err := e.evaluateRule(ctx, now, rule)
				results <- ruleEvaluationResult{alertsSent: alertsSent, err: err}
			}
		})
	}
	for _, rule := range rules {
		jobs <- rule
	}
	close(jobs)
	wg.Wait()
	close(results)

	var errs []error
	for r := range results {
		result.AlertsSent += r.alertsSent
		if r.err != nil {
			errs = append(errs, r.err)
		}
	}
	return result, errors.Join(errs...)
}

// ruleEvaluationResult contains the outcome of evaluating an alert rule.
type ruleEvaluationResult struct {
	alertsSent int
	err        error
}

// evaluateRule evaluates an alert rule and sends emails if it newly fires.
func (e *Evaluator) evaluateRule(ctx context.Context, now time.Time, alert *alertRule) (int, error) {
	rule := alert.rule
	// Resolve the user-selected metric into the email metadata and Usage API
	// field that should be queried.
	metric, ok := metricDefinitions[rule.UsageAlertingMetric]
	if !ok {
		e.recordError(errorStageMissingMetric)
		return 0, fmt.Errorf("rule %q has unsupported metric %s", rule.UsageAlertingRuleID, rule.UsageAlertingMetric)
	}
	usageField, ok := usage_service.UsageFieldForAlertingMetric(rule.UsageAlertingMetric)
	if !ok {
		e.recordError(errorStageMissingMetric)
		return 0, fmt.Errorf("rule %q has unsupported metric %s", rule.UsageAlertingRuleID, rule.UsageAlertingMetric)
	}
	// Evaluate each rule against the complete current day, week, or month
	// window containing now.
	start, end, err := windowRange(now, rule.Window)
	if err != nil {
		e.recordError(errorStageUnsupportedWindow)
		return 0, fmt.Errorf("rule %q has unsupported window %s", rule.UsageAlertingRuleID, rule.Window)
	}

	// Query the aggregate usage value for the rule's group and window.
	value, err := e.usage.QueryUsage(ctx, usageField, rule.GroupID, start, end)
	if err != nil {
		e.recordError(errorStageQueryUsage)
		return 0, fmt.Errorf("query usage for rule %q: %w", rule.UsageAlertingRuleID, err)
	}

	// Fire only when usage exceeds the threshold and this rule has not already
	// fired in the current window.
	if value <= rule.AbsoluteThreshold || usecInRange(rule.LastFiredUsec, start, end) {
		return 0, nil
	}

	msg, err := buildAlertEmail(alert, metric, alert.recipients, value, start, end)
	if err != nil {
		e.recordError(errorStageSendEmail)
		return 0, fmt.Errorf("build usage alert email for rule %q: %w", rule.UsageAlertingRuleID, err)
	}
	// Send before recording the fired timestamp so we do not miss alerts if
	// this process exits before delivery. Kubernetes CronJobs can overlap (even
	// with concurrencyPolicy `Forbid`), so this can send duplicate emails
	// before one run records that the rule fired.
	//
	// TODO: Maybe have a separate email outbox table so delivery can be retried
	// without this duplicate-send tradeoff, or use an email provider that
	// supports idempotent delivery.
	if err := e.email.Send(ctx, msg); err != nil {
		e.recordError(errorStageSendEmail)
		return 0, fmt.Errorf("send usage alert email for rule %q: %w", rule.UsageAlertingRuleID, err)
	}
	// After the email send succeeds, remember that this rule fired for the
	// window so future runs can skip it.
	lastFiredUsec := now.UnixMicro()
	recorded, err := e.recordRuleFired(ctx, rule.UsageAlertingRuleID, lastFiredUsec, start, end)
	if err != nil {
		e.recordError(errorStageUpdateRule)
		return len(alert.recipients), fmt.Errorf("record usage alerting rule %q fired: %w", rule.UsageAlertingRuleID, err)
	}
	if !recorded {
		return len(alert.recipients), nil
	}
	return len(alert.recipients), nil
}

// recordError increments the evaluator error counter for stage.
func (e *Evaluator) recordError(stage string) {
	if e.metrics != nil {
		e.metrics.errors.WithLabelValues(stage).Inc()
	}
}

// queryRulesAndRecipients loads alert rules and their current admin recipients.
func (e *Evaluator) queryRulesAndRecipients(ctx context.Context) ([]*alertRule, error) {
	// Query rules + recipients together so we only evaluate deliverable rules.
	type ruleRecipientRow struct {
		tables.UsageAlertingRule
		GroupName          string
		GroupURLIdentifier string
		RecipientEmail     string
		RecipientFirstName string
		RecipientLastName  string
	}
	rq := e.dbh.NewQuery(ctx, "usage_alert_query_rules_and_recipients").Raw(`
		SELECT r.*,
		COALESCE(g.name, '') AS group_name,
		COALESCE(g.url_identifier, '') AS group_url_identifier,
		u.email AS recipient_email,
		u.first_name AS recipient_first_name,
		u.last_name AS recipient_last_name
		FROM "UsageAlertingRules" AS r
		LEFT JOIN "Groups" AS g ON g.group_id = r.group_id
		JOIN "UserGroups" AS ug ON ug.group_group_id = r.group_id
		JOIN "Users" AS u ON u.user_id = ug.user_user_id
		WHERE ug.membership_status = ?
		AND (ug.role & ?) = ?
		AND TRIM(u.email) <> ''
		ORDER BY r.group_id ASC, r.usage_alerting_rule_id ASC, u.user_id ASC
	`, int32(grpb.GroupMembershipStatus_MEMBER), uint32(role.Admin), uint32(role.Admin))
	rows, err := db.ScanAll(rq, &ruleRecipientRow{})
	if err != nil {
		return nil, err
	}

	var rules []*alertRule
	byID := make(map[string]*alertRule)
	for _, row := range rows {
		rule := byID[row.UsageAlertingRuleID]
		if rule == nil {
			r := row.UsageAlertingRule
			rule = &alertRule{
				rule:               &r,
				groupName:          row.GroupName,
				groupURLIdentifier: row.GroupURLIdentifier,
				recipientEmails:    make(map[string]struct{}),
			}
			byID[row.UsageAlertingRuleID] = rule
			rules = append(rules, rule)
		}
		recipientEmail := strings.TrimSpace(row.RecipientEmail)
		recipientKey := strings.ToLower(recipientEmail)
		if _, ok := rule.recipientEmails[recipientKey]; ok {
			continue
		}
		rule.recipientEmails[recipientKey] = struct{}{}
		rule.recipients = append(rule.recipients, email.Address{
			Name:  strings.TrimSpace(row.RecipientFirstName + " " + row.RecipientLastName),
			Email: recipientEmail,
		})
	}
	return rules, nil
}

// recordRuleFired records that an alert rule fired if it has not already fired
// in the evaluated window.
func (e *Evaluator) recordRuleFired(ctx context.Context, ruleID string, lastFiredUsec int64, start, end time.Time) (bool, error) {
	result := e.dbh.NewQuery(ctx, "usage_alert_record_fired_rule").Raw(`
		UPDATE "UsageAlertingRules"
		SET last_fired_usec = ?
		WHERE usage_alerting_rule_id = ?
		AND (
			last_fired_usec = 0 OR
			last_fired_usec < ? OR
			last_fired_usec >= ?
		)
	`, lastFiredUsec, ruleID, start.UnixMicro(), end.UnixMicro()).Exec()
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// usageQuerier queries usage rows from ClickHouse.
type usageQuerier struct {
	dbh interfaces.OLAPDBHandle
}

// QueryUsage returns aggregate usage for the field and time window.
func (q *usageQuerier) QueryUsage(ctx context.Context, field *usage_service.UsageField, groupID string, start, end time.Time) (int64, error) {
	row := &struct {
		Value int64
	}{}
	if err := q.dbh.NewQuery(ctx, "usage_alert_query_raw_usage").Raw(`
		SELECT ifNull(`+field.OLAPExpression+`, 0) AS value
		FROM RawUsage FINAL
		WHERE group_id = ?
		AND period_start >= ?
		AND period_start < ?
	`, groupID, start, end).Take(row); err != nil {
		return 0, err
	}
	return row.Value, nil
}

// metricDefinition maps an alerting metric to email display metadata.
type metricDefinition struct {
	// Metric is the user-facing alerting metric enum value.
	Metric usagepb.UsageAlertingMetric_Value
	// Display is the label used in alert emails.
	Display string
	// Unit is the usage unit used by the UI for thresholds and display.
	Unit metricUnit
}

// metricUnit mirrors the unit names used by usage_alerts_model.ts.
type metricUnit string

const (
	countMetricUnit         metricUnit = "count"
	bytesMetricUnit         metricUnit = "bytes"
	durationUsecMetricUnit  metricUnit = "duration_usec"
	durationNanosMetricUnit metricUnit = "duration_nanos"
)

// metricDefinitions contains evaluator mappings for alerting metrics.
// TODO: define these as protos and share with the UI.
var metricDefinitions = map[usagepb.UsageAlertingMetric_Value]*metricDefinition{
	usagepb.UsageAlertingMetric_INVOCATIONS: {
		Metric:  usagepb.UsageAlertingMetric_INVOCATIONS,
		Display: "Invocations",
		Unit:    countMetricUnit,
	},
	usagepb.UsageAlertingMetric_ACTION_CACHE_HITS: {
		Metric:  usagepb.UsageAlertingMetric_ACTION_CACHE_HITS,
		Display: "Action cache hits",
		Unit:    countMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_CACHED_ACTION_EXEC_USEC: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_CACHED_ACTION_EXEC_USEC,
		Display: "Cached build minutes",
		Unit:    durationUsecMetricUnit,
	},
	usagepb.UsageAlertingMetric_CAS_CACHE_HITS: {
		Metric:  usagepb.UsageAlertingMetric_CAS_CACHE_HITS,
		Display: "Content addressable storage cache hits",
		Unit:    countMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_DOWNLOAD_SIZE_BYTES,
		Display: "Total bytes downloaded from cache (All)",
		Unit:    bytesMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_EXTERNAL_DOWNLOAD_SIZE_BYTES: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_EXTERNAL_DOWNLOAD_SIZE_BYTES,
		Display: "Total bytes downloaded from cache (External)",
		Unit:    bytesMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_INTERNAL_DOWNLOAD_SIZE_BYTES: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_INTERNAL_DOWNLOAD_SIZE_BYTES,
		Display: "Total bytes downloaded from cache (Internal)",
		Unit:    bytesMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_WORKFLOW_DOWNLOAD_SIZE_BYTES: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_WORKFLOW_DOWNLOAD_SIZE_BYTES,
		Display: "Total bytes downloaded from cache (Workflows)",
		Unit:    bytesMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_UPLOAD_SIZE_BYTES,
		Display: "Total bytes uploaded to cache (All)",
		Unit:    bytesMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_EXTERNAL_UPLOAD_SIZE_BYTES: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_EXTERNAL_UPLOAD_SIZE_BYTES,
		Display: "Total bytes uploaded to cache (External)",
		Unit:    bytesMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_INTERNAL_UPLOAD_SIZE_BYTES: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_INTERNAL_UPLOAD_SIZE_BYTES,
		Display: "Total bytes uploaded to cache (Internal)",
		Unit:    bytesMetricUnit,
	},
	usagepb.UsageAlertingMetric_TOTAL_WORKFLOW_UPLOAD_SIZE_BYTES: {
		Metric:  usagepb.UsageAlertingMetric_TOTAL_WORKFLOW_UPLOAD_SIZE_BYTES,
		Display: "Total bytes uploaded to cache (Workflows)",
		Unit:    bytesMetricUnit,
	},
	usagepb.UsageAlertingMetric_LINUX_EXECUTION_DURATION_USEC: {
		Metric:  usagepb.UsageAlertingMetric_LINUX_EXECUTION_DURATION_USEC,
		Display: "Linux remote execution duration (All)",
		Unit:    durationUsecMetricUnit,
	},
	usagepb.UsageAlertingMetric_CLOUD_RBE_LINUX_EXECUTION_DURATION_USEC: {
		Metric:  usagepb.UsageAlertingMetric_CLOUD_RBE_LINUX_EXECUTION_DURATION_USEC,
		Display: "Linux remote execution duration (Cloud RBE)",
		Unit:    durationUsecMetricUnit,
	},
	usagepb.UsageAlertingMetric_CLOUD_WORKFLOW_LINUX_EXECUTION_DURATION_USEC: {
		Metric:  usagepb.UsageAlertingMetric_CLOUD_WORKFLOW_LINUX_EXECUTION_DURATION_USEC,
		Display: "Linux remote execution duration (Workflows)",
		Unit:    durationUsecMetricUnit,
	},
	usagepb.UsageAlertingMetric_CLOUD_CPU_NANOS: {
		Metric:  usagepb.UsageAlertingMetric_CLOUD_CPU_NANOS,
		Display: "Linux CPU duration (All)",
		Unit:    durationNanosMetricUnit,
	},
	usagepb.UsageAlertingMetric_CLOUD_RBE_CPU_NANOS: {
		Metric:  usagepb.UsageAlertingMetric_CLOUD_RBE_CPU_NANOS,
		Display: "Linux CPU duration (Cloud RBE)",
		Unit:    durationNanosMetricUnit,
	},
	usagepb.UsageAlertingMetric_CLOUD_WORKFLOW_CPU_NANOS: {
		Metric:  usagepb.UsageAlertingMetric_CLOUD_WORKFLOW_CPU_NANOS,
		Display: "Linux CPU duration (Workflows)",
		Unit:    durationNanosMetricUnit,
	},
}

// windowRange returns the UTC bounds for an alerting window containing now.
func windowRange(now time.Time, window usagepb.UsageAlertingWindow_Value) (time.Time, time.Time, error) {
	now = now.UTC()
	switch window {
	case usagepb.UsageAlertingWindow_DAY:
		start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 0, 1), nil
	case usagepb.UsageAlertingWindow_WEEK:
		weekdayOffset := (int(now.Weekday()) + 6) % 7
		start := time.Date(now.Year(), now.Month(), now.Day()-weekdayOffset, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 0, 7), nil
	case usagepb.UsageAlertingWindow_MONTH:
		start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, 0), nil
	default:
		return time.Time{}, time.Time{}, status.InvalidArgumentErrorf("unsupported usage alerting window: %s", window)
	}
}

// usecInRange reports whether a microsecond timestamp is inside [start, end).
func usecInRange(usec int64, start, end time.Time) bool {
	if usec == 0 {
		return false
	}
	t := time.UnixMicro(usec).UTC()
	return !t.Before(start) && t.Before(end)
}

// buildAlertEmail builds the HTML notification for a fired alert.
func buildAlertEmail(alert *alertRule, metric *metricDefinition, recipients []email.Address, value int64, start, end time.Time) (*email.Message, error) {
	if len(recipients) == 0 {
		return nil, status.InvalidArgumentError("at least one recipient is required")
	}
	rule := alert.rule
	// Group display name: prefer group name, slug, then group ID.
	groupDisplayName := strings.TrimSpace(alert.groupName)
	if groupDisplayName == "" {
		groupDisplayName = alert.groupURLIdentifier
	}
	if groupDisplayName == "" {
		groupDisplayName = rule.GroupID
	}
	usageURL := build_buddy_url.WithPath("/usage").String()
	if alert.groupURLIdentifier != "" && subdomain.Enabled() {
		usageURL = subdomain.ReplaceURLSubdomainForGroup(usageURL, &tables.Group{URLIdentifier: alert.groupURLIdentifier})
	}
	subject := fmt.Sprintf("[BuildBuddy Usage Alert] [%s] %s %s threshold reached", groupDisplayName, usageAlertingWindowLabel(rule.Window), metric.Display)
	bodyTemplate, err := alertEmailBodyTemplate()
	if err != nil {
		return nil, fmt.Errorf("load email body template: %w", err)
	}
	var body strings.Builder
	if err := bodyTemplate.Execute(&body, struct {
		WindowLabel      string
		MetricDisplay    string
		GroupDisplayName string
		Value            string
		Threshold        string
		Start            string
		End              string
		UsageURL         string
	}{
		WindowLabel:      usageAlertingWindowLabel(rule.Window),
		MetricDisplay:    metric.Display,
		GroupDisplayName: groupDisplayName,
		Value:            formatMetricValue(metric, value),
		Threshold:        formatMetricValue(metric, rule.AbsoluteThreshold),
		Start:            start.Format(time.RFC3339),
		End:              end.Format(time.RFC3339),
		UsageURL:         usageURL,
	}); err != nil {
		return nil, fmt.Errorf("render email body template: %w", err)
	}
	return &email.Message{
		ToAddresses: recipients,
		Subject:     subject,
		Body:        body.String(),
	}, nil
}

// usageAlertingWindowLabel returns the user-facing label for an alerting window.
func usageAlertingWindowLabel(window usagepb.UsageAlertingWindow_Value) string {
	switch window {
	case usagepb.UsageAlertingWindow_DAY:
		return "Daily"
	case usagepb.UsageAlertingWindow_WEEK:
		return "Weekly"
	case usagepb.UsageAlertingWindow_MONTH:
		return "Monthly"
	default:
		return ""
	}
}

// formatMetricValue formats usage values using the same unit categories as the UI.
func formatMetricValue(metric *metricDefinition, value int64) string {
	switch metric.Unit {
	case bytesMetricUnit:
		return formatBytes(value)
	case durationUsecMetricUnit:
		minutes := int64(math.Round(float64(value) / 60e6))
		return formatCount(minutes) + " " + pluralize("minute", minutes)
	case durationNanosMetricUnit:
		minutes := int64(math.Round(float64(value) / 60e9))
		return formatCount(minutes) + " " + pluralize("minute", minutes)
	case countMetricUnit:
		return formatCount(value)
	default:
		return formatCount(value)
	}
}

// formatBytes formats decimal byte units to match the usage alerting UI.
func formatBytes(value int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	v := float64(value)
	for i, unit := range units {
		if v < math.Pow(1000, float64(i+1)) || i == len(units)-1 {
			return strconv.FormatFloat(v/math.Pow(1000, float64(i)), 'g', 4, 64) + unit
		}
	}
	return strconv.FormatInt(value, 10) + "B"
}

// formatCount formats compact counts to match the usage alerting UI.
func formatCount(value int64) string {
	if value < 1000 {
		return strconv.FormatInt(value, 10)
	}
	if value < 1e6 {
		return strconv.FormatFloat(float64(value)/1e3, 'g', 4, 64) + "K"
	}
	if value < 1e9 {
		return strconv.FormatFloat(float64(value)/1e6, 'g', 4, 64) + "M"
	}
	return strconv.FormatFloat(float64(value)/1e9, 'g', 4, 64) + "B"
}

// pluralize returns singular or plural text for count.
func pluralize(singular string, count int64) string {
	if count == 1 {
		return singular
	}
	return singular + "s"
}

// evaluatorMetrics owns evaluator-specific metrics.
type evaluatorMetrics struct {
	registry *prometheus.Registry
	errors   *prometheus.CounterVec
}

// newEvaluatorMetrics registers metrics used by the evaluator.
func newEvaluatorMetrics() (*evaluatorMetrics, error) {
	registry := prometheus.NewRegistry()
	errors := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "buildbuddy_usage_alert_errors_total",
		Help: "Number of usage alert errors.",
	}, []string{"stage"})
	if err := registry.Register(errors); err != nil {
		return nil, fmt.Errorf("register usage alert errors metric: %w", err)
	}
	return &evaluatorMetrics{
		registry: registry,
		errors:   errors,
	}, nil
}

// newPushGatewayGatherer combines common process metrics with evaluator metrics.
func newPushGatewayGatherer(metrics *evaluatorMetrics) prometheus.Gatherer {
	if metrics == nil {
		return prometheus.DefaultGatherer
	}
	return prometheus.Gatherers{prometheus.DefaultGatherer, metrics.registry}
}

// pushGatewayPusher pushes metrics to Prometheus Pushgateway when configured.
type pushGatewayPusher struct {
	gatherer prometheus.Gatherer
}

// Push publishes metrics or no-ops when no Pushgateway address is set.
func (p *pushGatewayPusher) Push() error {
	address := strings.TrimSpace(*pushGateway)
	if address == "" {
		return nil
	}
	return push.New(address, "usagealert").Gatherer(p.gatherer).Push()
}

// alertingMetricValues returns non-UNKNOWN alerting metric enum values.
func alertingMetricValues() []usagepb.UsageAlertingMetric_Value {
	values := make([]usagepb.UsageAlertingMetric_Value, 0, len(usagepb.UsageAlertingMetric_Value_name))
	for value := range usagepb.UsageAlertingMetric_Value_name {
		metric := usagepb.UsageAlertingMetric_Value(value)
		if metric == usagepb.UsageAlertingMetric_UNKNOWN {
			continue
		}
		values = append(values, metric)
	}
	slices.Sort(values)
	return values
}
