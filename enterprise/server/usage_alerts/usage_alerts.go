package usage_alerts

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/notifications"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/go-redis/redis/v8"
	"github.com/jonboulle/clockwork"

	e "github.com/buildbuddy-io/buildbuddy/enterprise/server/notifications/email"
	nfpb "github.com/buildbuddy-io/buildbuddy/proto/notification"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

var (
	usageAlertWorkers  = flag.Int("usage.alerts.workers", 2, "Number of usage alert evaluator workers to run concurrently.")
	evaluationInterval = flag.Duration("usage.alerts.evaluation_interval", 1*time.Minute, "How often to evaluate usage alerting rules.")
)

const (
	// Redis key prefix for caching usage queries.
	redisAlertingPeriodUsageCacheKeyPrefix = "usage/alerting_period_usage/"

	redisSchedulerQueueKey  = "usage/alerting/evaluation/queue"
	redisSchedulerTickerKey = "usage/alerting/evaluation/ticker"
)

type NotificationService interface {
	// Create creates a group notification within the given DB transaction,
	// enqueueing it to be delivered.
	//
	// The group ID and content ID should uniquely identify a notification
	// within a group, forming an "idempotency key". A notification will not be
	// enqueued for delivery if a notification with the same idempotency key has
	// already been enqueued.
	Create(ctx context.Context, tx notifications.TX, groupID, contentID string, n *nfpb.Notification) (bool, error)
}

// Evaluator fulfills the alerting schedule established by the alerting rules in
// the DB.
//
// It periodically evaluates rules according to their schedule, and if the
// threshold is exceeded, it triggers an alert if the alert threshold is
// exceeded.
type Evaluator struct {
	clock               clockwork.Clock
	dbh                 interfaces.DBHandle
	rdb                 redis.UniversalClient
	notificationService NotificationService

	q        *redisutil.UnreliableWorkQueue
	shutdown chan struct{}
	done     chan struct{}
}

func NewEvaluator(ctx context.Context, clock clockwork.Clock, dbh interfaces.DBHandle, rdb redis.UniversalClient, notificationService NotificationService) (*Evaluator, error) {
	q, err := redisutil.NewUnreliableWorkQueue(ctx, rdb, redisSchedulerQueueKey)
	if err != nil {
		return nil, status.WrapError(err, "initialize work queue")
	}
	return &Evaluator{
		clock:               clock,
		dbh:                 dbh,
		rdb:                 rdb,
		notificationService: notificationService,
		q:                   q,
		shutdown:            make(chan struct{}),
		done:                make(chan struct{}),
	}, nil
}

// Start starts the evaluator.
// Cancelling the given context cancels any in-progress work, which can
// result in duplicate notifications being delivered if the backend does not
// support idempotency. So it is preferable to call Shutdown as soon as the
// server starts shutting down, to allow any ongoing work to be completed.
func (ev *Evaluator) Start(ctx context.Context) {
	go func() {
		defer close(ev.done)
		ev.run(ctx)
	}()
}

// Shutdown stops any new alerts from being evaluated and waits for
// any in-flight work to be completed.
func (ev *Evaluator) Shutdown() {
	close(ev.shutdown)
	<-ev.done
	log.Debugf("Stopped usage alert evaluation workers")
}

func (ev *Evaluator) run(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()

	ch := make(chan string)
	go func() {
		defer close(ch)
		// Read work from the stream and send it to workers.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			<-ev.shutdown
			cancel()
		}()
		for {
			ruleID, err := ev.q.Dequeue(ctx)
			if err != nil {
				log.CtxWarningf(ctx, "Failed to dequeue from redis stream: %s", err)
				// There's not much we can do in this case since the dequeue op
				// handles non-fatal errors by retrying.
				// Just sleep a little bit and retry in case we can manually
				// recover.
				select {
				case <-ev.clock.After(1 * time.Minute):
				case <-ctx.Done():
					return
				}
				continue
			}
			ch <- ruleID
		}
	}()

	// Start up some workers to evaluate alerts.
	for range *usageAlertWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev.runEvalWorker(ctx, ch)
		}()
	}

	// TODO: this could be more efficient and also more responsive if we had a
	// way to reliably notify all of the apps when alerting rules change.
	for {
		// Wait for the ticker to expire.
		ttl, err := ev.rdb.TTL(ctx, redisSchedulerTickerKey).Result()
		if err != nil {
			ttl = *evaluationInterval
			log.CtxWarningf(ctx, "Failed to read next scheduling tick: %s", err)
		}
		select {
		case <-ev.shutdown:
			return
		case <-ctx.Done():
			return
		case <-ev.clock.After(ttl):
		}
		// Set the ticker key if it does not exist.
		// If it didn't exist, we should handle scheduling for this tick.
		ok, err := ev.rdb.SetNX(ctx, redisSchedulerTickerKey, "", *evaluationInterval).Result()
		if !ok {
			continue
		}
		if err != nil {
			// Just log if there's an error so we can still make progress
			// if SetNX is failing because redis is temporarily unavailable.
			log.CtxWarningf(ctx, "Failed to set next scheduling tick: %s", err)
		}
		if err := ev.scheduleWork(ctx); err != nil {
			log.CtxWarningf(ctx, "Work scheduling failed: %s", err)
		}
	}
}

func (ev *Evaluator) scheduleWork(ctx context.Context) error {
	log.CtxDebugf(ctx, "Scheduling alerting rule evaluation work")
	rules := ev.getRulesToEval(ctx)
	for _, r := range rules {
		err := ev.q.Enqueue(ctx, r.UsageAlertingRuleID, float64(time.Now().UnixMicro()))
		if err != nil {
			return status.WrapError(err, "schedule alerting rule evaluation")
		}
	}
	return nil
}

func (ev *Evaluator) runEvalWorker(ctx context.Context, ch chan string) {
	for ruleID := range ch {
		if err := ev.evalRule(ctx, ruleID); err != nil {
			log.CtxWarningf(ctx, "Failed to evaluate alerting rule: %s", err)
		}
	}
}

func (ev *Evaluator) getRulesToEval(ctx context.Context) []*tables.UsageAlertingRule {
	// TODO: make timezone configurable. For now, we align rule evaluation
	// periods to billing days, which are in UTC.

	// TODO: apply tighter predicates here to avoid selecting rules that have
	// already been triggered during their current alerting period.

	q := ev.dbh.NewQuery(ctx, "usage_alerts_get_rules_to_eval").Raw(`
		SELECT *
		FROM "UsageAlertingRules"
		`,
		ev.clock.Now().UnixMicro(),
	)
	rules, err := db.ScanAll[tables.UsageAlertingRule](q, nil)
	if err != nil {
		log.CtxErrorf(ctx, "Failed to scan usage alerting rules: %s", err)
	}
	return rules
}

func (ev *Evaluator) evalRule(ctx context.Context, usageAlertingRuleID string) error {
	log.CtxDebugf(ctx, "Worker evaluating rule %s", usageAlertingRuleID)
	// Evaluate the rule inside a transaction which locks the rule so that
	// other apps can't concurrently evaluate it.
	err := notifications.Transaction(ctx, ev.dbh, func(tx notifications.TX) error {
		rule := &tables.UsageAlertingRule{}
		err := tx.NewQuery(ctx, "usage_alerts_get_rule_id").Raw(`
			SELECT *
			FROM "UsageAlertingRules"
			WHERE usage_alerting_rule_id = ?
			`+ev.dbh.SelectForUpdateModifier(),
			usageAlertingRuleID,
		).Take(rule)
		if err != nil {
			return err
		}
		now := ev.clock.Now()

		// Determine the most recently elapsed usage period. For example, if the
		// rule is evaluated monthly, this period starts 00:00 UTC on the first
		// of last month, up to but not including 00:00 UTC of the first of this
		// month.
		//
		// For simplicity, we send alerts only for the most recently elapsed
		// evaluation period. This comes with the limitation that if the alert
		// evaluation is down for longer than the minimum allowed period
		// (currently 1 day), we may drop alerts.

		currentPeriodStartTime, err := getAlertingPeriodStartTime(rule.AlertingPeriod, now)
		if err != nil {
			return err
		}

		// Check whether the alert was already triggered on or after the current
		// period start time. Note: we don't check the period *end* time here
		// because we might be lagging slightly behind another app, and it's
		// possible that the alert was triggered during the *next* alerting
		// period.
		lastTriggeredTime := time.UnixMicro(rule.LastTriggeredAtUsec)
		if lastTriggeredTime.Unix() >= currentPeriodStartTime.Unix() {
			log.CtxDebugf(ctx, "Not alerting: rule already triggered during this period")
			return nil
		}

		// Get the usage for this period, using the requested column.
		alertingPeriodUsageTotal, err := ev.queryUsageWithRedisCache(ctx, tx, rule, now, currentPeriodStartTime)
		if err != nil {
			return err
		}

		groupInfo := &tables.Group{}
		err = tx.NewQuery(ctx, "usage_alerts_get_group_details").Raw(`
			SELECT group_id, name, url_identifier
			FROM "Groups"
			WHERE group_id = ?
			`,
			rule.GroupID,
		).Take(groupInfo)
		if db.IsRecordNotFound(err) {
			return status.NotFoundError("group not found")
		}
		if err != nil {
			return status.WrapError(err, "get group details")
		}

		if alertingPeriodUsageTotal > rule.Threshold {
			// Create a notification, inserting a Notification row within this
			// transaction.
			alertingPeriodMsg, err := formatAlertingPeriod(rule.AlertingPeriod)
			if err != nil {
				return err
			}
			usageMetricMsg, err := formatUsageMetric(rule.UsageMetric)
			if err != nil {
				return err
			}
			quantityMsg, err := formatValue(rule.UsageMetric, rule.Threshold)
			if err != nil {
				return err
			}
			message := fmt.Sprintf("%s: %s %s usage exceeded %s", groupInfo.Name, alertingPeriodMsg, usageMetricMsg, quantityMsg)

			payload := &nfpb.Notification{
				Text: message,
				Email: &nfpb.Email{
					Subject: "BuildBuddy usage alert",
					Body: e.HTML(
						e.Header(),
						e.Title(e.B(groupInfo.Name), " / Usage alerts"),
						e.Card(
							e.Center(e.ImageAsset("circle-alert_orange_48px.png", 48, 48)),
							e.VGap(8),
							e.Center(e.Heading("Usage alert triggered")),
							e.P("BuildBuddy ", e.B(fmt.Sprintf("%s %s usage", alertingPeriodMsg, usageMetricMsg)), " has exceeded a configured alerting threshold."),
							e.CTA(subdomain.ReplaceURLSubdomainForGroup(build_buddy_url.WithPath("/usage/").String(), groupInfo), "See usage"),
						),
						e.Footer(),
					),
				},
			}

			// Structure the content ID so that alert notification triggering is
			// deduped based on rule ID and rule evaluation period.
			contentID := fmt.Sprintf("%s/%d", rule.UsageAlertingRuleID, currentPeriodStartTime.UnixMicro())
			if _, err := ev.notificationService.Create(ctx, tx, rule.GroupID, contentID, payload); err != nil {
				return status.WrapError(err, "create notification")
			}

			// Update the last_triggered_at timestamp.
			err = tx.NewQuery(ctx, "usage_alerts_update_evaluation_timestamp").Raw(`
				UPDATE "UsageAlertingRules"
				SET last_triggered_at_usec = ?
				WHERE usage_alerting_rule_id = ?
				`,
				now.UnixMicro(),
				rule.UsageAlertingRuleID,
			).Exec().Error
			if err != nil {
				return status.InternalErrorf("update rule evaluation timestamp: %s", err)
			}

			log.CtxDebugf(ctx, "Created usage alert notification")
		}

		return nil
	})
	// TODO: ignore "locked" errors - it just means another app got to this
	// alert first.
	return err
}

func (ev *Evaluator) readCachedAlertingPeriodUsage(ctx context.Context, rule *tables.UsageAlertingRule, alertingPeriodStartTime time.Time) (total int64, endTime time.Time, err error) {
	h, err := ev.rdb.HGetAll(ctx, alertingPeriodUsageCacheKey(rule, alertingPeriodStartTime)).Result()
	if err != nil {
		return 0, time.Time{}, err
	}
	if len(h) == 0 {
		return 0, time.Time{}, nil
	}
	total, err = strconv.ParseInt(h["total"], 10, 64)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf(`invalid "total" value %q`, h["total"])
	}
	endTimeUnixMicro, err := strconv.ParseInt(h["end_time_usec"], 10, 64)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf(`invalid "end_time_usec" value %q`, h["total"])
	}
	return total, time.UnixMicro(endTimeUnixMicro), nil
}

func (ev *Evaluator) writeCachedAlertingPeriodUsage(ctx context.Context, rule *tables.UsageAlertingRule, alertingPeriodStartTime, cachedEndTime time.Time, total int64) error {
	_, err := ev.rdb.HSet(ctx, alertingPeriodUsageCacheKey(rule, alertingPeriodStartTime), map[string]any{
		"total":         total,
		"end_time_usec": cachedEndTime.UnixMicro(),
	}).Result()
	return err
}

func (ev *Evaluator) queryUsageWithRedisCache(ctx context.Context, tx interfaces.DB, rule *tables.UsageAlertingRule, now, alertingPeriodStartTime time.Time) (int64, error) {
	alertingPeriodEndTime, err := nextAlertingPeriodStartTime(rule.AlertingPeriod, now)
	if err != nil {
		return 0, err
	}

	columnName, err := usageColumnName(rule.UsageMetric)
	if err != nil {
		return 0, err
	}

	// We only cache finalized usage data, i.e. usage data during
	// time windows that will no longer be updated in the DB. This means
	// that to get the usage for the current alerting period, we only need to
	// fetch the incremental usage since the last finalized row.
	//
	// TODO: usage.periodStartingAt(now - 6min)
	// TODO: s/6min/usage.redisKeyTTL + usage.periodDuration/
	now = now.UTC()
	finalizedUsageEndTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute()-6, 0, 0, time.UTC)

	total, cachedEndTime, err := ev.readCachedAlertingPeriodUsage(ctx, rule, alertingPeriodStartTime)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to read cached alerting period usage: %s", err)
	}

	// If applicable, do an SQL query to fetch the finalized incremental usage
	// since the cached end time, then add that to the cached usage.
	if finalizedUsageEndTime.After(alertingPeriodStartTime) && finalizedUsageEndTime.After(cachedEndTime) {
		var result struct{ Total int64 }
		err = tx.NewQuery(ctx, "usage_alerts_query_finalized_usage").Raw(`
			SELECT SUM(`+columnName+`) AS total
			FROM "Usages"
			WHERE group_id = ?
			AND period_start_usec >= ?
			AND period_start_usec < ?
			`,
			rule.GroupID,
			// If we didn't have any cached usage, then cachedEndTime will be
			// zero. Use max() here to ensure we only query within the alerting
			// period.
			max(alertingPeriodStartTime.UnixMicro(), cachedEndTime.UnixMicro()),
			// Note: finalizedUsageEndTime should be before
			// alertingPeriodEndTime because finalizedUsageEndTime is before
			// now, and now is before alertingPeriodEndTime. So this query
			// should not include usage after the alerting period.
			finalizedUsageEndTime.UnixMicro(),
		).Take(&result)
		if err != nil {
			return 0, err
		}
		total += result.Total
		if err := ev.writeCachedAlertingPeriodUsage(ctx, rule, alertingPeriodStartTime, finalizedUsageEndTime, total); err != nil {
			log.CtxWarningf(ctx, "Failed to cache alerting period usage: %s", err)
		}
	}

	// Finally, read the non-finalized usage rows within the current alerting
	// period. We cannot avoid DB queries for these if we want to minimize
	// latency, but we should only be reading a very small number of rows here
	// since this only includes a few minutes of usage data.
	var result struct{ Total int64 }
	err = tx.NewQuery(ctx, "usage_alerts_query_nonfinal_usage").Raw(`
		SELECT SUM(`+columnName+`) AS total
		FROM "Usages"
		WHERE group_id = ?
		AND period_start_usec >= ?
		AND period_start_usec < ?
		`,
		rule.GroupID,
		max(alertingPeriodStartTime.UnixMicro(), finalizedUsageEndTime.UnixMicro()),
		alertingPeriodEndTime.UnixMicro(),
	).Take(&result)
	if err != nil {
		return 0, err
	}
	total += result.Total

	return total, nil
}

func alertingPeriodUsageCacheKey(rule *tables.UsageAlertingRule, alertingPeriodStartTime time.Time) string {
	// Note: not just using rule ID as the key, since rules can be updated
	// with different alerting periods and/or usage columns.
	return fmt.Sprintf("%s%s/%d/%d/%d", redisAlertingPeriodUsageCacheKeyPrefix, rule.GroupID, rule.AlertingPeriod, rule.UsageMetric, alertingPeriodStartTime.UnixMicro())
}

func getAlertingPeriodStartTime(evalPeriod usagepb.AlertingPeriod, t time.Time) (time.Time, error) {
	switch evalPeriod {
	case usagepb.AlertingPeriod_DAILY:
		return startOfDayUTC(t), nil
	case usagepb.AlertingPeriod_WEEKLY:
		return startOfBusinessWeekUTC(t), nil
	case usagepb.AlertingPeriod_MONTHLY:
		return startOfMonthUTC(t), nil
	default:
		return time.Time{}, status.InvalidArgumentErrorf("invalid eval period %d", evalPeriod)
	}
}

func nextAlertingPeriodStartTime(evalPeriod usagepb.AlertingPeriod, t time.Time) (time.Time, error) {
	switch evalPeriod {
	case usagepb.AlertingPeriod_DAILY:
		return startOfNextDayUTC(t), nil
	case usagepb.AlertingPeriod_WEEKLY:
		return startOfNextBusinessWeekUTC(t), nil
	case usagepb.AlertingPeriod_MONTHLY:
		return startOfNextMonthUTC(t), nil
	default:
		return time.Time{}, status.InvalidArgumentErrorf("invalid eval period %d", evalPeriod)
	}
}

func startOfDayUTC(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func startOfNextDayUTC(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
}

func startOfBusinessWeekUTC(t time.Time) time.Time {
	t = t.UTC()
	// t.Weekday() returns the 0-indexed weekday starting with Sunday, but we
	// want it to start with Monday, so we subtract 1, mod 7.
	businessWeekday := mod(int(t.Weekday())-1, 7)
	return time.Date(t.Year(), t.Month(), t.Day()-businessWeekday, 0, 0, 0, 0, time.UTC)
}

func startOfNextBusinessWeekUTC(t time.Time) time.Time {
	t = t.UTC()
	sameDayAndTimeNextWeek := t.Add(7 * 24 * time.Hour)
	return startOfBusinessWeekUTC(sameDayAndTimeNextWeek)
}

func startOfMonthUTC(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func startOfNextMonthUTC(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

func usageColumnName(v usagepb.UsageMetric) (string, error) {
	switch v {
	case usagepb.UsageMetric_INVOCATIONS:
		return "invocations", nil
	case usagepb.UsageMetric_ACTION_CACHE_HITS:
		return "action_cache_hits", nil
	case usagepb.UsageMetric_CAS_CACHE_HITS:
		return "cas_cache_hits", nil
	case usagepb.UsageMetric_TOTAL_DOWNLOAD_SIZE_BYTES:
		return "total_download_size_bytes", nil
	case usagepb.UsageMetric_LINUX_EXECUTION_DURATION_USEC:
		return "linux_execution_duration_usec", nil
	case usagepb.UsageMetric_TOTAL_UPLOAD_SIZE_BYTES:
		return "total_upload_size_bytes", nil
	case usagepb.UsageMetric_TOTAL_CACHED_ACTION_EXEC_USEC:
		return "total_cached_action_exec_usec", nil
	default:
		return "", status.InvalidArgumentErrorf("invalid usage count enum %d", v)
	}
}

func formatAlertingPeriod(evalPeriod usagepb.AlertingPeriod) (string, error) {
	switch evalPeriod {
	case usagepb.AlertingPeriod_DAILY:
		return "daily", nil
	case usagepb.AlertingPeriod_WEEKLY:
		return "weekly", nil
	case usagepb.AlertingPeriod_MONTHLY:
		return "monthly", nil
	default:
		return "", status.InvalidArgumentErrorf("invalid eval period enum %d", evalPeriod)
	}
}

// Returns the usage counter in a format that fits grammatically into the
// sentence "Monthly <name> exceeded ..."
func formatUsageMetric(v usagepb.UsageMetric) (string, error) {
	switch v {
	case usagepb.UsageMetric_INVOCATIONS:
		return "invocation count", nil
	case usagepb.UsageMetric_ACTION_CACHE_HITS:
		return "action cache hits", nil
	case usagepb.UsageMetric_CAS_CACHE_HITS:
		return "content-addressable storage cache hits", nil
	case usagepb.UsageMetric_TOTAL_DOWNLOAD_SIZE_BYTES:
		return "cache download", nil
	case usagepb.UsageMetric_LINUX_EXECUTION_DURATION_USEC:
		return "Linux execution duration", nil
	case usagepb.UsageMetric_TOTAL_UPLOAD_SIZE_BYTES:
		return "cache upload", nil
	case usagepb.UsageMetric_TOTAL_CACHED_ACTION_EXEC_USEC:
		return "cached build duration", nil
	default:
		return "", status.InvalidArgumentErrorf("invalid usage count enum %d", v)
	}
}

// TODO: nicer formatting (10K invocations, 100GB cache download, etc.)
func formatValue(v usagepb.UsageMetric, value int64) (string, error) {
	switch v {
	case usagepb.UsageMetric_INVOCATIONS:
		return fmt.Sprintf("%d", value), nil
	case usagepb.UsageMetric_ACTION_CACHE_HITS:
		return fmt.Sprintf("%d", value), nil
	case usagepb.UsageMetric_CAS_CACHE_HITS:
		return fmt.Sprintf("%d", value), nil
	case usagepb.UsageMetric_TOTAL_DOWNLOAD_SIZE_BYTES:
		return fmt.Sprintf("%d bytes", value), nil
	case usagepb.UsageMetric_LINUX_EXECUTION_DURATION_USEC:
		return fmt.Sprintf("%d minutes", value/60e6), nil
	case usagepb.UsageMetric_TOTAL_UPLOAD_SIZE_BYTES:
		return fmt.Sprintf("%d bytes", value), nil
	case usagepb.UsageMetric_TOTAL_CACHED_ACTION_EXEC_USEC:
		return fmt.Sprintf("%d minutes", value/60e6), nil
	default:
		return "", status.InvalidArgumentErrorf("invalid usage count enum %d", v)
	}
}

// True mod operator, returning a mod b which is always positive if b is
// positive.
// Panics if b <= 0.
func mod(a, b int) int {
	if b <= 0 {
		panic("modulus must be positive")
	}
	return (a%b + b) % b
}
