package usage_service

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"

	usage_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage/config"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

var usageStartDate = flag.String("app.usage_start_date", "", "If set, usage data will only be viewable on or after this timestamp. Specified in RFC3339 format, like 2021-10-01T00:00:00Z")

const (
	// Max number of alerting rules that an org is allowed to have.
	alertingRulesLimit = 20
)

type usageService struct {
	env   environment.Env
	clock clockwork.Clock

	// start is the earliest moment in time at which we're able to return usage
	// data to the user. We return usage data from the start of the month
	// corresponding to this date.
	start time.Time
}

// Registers the usage service if usage tracking is enabled
func Register(env *real_environment.RealEnv) error {
	if usage_config.UsageTrackingEnabled() {
		env.SetUsageService(New(env, clockwork.NewRealClock()))
	}
	return nil
}

func New(env environment.Env, clock clockwork.Clock) *usageService {
	return &usageService{
		env:   env,
		clock: clock,
		start: configuredUsageStartDate(),
	}
}

// Just a little function to make testing less miserable.
func (s *usageService) GetUsageInternal(ctx context.Context, g *tables.Group, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error) {
	earliestAvailableUsagePeriod := max(g.CreatedAtUsec, configuredUsageStartDate().UnixMicro())
	now := s.clock.Now().UTC()
	endOfLatestUsagePeriod := addCalendarMonths(getUsagePeriod(now).Start(), 1)

	var availableUsagePeriods []string
	for usagePeriodStart := time.UnixMicro(earliestAvailableUsagePeriod); usagePeriodStart.Before(endOfLatestUsagePeriod); usagePeriodStart = addCalendarMonths(usagePeriodStart, 1) {
		p := getUsagePeriod(usagePeriodStart)
		availableUsagePeriods = append([]string{p.String()}, availableUsagePeriods...)
	}

	var start, end time.Time
	if req.GetUsagePeriod() != "" {
		p, err := parseUsagePeriod(req.GetUsagePeriod())
		if err != nil {
			return nil, err
		}
		start = p.Start()
		end = addCalendarMonths(start, 1)
	} else {
		start = getUsagePeriod(now).Start()
		end = addCalendarMonths(start, 1)
	}

	usages, err := s.scanUsages(ctx, g.GroupID, start, end)
	if err != nil {
		return nil, err
	}

	rsp := &usagepb.GetUsageResponse{
		AvailableUsagePeriods: availableUsagePeriods,
	}
	period := getUsagePeriod(start).String()

	aggregateUsage := &usagepb.Usage{
		Period: period,
	}

	for _, u := range usages {
		aggregateUsage.Invocations += u.GetInvocations()
		aggregateUsage.ActionCacheHits += u.GetActionCacheHits()
		aggregateUsage.CasCacheHits += u.GetCasCacheHits()
		aggregateUsage.TotalDownloadSizeBytes += u.GetTotalDownloadSizeBytes()
		aggregateUsage.TotalUploadSizeBytes += u.GetTotalUploadSizeBytes()
		aggregateUsage.LinuxExecutionDurationUsec += u.GetLinuxExecutionDurationUsec()
		aggregateUsage.TotalCachedActionExecUsec += u.GetTotalCachedActionExecUsec()
	}

	rsp.Usage = aggregateUsage
	rsp.DailyUsage = usages
	return rsp, nil
}

func (s *usageService) GetUsage(ctx context.Context, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()

	g, err := s.env.GetUserDB().GetGroupByID(ctx, groupID)
	if err != nil {
		return nil, err
	}

	return s.GetUsageInternal(ctx, g, req)
}

func (s *usageService) GetUsageAlertingRules(ctx context.Context, req *usagepb.GetUsageAlertingRulesRequest) (*usagepb.GetUsageAlertingRulesResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := db.ScanAll(s.env.GetDBHandle().NewQuery(ctx, "usage_get_alerting_rules").Raw(`
		SELECT *
		FROM "UsageAlertingRules"
		WHERE group_id = ?
		ORDER BY created_at_usec ASC
		`,
		u.GetGroupID(),
	), &tables.UsageAlertingRule{})
	if err != nil {
		log.CtxErrorf(ctx, "Alerting rules query failed: %s", err)
		return nil, status.InternalError("failed to query rules")
	}
	rsp := &usagepb.GetUsageAlertingRulesResponse{
		AlertingRules: make([]*usagepb.AlertingRule, 0, len(rows)),
		RuleLimit:     alertingRulesLimit,
	}
	for _, row := range rows {
		rsp.AlertingRules = append(rsp.AlertingRules, &usagepb.AlertingRule{
			AlertingRuleId: row.UsageAlertingRuleID,
			AlertingPeriod: row.AlertingPeriod,
			UsageMetric:    row.UsageMetric,
			Threshold:      row.Threshold,
		})
	}
	return rsp, nil
}

func (s *usageService) UpdateUsageAlertingRules(ctx context.Context, req *usagepb.UpdateUsageAlertingRulesRequest) (*usagepb.UpdateUsageAlertingRulesResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	err = s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		for _, update := range req.GetUpdates() {
			if deleteID := update.GetDeleteAlertingRuleId(); deleteID != "" {
				// Delete
				err = tx.NewQuery(ctx, "usage_alerting_rules_delete").Raw(`
					DELETE FROM "UsageAlertingRules"
					WHERE group_id = ?
					AND usage_alerting_rule_id = ?
					`,
					u.GetGroupID(),
					deleteID,
				).Exec().Error
				if err != nil {
					return status.InternalErrorf("delete rule %q: %s", deleteID, err)
				}
				continue
			}

			// Create or update
			rule := update.GetAlertingRule()
			if err := validateAlertingRule(rule); err != nil {
				return status.InvalidArgumentErrorf("invalid alerting rule: %s", err)
			}

			if rule.GetAlertingRuleId() == "" {
				// Create
				pk, err := tables.PrimaryKeyForTable((&tables.UsageAlertingRule{}).TableName())
				if err != nil {
					return err
				}
				err = tx.NewQuery(ctx, "usage_alerting_rules_create").Create(&tables.UsageAlertingRule{
					UsageAlertingRuleID: pk,
					GroupID:             u.GetGroupID(),
					UsageMetric:         rule.GetUsageMetric(),
					AlertingPeriod:      rule.GetAlertingPeriod(),
					Threshold:           rule.GetThreshold(),
				})
				if err != nil {
					return status.InternalErrorf("create alerting rule: %s", err)
				}
			} else {
				// Update
				res := tx.NewQuery(ctx, "usage_alerting_rules_update").Raw(`
					UPDATE "UsageAlertingRules"
					SET alerting_period = ?, usage_metric = ?, threshold = ?
					WHERE group_id = ? AND usage_alerting_rule_id = ?
					`,
					rule.GetAlertingPeriod(),
					rule.GetUsageMetric(),
					rule.GetThreshold(),
					u.GetGroupID(),
					rule.GetAlertingRuleId(),
				).Exec()
				if res.Error != nil {
					return status.InternalErrorf("update alerting rule: %s", err)
				}
				if res.RowsAffected == 0 {
					return status.NotFoundErrorf("alerting rule %q not found (possible concurrent edit by another user?)", err)
				}
			}
		}

		// Before committing the transaction, make sure we didn't exceed the
		// rule limit.
		var result struct{ Count int64 }
		err := tx.NewQuery(ctx, "usage_alerting_rules_check_count").Raw(`
			SELECT COUNT(*) as count
			FROM "UsageAlertingRules"
			WHERE group_id = ?
			`,
			u.GetGroupID(),
		).Take(&result)
		if err != nil {
			return status.InternalErrorf("check postconditions: %s", err)
		}
		if result.Count > alertingRulesLimit {
			return status.UnknownErrorf("exceeded rule limit (%d)", alertingRulesLimit)
		}
		return nil
	})
	if err != nil {
		return nil, status.InternalErrorf("failed to update rules: %s", err)
	}
	return &usagepb.UpdateUsageAlertingRulesResponse{}, nil
}

func validateAlertingRule(rule *usagepb.AlertingRule) error {
	if rule == nil {
		return status.InvalidArgumentErrorf("missing alerting rule")
	}
	if rule.GetAlertingPeriod() == 0 {
		return status.InvalidArgumentErrorf("missing alerting rule alerting period field")
	}
	if rule.GetUsageMetric() == 0 {
		return status.InvalidArgumentErrorf("missing alerting rule usage count field")
	}
	if rule.GetThreshold() < 0 {
		return status.InvalidArgumentErrorf("alerting rule threshold cannot be negative")
	}
	return nil
}

func (s *usageService) scanUsages(ctx context.Context, groupID string, start, end time.Time) ([]*usagepb.Usage, error) {
	dbh := s.env.GetDBHandle()
	rq := dbh.NewQuery(ctx, "usage_service_scan").Raw(`
		SELECT `+dbh.DateFromUsecTimestamp("period_start_usec", 0)+` AS period,
		SUM(invocations) AS invocations,
		SUM(action_cache_hits) AS action_cache_hits,
		SUM(cas_cache_hits) AS cas_cache_hits,
		SUM(total_download_size_bytes) AS total_download_size_bytes,
		SUM(linux_execution_duration_usec) AS linux_execution_duration_usec,
		SUM(total_upload_size_bytes) AS total_upload_size_bytes,
		SUM(total_cached_action_exec_usec) AS total_cached_action_exec_usec
		FROM "Usages"
		WHERE period_start_usec >= ? AND period_start_usec < ?
		AND group_id = ?
		GROUP BY period
		ORDER BY period ASC
	`, start.UnixMicro(), end.UnixMicro(), groupID)
	return db.ScanAll(rq, &usagepb.Usage{})
}

type usagePeriod struct {
	year  int
	month time.Month
}

func parseUsagePeriod(s string) (*usagePeriod, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return nil, status.InvalidArgumentError("invalid usage period")
	}
	y, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, status.InvalidArgumentError("invalid usage period")
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil || m < 1 || m > 12 {
		return nil, status.InvalidArgumentError("invalid usage period")
	}
	return &usagePeriod{year: y, month: time.Month(m)}, nil
}

func getUsagePeriod(t time.Time) *usagePeriod {
	t = t.UTC()
	return &usagePeriod{year: t.Year(), month: t.Month()}
}

func (u *usagePeriod) String() string {
	return fmt.Sprintf("%d-%02d", u.year, u.month)
}

func (u *usagePeriod) Start() time.Time {
	return time.Date(u.year, u.month, 1, 0, 0, 0, 0, time.UTC)
}

func addCalendarMonths(t time.Time, months int) time.Time {
	// Note: the month arithmetic works because Go allows passing month values
	// outside their usual range. For example, a month value of 0 corresponds to
	// December of the preceding year.
	return time.Date(
		t.Year(), time.Month(int(t.Month())+months), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		t.Location())
}

func configuredUsageStartDate() time.Time {
	if *usageStartDate == "" {
		log.Warningf("Usage start date is not configured; usage page may show some months with missing usage data.")
		return time.Unix(0, 0).UTC()
	}
	start, err := time.Parse(time.RFC3339, *usageStartDate)
	if err != nil {
		log.Errorf("Failed to parse app.usage_start_date from string %q: %s", *usageStartDate, err)
		return time.Unix(0, 0).UTC()
	}
	if start.After(time.Now()) {
		log.Warningf("Configured usage start date %q is in the future; usage page may show empty usage data.", *usageStartDate)
	}
	return start.UTC()
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
