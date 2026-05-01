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
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"google.golang.org/protobuf/types/known/timestamppb"

	usage_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage/config"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
)

var (
	usageStartDate = flag.String("app.usage_start_date", "", "If set, usage data will only be viewable on or after this timestamp. Specified in RFC3339 format, like 2021-10-01T00:00:00Z")
	alertsEnabled  = flag.Bool("app.usage_alerts_enabled", false, "If set, usage alerts will be enabled in the UI.")
)

const (
	// MaxUsageAlertingRulesPerGroup is the maximum number of usage alerting
	// rules a group can create.
	MaxUsageAlertingRulesPerGroup = 100
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

// GetAlertsEnabled returns whether usage alerting should be exposed to the frontend.
func (s *usageService) GetAlertsEnabled() bool {
	return *alertsEnabled
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
		aggregateUsage.TotalExternalDownloadSizeBytes += u.GetTotalExternalDownloadSizeBytes()
		aggregateUsage.TotalInternalDownloadSizeBytes += u.GetTotalInternalDownloadSizeBytes()
		aggregateUsage.TotalWorkflowDownloadSizeBytes += u.GetTotalWorkflowDownloadSizeBytes()
		aggregateUsage.TotalUploadSizeBytes += u.GetTotalUploadSizeBytes()
		aggregateUsage.TotalExternalUploadSizeBytes += u.GetTotalExternalUploadSizeBytes()
		aggregateUsage.TotalInternalUploadSizeBytes += u.GetTotalInternalUploadSizeBytes()
		aggregateUsage.TotalWorkflowUploadSizeBytes += u.GetTotalWorkflowUploadSizeBytes()
		aggregateUsage.LinuxExecutionDurationUsec += u.GetLinuxExecutionDurationUsec()
		aggregateUsage.TotalCachedActionExecUsec += u.GetTotalCachedActionExecUsec()
		aggregateUsage.CloudRbeLinuxExecutionDurationUsec += u.GetCloudRbeLinuxExecutionDurationUsec()
		aggregateUsage.CloudWorkflowLinuxExecutionDurationUsec += u.GetCloudWorkflowLinuxExecutionDurationUsec()
		aggregateUsage.CloudCpuNanos += u.GetCloudCpuNanos()
		aggregateUsage.CloudRbeCpuNanos += u.GetCloudRbeCpuNanos()
		aggregateUsage.CloudWorkflowCpuNanos += u.GetCloudWorkflowCpuNanos()
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
	groupID := u.GetGroupID()
	if groupID == "" {
		return nil, status.PermissionDeniedError("group ID is required")
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		return nil, err
	}

	rq := s.env.GetDBHandle().NewQuery(ctx, "usage_service_get_alerting_rules").Raw(`
		SELECT *
		FROM "UsageAlertingRules"
		WHERE group_id = ?
		ORDER BY created_at_usec ASC, usage_alerting_rule_id ASC
	`, groupID)
	rows, err := db.ScanAll(rq, &tables.UsageAlertingRule{})
	if err != nil {
		return nil, err
	}

	rsp := &usagepb.GetUsageAlertingRulesResponse{}
	for _, row := range rows {
		rule, err := s.usageAlertingRuleToProto(ctx, row)
		if err != nil {
			return nil, err
		}
		rsp.UsageAlertingRule = append(rsp.UsageAlertingRule, rule)
	}
	return rsp, nil
}

func (s *usageService) CreateUsageAlertingRule(ctx context.Context, req *usagepb.CreateUsageAlertingRuleRequest) (*usagepb.CreateUsageAlertingRuleResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()
	if groupID == "" {
		return nil, status.PermissionDeniedError("group ID is required")
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		return nil, err
	}

	config := req.GetConfiguration()
	if err := validateUsageAlertingRuleConfiguration(config); err != nil {
		return nil, err
	}
	id, err := tables.PrimaryKeyForTable((&tables.UsageAlertingRule{}).TableName())
	if err != nil {
		return nil, err
	}
	row := &tables.UsageAlertingRule{
		UsageAlertingRuleID: id,
		GroupID:             groupID,
		UserID:              u.GetUserID(),
		UsageAlertingMetric: config.GetMetric(),
		AbsoluteThreshold:   config.GetAbsoluteThreshold(),
		Window:              config.GetWindow(),
	}
	err = s.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		count, err := s.countUsageAlertingRules(ctx, tx, groupID)
		if err != nil {
			return err
		}
		if count >= MaxUsageAlertingRulesPerGroup {
			return status.ResourceExhaustedErrorf("usage alerting rule limit exceeded (%d)", MaxUsageAlertingRulesPerGroup)
		}
		return tx.NewQuery(ctx, "usage_service_create_alerting_rule").Create(row)
	})
	if err != nil {
		if s.env.GetDBHandle().IsDuplicateKeyError(err) {
			return nil, status.AlreadyExistsError("usage alerting rule already exists")
		}
		return nil, err
	}

	rule, err := s.usageAlertingRuleToProto(ctx, row)
	if err != nil {
		return nil, err
	}
	return &usagepb.CreateUsageAlertingRuleResponse{
		UsageAlertingRule: rule,
	}, nil
}

func (s *usageService) DeleteUsageAlertingRule(ctx context.Context, req *usagepb.DeleteUsageAlertingRuleRequest) (*usagepb.DeleteUsageAlertingRuleResponse, error) {
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()
	if groupID == "" {
		return nil, status.PermissionDeniedError("group ID is required")
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		return nil, err
	}
	ruleID := req.GetUsageAlertingRuleId()
	if ruleID == "" {
		return nil, status.InvalidArgumentError("usage alerting rule ID is required")
	}

	result := s.env.GetDBHandle().NewQuery(ctx, "usage_service_delete_alerting_rule").Raw(`
		DELETE FROM "UsageAlertingRules"
		WHERE usage_alerting_rule_id = ? AND group_id = ?
	`, ruleID, groupID).Exec()
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, status.NotFoundError("usage alerting rule not found")
	}
	return &usagepb.DeleteUsageAlertingRuleResponse{}, nil
}

func (s *usageService) countUsageAlertingRules(ctx context.Context, dbh interfaces.DB, groupID string) (int64, error) {
	row := &struct{ Count int64 }{}
	if err := dbh.NewQuery(ctx, "usage_service_count_alerting_rules").Raw(`
		SELECT COUNT(*) AS count
		FROM "UsageAlertingRules"
		WHERE group_id = ?
	`, groupID).Take(row); err != nil {
		return 0, err
	}
	return row.Count, nil
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
		SUM(total_cached_action_exec_usec) AS total_cached_action_exec_usec,
		SUM(CASE WHEN origin <> 'internal' THEN total_download_size_bytes ELSE 0 END) AS total_external_download_size_bytes,
		SUM(CASE WHEN (origin = 'internal' AND NOT (client = 'executor-workflows' OR client = 'bazel')) THEN total_download_size_bytes ELSE 0 END) AS total_internal_download_size_bytes,
		SUM(CASE WHEN (origin = 'internal' AND (client = 'executor-workflows' OR client = 'bazel')) THEN total_download_size_bytes ELSE 0 END) AS total_workflow_download_size_bytes,
		SUM(CASE WHEN origin <> 'internal' THEN total_upload_size_bytes ELSE 0 END) AS total_external_upload_size_bytes,
		SUM(CASE WHEN (origin = 'internal' AND NOT (client = 'executor-workflows' OR client = 'bazel')) THEN total_upload_size_bytes ELSE 0 END) AS total_internal_upload_size_bytes,
		SUM(CASE WHEN (origin = 'internal' AND (client = 'executor-workflows' OR client = 'bazel')) THEN total_upload_size_bytes ELSE 0 END) AS total_workflow_upload_size_bytes,
		SUM(CASE WHEN origin = 'internal' THEN cpu_nanos ELSE 0 END) AS cloud_cpu_nanos,
		SUM(CASE WHEN (origin = 'internal' AND NOT (client = 'executor-workflows' OR client = 'bazel')) THEN cpu_nanos ELSE 0 END) AS cloud_rbe_cpu_nanos,
		SUM(CASE WHEN (origin = 'internal' AND (client = 'executor-workflows' OR client = 'bazel')) THEN cpu_nanos ELSE 0 END) AS cloud_workflow_cpu_nanos,
		SUM(CASE WHEN (origin = 'internal' AND NOT (client = 'executor-workflows' OR client = 'bazel')) THEN linux_execution_duration_usec ELSE 0 END) AS cloud_rbe_linux_execution_duration_usec,
		SUM(CASE WHEN (origin = 'internal' AND (client = 'executor-workflows' OR client = 'bazel')) THEN linux_execution_duration_usec ELSE 0 END) AS cloud_workflow_linux_execution_duration_usec
		FROM "Usages"
		WHERE period_start_usec >= ? AND period_start_usec < ?
		AND group_id = ?
		GROUP BY period
		ORDER BY period ASC
	`, start.UnixMicro(), end.UnixMicro(), groupID)
	return db.ScanAll(rq, &usagepb.Usage{})
}

func validateUsageAlertingRuleConfiguration(config *usagepb.UsageAlertingRuleConfiguration) error {
	if config == nil {
		return status.InvalidArgumentError("configuration is required")
	}
	if !isValidUsageAlertingMetric(config.GetMetric()) {
		return status.InvalidArgumentError("usage alerting metric is required")
	}
	if config.GetAbsoluteThreshold() <= 0 {
		return status.InvalidArgumentError("absolute threshold must be positive")
	}
	if !isValidUsageAlertingWindow(config.GetWindow()) {
		return status.InvalidArgumentError("usage alerting window is required")
	}
	return nil
}

func isValidUsageAlertingMetric(metric usagepb.UsageAlertingMetric_Value) bool {
	_, ok := usagepb.UsageAlertingMetric_Value_name[int32(metric)]
	return metric != usagepb.UsageAlertingMetric_UNKNOWN && ok
}

func isValidUsageAlertingWindow(window usagepb.UsageAlertingWindow_Value) bool {
	_, ok := usagepb.UsageAlertingWindow_Value_name[int32(window)]
	return window != usagepb.UsageAlertingWindow_UNKNOWN && ok
}

func (s *usageService) usageAlertingRuleToProto(ctx context.Context, row *tables.UsageAlertingRule) (*usagepb.UsageAlertingRule, error) {
	return &usagepb.UsageAlertingRule{
		Metadata: &usagepb.UsageAlertingRuleMetadata{
			UsageAlertingRuleId: row.UsageAlertingRuleID,
			CreatedByUser:       s.displayUser(ctx, row.UserID),
			CreatedTimestamp:    timestampFromUsec(row.CreatedAtUsec),
		},
		Configuration: &usagepb.UsageAlertingRuleConfiguration{
			Metric:            row.UsageAlertingMetric,
			AbsoluteThreshold: row.AbsoluteThreshold,
			Window:            row.Window,
		},
		Status: &usagepb.UsageAlertingRuleStatus{
			LastEvaluationTimestamp: timestampFromUsec(row.LastEvaluationUsec),
			LastFiredTimestamp:      timestampFromUsec(row.LastFiredUsec),
		},
	}, nil
}

func (s *usageService) displayUser(ctx context.Context, userID string) *uidpb.DisplayUser {
	if userID == "" {
		return nil
	}
	if udb := s.env.GetUserDB(); udb != nil {
		// We only need user profile fields, so only fetch direct memberships.
		u, err := udb.GetUserByIDWithoutAuthCheck(ctx, userID, &interfaces.GetUserOpts{DirectMembershipsOnly: true})
		if err == nil {
			return u.ToProto()
		}
		if !status.IsNotFoundError(err) {
			log.CtxWarningf(ctx, "Could not load user %q for usage alerting rule: %s", userID, err)
		}
	}
	return &uidpb.DisplayUser{
		UserId: &uidpb.UserId{Id: userID},
	}
}

func timestampFromUsec(usec int64) *timestamppb.Timestamp {
	if usec == 0 {
		return nil
	}
	return timestamppb.New(time.UnixMicro(usec).UTC())
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
