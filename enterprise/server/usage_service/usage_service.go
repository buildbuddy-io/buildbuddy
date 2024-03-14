package usage_service

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"

	usage_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage/config"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

var usageStartDate = flag.String("app.usage_start_date", "", "If set, usage data will only be viewable on or after this timestamp. Specified in RFC3339 format, like 2021-10-01T00:00:00Z")

const (
	// Allow the user to view this many months of usage data.
	maxNumMonthsOfUsageToReturn = 6
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

func (s *usageService) GetUsage(ctx context.Context, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error) {
	groupID := req.GetRequestContext().GetGroupId()
	if err := authutil.AuthorizeGroupAccess(ctx, s.env, groupID); err != nil {
		return nil, err
	}

	// Build up the list of available usage periods to return to the client. For
	// now we just send the last 6 periods regardless of whether any usage data
	// is available for those periods.
	now := s.clock.Now().UTC()
	var availableUsagePeriods []string
	for i := 0; i < maxNumMonthsOfUsageToReturn; i++ {
		p := getUsagePeriod(addCalendarMonths(now, -i))
		availableUsagePeriods = append(availableUsagePeriods, p.String())
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
		// TODO(bduffany): after the next rollout, just return the current usage
		// period if the usage_period field is empty, instead of the last 6.

		// Get the timestamp corresponding to the start of the current month in UTC,
		// with a month offset so that we return the desired number of months of
		// usage.
		maxNumHistoricalMonths := maxNumMonthsOfUsageToReturn - 1
		start = addCalendarMonths(getUsagePeriod(now).Start(), -maxNumHistoricalMonths)
		// Limit the start date to the start of the month in which we began collecting
		// usage data.
		start = maxTime(start, getUsagePeriod(s.start).Start())
		end = addCalendarMonths(getUsagePeriod(now).Start(), 1)
	}

	usages, err := s.scanUsages(ctx, groupID, start, end)
	if err != nil {
		return nil, err
	}

	// Build the response list from scanned rows, inserting explicit zeroes for
	// periods with no data.
	rsp := &usagepb.GetUsageResponse{
		AvailableUsagePeriods: availableUsagePeriods,
	}
	for t := start; !(t.After(end) || t.Equal(end)); t = addCalendarMonths(t, 1) {
		period := getUsagePeriod(t).String()

		if len(usages) > 0 && usages[0].Period == period {
			rsp.Usage = append(rsp.Usage, usages[0])
			usages = usages[1:]
		} else {
			rsp.Usage = append(rsp.Usage, &usagepb.Usage{Period: period})
		}
	}
	// Make sure we always return at least one month (to make the client simpler).
	if len(rsp.Usage) == 0 {
		rsp.Usage = append(rsp.Usage, &usagepb.Usage{
			Period: getUsagePeriod(now).String(),
		})
	}
	// Return in reverse-chronological order.
	slices.Reverse(rsp.Usage)
	return rsp, nil
}

func (s *usageService) scanUsages(ctx context.Context, groupID string, start, end time.Time) ([]*usagepb.Usage, error) {
	dbh := s.env.GetDBHandle()
	rq := dbh.NewQuery(ctx, "usage_service_scan").Raw(`
		SELECT `+dbh.UTCMonthFromUsecTimestamp("period_start_usec")+` AS period,
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
