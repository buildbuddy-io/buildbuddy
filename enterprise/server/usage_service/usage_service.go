package usage_service

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"

	usage_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage/config"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

var usageStartDate = flag.String("app.usage_start_date", "", "If set, usage data will only be viewable on or after this timestamp. Specified in RFC3339 format, like 2021-10-01T00:00:00Z")

const (
	// Allow the user to view this many months of usage data.
	maxNumMonthsOfUsageToReturn = 6
)

type usageService struct {
	env environment.Env

	// start is the earliest moment in time at which we're able to return usage
	// data to the user. We return usage data from the start of the month
	// corresponding to this date.
	start time.Time
}

// Registers the usage service if usage tracking is enabled
func Register(env environment.Env) error {
	if usage_config.UsageTrackingEnabled() {
		env.SetUsageService(New(env))
	}
	return nil
}

func New(env environment.Env) *usageService {
	return &usageService{
		env:   env,
		start: configuredUsageStartDate(),
	}
}

func (s *usageService) GetUsage(ctx context.Context, req *usagepb.GetUsageRequest) (*usagepb.GetUsageResponse, error) {
	groupID := req.GetRequestContext().GetGroupId()
	if err := perms.AuthorizeGroupAccess(ctx, s.env, groupID); err != nil {
		return nil, err
	}

	// Get the timestamp corresponding to the start of the current month in UTC,
	// with a month offset so that we return the desired number of months of
	// usage.
	now := time.Now().UTC()
	maxNumHistoricalMonths := maxNumMonthsOfUsageToReturn - 1

	start := addCalendarMonths(startOfMonth(now), -maxNumHistoricalMonths)
	// Limit the start date to the start of the month in which we began collecting
	// usage data.
	start = maxTime(start, startOfMonth(s.start))

	end := addCalendarMonths(startOfMonth(now), 1)

	usages, err := s.scanUsages(ctx, groupID, start, end)
	if err != nil {
		return nil, err
	}

	// Build the response list from scanned rows, inserting explicit zeroes for
	// periods with no data.
	rsp := &usagepb.GetUsageResponse{}
	for t := start; !(t.After(end) || t.Equal(end)); t = addCalendarMonths(t, 1) {
		period := fmt.Sprintf("%d-%02d", t.Year(), t.Month())

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
			Period: fmt.Sprintf("%d-%02d", now.Year(), now.Month()),
		})
	}
	// Return in reverse-chronological order.
	reverseUsageSlice(rsp.Usage)
	return rsp, nil
}

func (s *usageService) scanUsages(ctx context.Context, groupID string, start, end time.Time) ([]*usagepb.Usage, error) {
	dbh := s.env.GetDBHandle()
	rows, err := dbh.DB(ctx).Raw(`
		SELECT `+dbh.UTCMonthFromUsecTimestamp("period_start_usec")+` AS period,
		SUM(invocations) AS invocations,
		SUM(action_cache_hits) AS action_cache_hits,
		SUM(cas_cache_hits) AS cas_cache_hits,
		SUM(total_download_size_bytes) AS total_download_size_bytes,
		SUM(linux_execution_duration_usec) AS linux_execution_duration_usec
		FROM Usages
		WHERE period_start_usec >= ? AND period_start_usec < ?
		AND group_id = ?
		GROUP BY period
		ORDER BY period ASC
	`, start.UnixMicro(), end.UnixMicro(), groupID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	usages := []*usagepb.Usage{}
	for rows.Next() {
		usage := &usagepb.Usage{}
		if err := dbh.DB(ctx).ScanRows(rows, usage); err != nil {
			return nil, err
		}
		usages = append(usages, usage)
	}

	return usages, nil
}

func startOfMonth(t time.Time) time.Time {
	// Set day to 1 and time to 0:00:00.000
	return time.Date(
		t.Year(), t.Month(), 1, /*=day*/
		0, 0, 0, 0, /*=hour,min,sec,nsec*/
		t.Location())
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

func reverseUsageSlice(a []*usagepb.Usage) {
	for i := 0; i < len(a)/2; i++ {
		j := len(a) - i - 1
		a[i], a[j] = a[j], a[i]
	}
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
