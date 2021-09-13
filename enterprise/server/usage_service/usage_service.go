package usage_service

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"

	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

const (
	// Allow the user to view this many months of usage data.
	maxNumMonthsOfUsageToReturn = 6
)

type usageService struct {
	env environment.Env
}

func New(env environment.Env) *usageService {
	return &usageService{env}
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
	end := addCalendarMonths(startOfMonth(now), 1)

	usages, err := s.scanUsages(ctx, groupID, start, end)
	if err != nil {
		return nil, err
	}
	invocationUsages, err := s.scanInvocationUsages(ctx, groupID, start, end)
	if err != nil {
		return nil, err
	}

	// Build the response list from scanned rows, inserting explicit zeroes for
	// periods with no data.
	rsp := &usagepb.GetUsageResponse{}
	for t := start; !(t.After(end) || t.Equal(end)); t = addCalendarMonths(t, 1) {
		period := fmt.Sprintf("%d-%02d", t.Year(), t.Month())

		var usage *usagepb.Usage
		if len(usages) > 0 && usages[0].Period == period {
			usage = usages[0]
			usages = usages[1:]
		} else {
			usage = &usagepb.Usage{Period: period}
		}

		// Populate invocation count if it exists
		if len(invocationUsages) > 0 && invocationUsages[0].Period == period {
			usage.TotalNumBuilds = invocationUsages[0].TotalNumBuilds
			invocationUsages = invocationUsages[1:]
		}

		rsp.Usage = append(rsp.Usage, usage)
	}

	// Return in reverse-chronological order.
	reverseUsageSlice(rsp.Usage)

	return rsp, nil
}

func (s *usageService) scanUsages(ctx context.Context, groupID string, start, end time.Time) ([]*usagepb.Usage, error) {
	db := s.env.GetDBHandle()

	rows, err := db.Raw(`
		SELECT `+db.UTCMonthFromUsecTimestamp("period_start_usec")+` AS period,
		SUM(action_cache_hits) AS action_cache_hits,
		SUM(cas_cache_hits) AS cas_cache_hits,
		SUM(total_download_size_bytes) AS total_download_size_bytes
		FROM Usages
		WHERE period_start_usec >= ? AND period_start_usec < ?
		AND group_id = ?
		GROUP BY period
		ORDER BY period ASC
	`, timeutil.ToUsec(start), timeutil.ToUsec(end), groupID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	usages := []*usagepb.Usage{}
	for rows.Next() {
		usage := &usagepb.Usage{}
		if err := db.ScanRows(rows, usage); err != nil {
			return nil, err
		}
		usages = append(usages, usage)
	}

	return usages, nil
}

func (s *usageService) scanInvocationUsages(ctx context.Context, groupID string, start, end time.Time) ([]*usagepb.Usage, error) {
	db := s.env.GetDBHandle()

	rows, err := db.Raw(`
		SELECT `+db.UTCMonthFromUsecTimestamp("period_start_usec")+` AS period,
		COUNT(*) AS total_num_builds
		FROM InvocationUsages
		WHERE period_start_usec >= ? AND period_start_usec < ?
		AND group_id = ?
		GROUP BY period
		ORDER BY period ASC
	`, timeutil.ToUsec(start), timeutil.ToUsec(end), groupID).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	usages := []*usagepb.Usage{}
	for rows.Next() {
		usage := &usagepb.Usage{}
		if err := db.ScanRows(rows, usage); err != nil {
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
