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
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return nil, err
	}

	db := s.env.GetDBHandle()

	// Get the timestamp corresponding to the start of the current month in UTC,
	// with a month offset so that we return the desired number of months of
	// usage.
	now := time.Now().UTC()
	maxNumHistoricalMonths := maxNumMonthsOfUsageToReturn - 1
	createdAfter := addCalendarMonths(startOfMonth(now), -maxNumHistoricalMonths)
	createdBefore := addCalendarMonths(startOfMonth(now), 1)

	rows, err := db.Raw(`
		SELECT `+db.UTCMonthFromUsecTimestamp("created_at_usec")+` AS period,
		COUNT(1) AS total_num_builds,
		SUM(action_cache_hits) AS action_cache_hits,
		SUM(cas_cache_hits) AS cas_cache_hits,
		SUM(total_download_size_bytes) AS total_download_size_bytes
		FROM Invocations
		WHERE created_at_usec >= ? AND created_at_usec < ?
		AND group_id = ?
		GROUP BY period
		ORDER BY period ASC
	`, u.GetGroupID(), timeutil.ToUsec(createdAfter), timeutil.ToUsec(createdBefore)).Rows()
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
	// Build the response list from scanned rows, inserting explicit zeroes for months with no data.
	rsp := &usagepb.GetUsageResponse{}
	for t := createdAfter; !(t.Equal(createdBefore) || t.After(createdBefore)); t = addCalendarMonths(t, 1) {
		period := fmt.Sprintf("%d-%02d", t.Year(), t.Month())
		if len(usages) > 0 && usages[0].Period == period {
			rsp.Usage = append(rsp.Usage, usages[0])
			usages = usages[1:]
		} else {
			rsp.Usage = append(rsp.Usage, &usagepb.Usage{
				Period: period,
			})
		}
	}

	// Return in reverse-chronological order.
	reverseUsageSlice(rsp.Usage)

	return rsp, nil
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
