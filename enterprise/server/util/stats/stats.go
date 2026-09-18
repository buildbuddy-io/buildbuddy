package stats

import (
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"

	stpb "github.com/buildbuddy-io/buildbuddy/proto/stats"
)

type StatInterval int

const (
	StatInterval5Minutes StatInterval = iota
	StatInterval15Minutes
	StatInterval30Minutes
	StatInterval1Hour
	StatInterval2Hours
	StatInterval4Hours
	StatInterval1Day
)

var (
	finerTimeBuckets = flag.Bool("app.finer_time_buckets", true, "If enabled, split trends and drilldowns into smaller time buckets when the user has a smaller date range selected.")
)

func (s StatInterval) Duration() time.Duration {
	switch s {
	case StatInterval5Minutes:
		return 5 * time.Minute
	case StatInterval15Minutes:
		return 15 * time.Minute
	case StatInterval30Minutes:
		return 30 * time.Minute
	case StatInterval1Hour:
		return 1 * time.Hour
	case StatInterval2Hours:
		return 2 * time.Hour
	case StatInterval4Hours:
		return 4 * time.Hour
	case StatInterval1Day:
		return 24 * time.Hour
	}
	return 24 * time.Hour
}

func (s StatInterval) ClickhouseInterval() string {
	switch s {
	case StatInterval5Minutes:
		return "5 MINUTE"
	case StatInterval15Minutes:
		return "15 MINUTE"
	case StatInterval30Minutes:
		return "30 MINUTE"
	case StatInterval1Hour:
		return "1 HOUR"
	case StatInterval2Hours:
		return "2 HOUR"
	case StatInterval4Hours:
		return "4 HOUR"
	case StatInterval1Day:
		return "1 DAY"
	}
	return "1 DAY"
}

func (s StatInterval) IntervalProto() *stpb.StatsInterval {
	switch s {
	case StatInterval5Minutes:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_MINUTE,
			Count: 5,
		}
	case StatInterval15Minutes:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_MINUTE,
			Count: 15,
		}
	case StatInterval30Minutes:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_MINUTE,
			Count: 30,
		}
	case StatInterval1Hour:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_HOUR,
			Count: 1,
		}
	case StatInterval2Hours:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_HOUR,
			Count: 2,
		}
	case StatInterval4Hours:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_HOUR,
			Count: 4,
		}
	case StatInterval1Day:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_DAY,
			Count: 1,
		}
	}
	return &stpb.StatsInterval{
		Type:  stpb.IntervalType_INTERVAL_TYPE_DAY,
		Count: 1,
	}
}

// ComputeTrendsInterval returns the stats bucket size to use for a response
// covering a time range of the given duration.
// These values are currently set to keep us under ~50 intervals in a response.
// We need to make some visual improvements to cache charts so that they're
// easier to read with lots of small intervals before we can do more than this.
func ComputeStatInterval(d time.Duration) StatInterval {
	if d <= 3*time.Hour {
		return StatInterval5Minutes
	}
	if d <= 12*time.Hour {
		return StatInterval15Minutes
	}
	// 25 hours so that even when crossing DST, we still show 30-min intervals.
	if d <= 25*time.Hour {
		return StatInterval30Minutes
	}
	if d <= 48*time.Hour {
		return StatInterval1Hour
	}
	if d <= 4*24*time.Hour {
		return StatInterval2Hours
	}
	if d <= 8*24*time.Hour {
		return StatInterval4Hours
	}
	return StatInterval1Day
}

// FinerTimeBucketsEnabled returns whether responses with time-bucketed stats
// should use buckets sized to the query's date range instead of 1-day buckets.
func FinerTimeBucketsEnabled() bool {
	return *finerTimeBuckets
}
