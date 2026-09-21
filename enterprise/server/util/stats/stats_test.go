package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	stpb "github.com/buildbuddy-io/buildbuddy/proto/stats"
)

var allIntervals = []struct {
	interval           StatInterval
	duration           time.Duration
	clickhouseInterval string
	protoType          stpb.IntervalType
	protoCount         int64
}{
	{StatInterval5Minutes, 5 * time.Minute, "5 MINUTE", stpb.IntervalType_INTERVAL_TYPE_MINUTE, 5},
	{StatInterval15Minutes, 15 * time.Minute, "15 MINUTE", stpb.IntervalType_INTERVAL_TYPE_MINUTE, 15},
	{StatInterval30Minutes, 30 * time.Minute, "30 MINUTE", stpb.IntervalType_INTERVAL_TYPE_MINUTE, 30},
	{StatInterval1Hour, 1 * time.Hour, "1 HOUR", stpb.IntervalType_INTERVAL_TYPE_HOUR, 1},
	{StatInterval2Hours, 2 * time.Hour, "2 HOUR", stpb.IntervalType_INTERVAL_TYPE_HOUR, 2},
	{StatInterval4Hours, 4 * time.Hour, "4 HOUR", stpb.IntervalType_INTERVAL_TYPE_HOUR, 4},
	{StatInterval1Day, 24 * time.Hour, "1 DAY", stpb.IntervalType_INTERVAL_TYPE_DAY, 1},
}

func TestDuration(t *testing.T) {
	for _, tc := range allIntervals {
		t.Run(tc.clickhouseInterval, func(t *testing.T) {
			assert.Equal(t, tc.duration, tc.interval.Duration())
		})
	}
}

func TestClickhouseInterval(t *testing.T) {
	for _, tc := range allIntervals {
		t.Run(tc.clickhouseInterval, func(t *testing.T) {
			assert.Equal(t, tc.clickhouseInterval, tc.interval.ClickhouseInterval())
		})
	}
}

func TestIntervalProto(t *testing.T) {
	for _, tc := range allIntervals {
		t.Run(tc.clickhouseInterval, func(t *testing.T) {
			p := tc.interval.IntervalProto()
			require.NotNil(t, p)
			assert.Equal(t, tc.protoType, p.GetType())
			assert.Equal(t, tc.protoCount, p.GetCount())
		})
	}
}

func TestUnknownIntervalDefaultsToOneDay(t *testing.T) {
	unknown := StatInterval1Day + 1
	assert.Equal(t, 24*time.Hour, unknown.Duration())
	assert.Equal(t, "1 DAY", unknown.ClickhouseInterval())
	p := unknown.IntervalProto()
	require.NotNil(t, p)
	assert.Equal(t, stpb.IntervalType_INTERVAL_TYPE_DAY, p.GetType())
	assert.Equal(t, int64(1), p.GetCount())
}

func TestComputeStatInterval(t *testing.T) {
	const day = 24 * time.Hour
	for _, tc := range []struct {
		name     string
		duration time.Duration
		expected StatInterval
	}{
		{"zero", 0, StatInterval5Minutes},
		{"negative", -time.Hour, StatInterval5Minutes},
		{"1h", time.Hour, StatInterval5Minutes},
		{"3h exactly", 3 * time.Hour, StatInterval5Minutes},
		{"just over 3h", 3*time.Hour + time.Nanosecond, StatInterval15Minutes},
		{"12h exactly", 12 * time.Hour, StatInterval15Minutes},
		{"just over 12h", 12*time.Hour + time.Nanosecond, StatInterval30Minutes},
		{"24h", 24 * time.Hour, StatInterval30Minutes},
		{"25h exactly (DST-crossing day)", 25 * time.Hour, StatInterval30Minutes},
		{"just over 25h", 25*time.Hour + time.Nanosecond, StatInterval1Hour},
		{"48h exactly", 48 * time.Hour, StatInterval1Hour},
		{"just over 48h", 48*time.Hour + time.Nanosecond, StatInterval2Hours},
		{"4d exactly", 4 * day, StatInterval2Hours},
		{"just over 4d", 4*day + time.Nanosecond, StatInterval4Hours},
		{"8d exactly", 8 * day, StatInterval4Hours},
		{"just over 8d", 8*day + time.Nanosecond, StatInterval1Day},
		{"30d", 30 * day, StatInterval1Day},
		{"1y", 365 * day, StatInterval1Day},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ComputeStatInterval(tc.duration))
		})
	}
}
