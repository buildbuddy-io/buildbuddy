package timing_profile

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/trace_events"
	"github.com/stretchr/testify/require"
)

var testProfile = struct {
	OtherData   map[string]any        `json:"otherData"`
	TraceEvents []*trace_events.Event `json:"traceEvents"`
}{
	OtherData: map[string]any{"build_id": "test"},
	TraceEvents: []*trace_events.Event{
		// Metadata ("M") event: counted in EventCount but not SpanCount.
		{Name: "thread_name", Phase: "M", ProcessID: 1, ThreadID: 1, Args: map[string]any{"name": "Main Thread"}},
		// Longest span overall, but a "build phase marker", not "action processing" —
		// should be excluded from SlowestActions.
		{Category: "build phase marker", Name: "buildTargets", Phase: "X", Timestamp: 0, Duration: 30, ProcessID: 1, ThreadID: 1},
		// Second "build phase marker"; duration should sum with the first build phase marker in DurationByCategory.
		{Category: "build phase marker", Name: "runAnalysisPhase", Phase: "X", Timestamp: 1, Duration: 5, ProcessID: 1, ThreadID: 1},
		// "remote action execution" should be excluded from SlowestActions because it isn't "action processing".
		{Category: "remote action execution", Name: "execute remotely", Phase: "X", Timestamp: 8, Duration: 10, ProcessID: 1, ThreadID: 1},
		// Second remote span. Durations should sum in DurationByCategory.
		{Category: "remote action execution", Name: "execute remotely", Phase: "X", Timestamp: 18, Duration: 7, ProcessID: 1, ThreadID: 1},
		// "action processing" is the only category kept in SlowestActions.
		{Category: "action processing", Name: "action 'GoCompilePkg server/foo/foo.a'", Phase: "X", Timestamp: 30, Duration: 4, ProcessID: 1, ThreadID: 1},
		{Category: "action processing", Name: "GoCompilePkg server/bar/bar.a", Phase: "X", Timestamp: 34, Duration: 6, ProcessID: 1, ThreadID: 1},
		{Category: "action processing", Name: "Testing //foo:foo_test (shard 1 of 2)", Phase: "X", Timestamp: 40, Duration: 11, ProcessID: 1, ThreadID: 1},
		{Category: "action processing", Name: "Testing //foo:foo_test (shard 2 of 2)", Phase: "X", Timestamp: 51, Duration: 13, ProcessID: 1, ThreadID: 1},
		// Skipped: negative end time (ts+dur = -5 < 0), so not counted as a span.
		{Category: "ignored", Name: "negative", Phase: "X", Timestamp: -10, Duration: 5, ProcessID: 1, ThreadID: 1},
		// Skipped: non-positive duration (dur <= 0), so not counted as a span.
		{Category: "ignored", Name: "zero", Phase: "X", Timestamp: 1, Duration: 0, ProcessID: 1, ThreadID: 1},
		// Counter ("C") event: counted in EventCount but not SpanCount.
		{Name: "action count", Phase: "C", Timestamp: 1, ProcessID: 1, Args: map[string]any{"action": 1}},
	},
}

func marshalTestProfile(t *testing.T) []byte {
	t.Helper()
	data, err := json.Marshal(testProfile)
	require.NoError(t, err)
	return data
}

func TestParseTimingProfile(t *testing.T) {
	profile, err := ParseTimingProfile(bytes.NewReader(marshalTestProfile(t)), unlimitedMaxTopSpans)
	require.NoError(t, err)

	require.Equal(t, 12, profile.EventCount)
	// Skips events with duration <= 0.
	require.Equal(t, 8, profile.SpanCount)
	require.Equal(t, time.Duration(64)*time.Microsecond, profile.TotalDuration)

	byCategory := aggregateDurations(profile.DurationByCategory)
	require.Equal(t, time.Duration(17)*time.Microsecond, byCategory["remote action execution"])
	require.Equal(t, time.Duration(35)*time.Microsecond, byCategory["build phase marker"])
	require.Equal(t, time.Duration(34)*time.Microsecond, byCategory["action processing"])

	require.Len(t, profile.SlowestActions, 4)
	for _, span := range profile.SlowestActions {
		require.Equal(t, "action processing", span.Category)
	}
	require.Equal(t, "Testing //foo:foo_test (shard 2 of 2)", profile.SlowestActions[0].Name)
	require.Equal(t, time.Duration(13)*time.Microsecond, profile.SlowestActions[0].Duration)
	require.InDelta(t, 20.3, profile.SlowestActions[0].WallTimePercent, 0.0001)
	require.Equal(t, "Testing //foo:foo_test (shard 1 of 2)", profile.SlowestActions[1].Name)
	require.Equal(t, time.Duration(11)*time.Microsecond, profile.SlowestActions[1].Duration)
	require.InDelta(t, 17.2, profile.SlowestActions[1].WallTimePercent, 0.0001)
}

func TestParseTimingProfile_Limit(t *testing.T) {
	profile, err := ParseTimingProfile(bytes.NewReader(marshalTestProfile(t)), 3)
	require.NoError(t, err)

	require.Equal(t, 12, profile.EventCount)
	// Skips events with duration <= 0.
	require.Equal(t, 8, profile.SpanCount)
	require.Equal(t, time.Duration(64)*time.Microsecond, profile.TotalDuration)

	byCategory := aggregateDurations(profile.DurationByCategory)
	require.Equal(t, time.Duration(17)*time.Microsecond, byCategory["remote action execution"])
	require.Equal(t, time.Duration(35)*time.Microsecond, byCategory["build phase marker"])
	require.Equal(t, time.Duration(34)*time.Microsecond, byCategory["action processing"])

	require.Len(t, profile.SlowestActions, 3)
	require.Equal(t, []time.Duration{13 * time.Microsecond, 11 * time.Microsecond, 6 * time.Microsecond}, []time.Duration{
		profile.SlowestActions[0].Duration,
		profile.SlowestActions[1].Duration,
		profile.SlowestActions[2].Duration,
	})
}

func TestParseTimingProfileGzip(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(marshalTestProfile(t))
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	profile, err := ParseTimingProfile(&buf, unlimitedMaxTopSpans)
	require.NoError(t, err)
	require.Equal(t, 8, profile.SpanCount)
}

func aggregateDurations(aggregates []TimingAggregate) map[string]time.Duration {
	out := make(map[string]time.Duration)
	for _, aggregate := range aggregates {
		out[aggregate.Name] = aggregate.Duration
	}
	return out
}
