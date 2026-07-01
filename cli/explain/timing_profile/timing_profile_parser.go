package timing_profile

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/trace_events"
)

const unlimitedMaxTopSpans = -1

// In the timing profile, Bazel records each action under several nested categories
// that are shown as stacked layers. Only show spans from one category - "action processing",
// the outermost layer - to avoid returning duplicates.
const actionCategory = "action processing"

// TimingProfile is a distilled summary of a Bazel build's timing profile.
type TimingProfile struct {
	// EventCount is the total number of raw trace events in the profile,
	// including untimed ones such as metadata and counters. It mainly indicates
	// the profile's size.
	EventCount int
	// SpanCount is the number of timed operations ("spans") recorded. Unlike
	// EventCount it counts only real units of timed work, so it reflects how
	// much actually happened during the build.
	SpanCount int
	// TotalDuration is the build's wall-clock time, from start until the last
	// operation finished.
	TotalDuration time.Duration
	// DurationByCategory is time summed by the kind of work Bazel was doing
	// (for example, running actions remotely vs. local analysis). It shows at a
	// high level where time went — which types of work dominate — so you know
	// where to look before drilling into individual actions. Because work runs
	// in parallel, these sums can exceed TotalDuration.
	DurationByCategory []TimingAggregate
	// SlowestActions are the longest-running actions.
	// An action is a single unit of build work, such as compiling a file
	// or running a test. Because work runs in parallel, these can exceed
	// TotalDuration.
	SlowestActions []TimingSpan
	// TODO: Add all critical path spans to the output.
}

type TimingAggregate struct {
	Name     string
	Duration time.Duration
	Count    int
}

type TimingSpan struct {
	Name     string
	Category string
	Duration time.Duration
	// WallTimePercent is the span's duration as a percentage of the profile's
	// total wall time. Because actions run in parallel, these can sum to more
	// than 100% across spans.
	WallTimePercent float64
}

type timingProfileParser struct {
	eventCount         int
	spanCount          int
	totalDurationUsec  int64
	durationByCategory map[string]*TimingAggregate
	maxTopSpans        int
	spans              []TimingSpan
}

func ParseTimingProfile(r io.Reader, maxTopSpans int) (*TimingProfile, error) {
	parser := &timingProfileParser{
		durationByCategory: make(map[string]*TimingAggregate),
		maxTopSpans:        maxTopSpans,
	}
	if err := parser.readEvents(r); err != nil {
		return nil, err
	}
	return parser.profile(), nil
}

func (p *timingProfileParser) readEvents(r io.Reader) error {
	reader, err := maybeGunzip(r)
	if err != nil {
		return err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	var profile trace_events.Profile
	if err := json.NewDecoder(reader).Decode(&profile); err != nil {
		return fmt.Errorf("decode timing profile: %w", err)
	}
	for _, event := range profile.TraceEvents {
		if err := p.addEvent(*event); err != nil {
			return err
		}
	}
	return nil
}

func (p *timingProfileParser) addEvent(event trace_events.Event) error {
	p.eventCount++
	if event.Phase == trace_events.PhaseComplete {
		p.addSpan(event)
	}
	return nil
}

func (p *timingProfileParser) addSpan(event trace_events.Event) {
	if event.Duration <= 0 || event.Timestamp+event.Duration < 0 {
		return
	}
	p.spanCount++
	p.totalDurationUsec = max(p.totalDurationUsec, event.Timestamp+event.Duration)
	p.addDuration(event.Category, event.Duration)
	p.spans = append(p.spans, TimingSpan{
		Name:     event.Name,
		Category: event.Category,
		Duration: time.Duration(event.Duration) * time.Microsecond,
	})
}

func (p *timingProfileParser) profile() *TimingProfile {
	return &TimingProfile{
		EventCount:         p.eventCount,
		SpanCount:          p.spanCount,
		TotalDuration:      time.Duration(p.totalDurationUsec) * time.Microsecond,
		DurationByCategory: sortedAggregates(p.durationByCategory),
		SlowestActions:     p.sortedSlowestActions(),
	}
}

// sortedSlowestActions returns the slowest action spans, keeping only the top
// `maxTopSpans` (a negative limit keeps all).
func (p *timingProfileParser) sortedSlowestActions() []TimingSpan {
	actions := make([]TimingSpan, 0, len(p.spans))
	for _, span := range p.spans {
		if span.Category == actionCategory {
			actions = append(actions, span)
		}
	}
	sort.SliceStable(actions, func(i, j int) bool {
		return actions[i].Duration > actions[j].Duration
	})
	if p.maxTopSpans >= 0 && len(actions) > p.maxTopSpans {
		actions = actions[:p.maxTopSpans]
	}
	for i := range actions {
		actions[i].WallTimePercent = p.wallTimePercent(actions[i].Duration)
	}
	return actions
}

// wallTimePercent returns d as a percentage of the profile's total wall time,
// rounded to one decimal place.
func (p *timingProfileParser) wallTimePercent(d time.Duration) float64 {
	total := time.Duration(p.totalDurationUsec) * time.Microsecond
	if total <= 0 {
		return 0
	}
	pct := float64(d) / float64(total) * 100
	return math.Round(pct*10) / 10
}

func maybeGunzip(r io.Reader) (io.Reader, error) {
	buffered := bufio.NewReader(r)
	header, err := buffered.Peek(2)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read timing profile header: %w", err)
	}
	if len(header) == 2 && header[0] == 0x1f && header[1] == 0x8b {
		gz, err := gzip.NewReader(buffered)
		if err != nil {
			return nil, fmt.Errorf("read gzip timing profile: %w", err)
		}
		return gz, nil
	}
	return buffered, nil
}

func (p *timingProfileParser) addDuration(category string, durationUsec int64) {
	if category == "" {
		category = "(unknown)"
	}
	aggregate := p.durationByCategory[category]
	if aggregate == nil {
		aggregate = &TimingAggregate{Name: category}
		p.durationByCategory[category] = aggregate
	}
	aggregate.Duration += time.Duration(durationUsec) * time.Microsecond
	aggregate.Count++
}

func sortedAggregates(aggregates map[string]*TimingAggregate) []TimingAggregate {
	out := make([]TimingAggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		if aggregate.Duration > 0 {
			out = append(out, *aggregate)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Duration != out[j].Duration {
			return out[i].Duration > out[j].Duration
		}
		return out[i].Name < out[j].Name
	})
	return out
}
