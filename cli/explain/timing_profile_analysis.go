package explain

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
)

const (
	tracePhaseComplete = "X"
	tracePhaseMetadata = "M"

	criticalPathThreadName = "Critical Path"
)

type TimingProfileReport struct {
	InvocationID            string
	InvocationDurationUsec  int64
	ProfileDurationUsec     int64
	EventCount              int
	SpanCount               int
	CriticalPath            CriticalPathSummary
	PhaseBreakdown          []TimingAggregate
	ExecutionBreakdown      []TimingAggregate
	AggregateDurationByName []TimingAggregate
}

type CriticalPathSummary struct {
	Found        bool
	DurationUsec int64
	SpanCount    int
	ByMnemonic   []TimingAggregate
	ByCategory   []TimingAggregate
	LongestSpans []TimingSpan
	Spans        []TimingSpan
}

type TimingAggregate struct {
	Name         string
	DurationUsec int64
	Count        int
}

type TimingSpan struct {
	ThreadID      int64
	ThreadName    string
	Category      string
	Name          string
	Mnemonic      string
	Target        string
	Output        string
	TimestampUsec int64
	DurationUsec  int64
}

type traceProfileEvent struct {
	Category  string         `json:"cat,omitempty"`
	Name      string         `json:"name,omitempty"`
	Phase     string         `json:"ph,omitempty"`
	Timestamp *int64         `json:"ts,omitempty"`
	Duration  *int64         `json:"dur,omitempty"`
	ProcessID *int64         `json:"pid,omitempty"`
	ThreadID  *int64         `json:"tid,omitempty"`
	Output    string         `json:"out,omitempty"`
	Args      map[string]any `json:"args,omitempty"`
}

type timingProfileAnalysis struct {
	eventCount      int
	spanCount       int
	profileEndUsec  int64
	threadNames     map[int64]string
	spansByThreadID map[int64][]TimingSpan
	durationByName  map[string]*TimingAggregate
	durationByCat   map[string]*TimingAggregate
	durationByKind  map[string]*TimingAggregate
}

func AnalyzeTimingProfile(r io.Reader) (*TimingProfileReport, error) {
	analysis := &timingProfileAnalysis{
		threadNames:     make(map[int64]string),
		spansByThreadID: make(map[int64][]TimingSpan),
		durationByName:  make(map[string]*TimingAggregate),
		durationByCat:   make(map[string]*TimingAggregate),
		durationByKind:  make(map[string]*TimingAggregate),
	}
	if err := readTraceProfileEvents(r, analysis.addEvent); err != nil {
		return nil, err
	}
	return analysis.report(), nil
}

func (a *timingProfileAnalysis) addEvent(event traceProfileEvent) error {
	a.eventCount++
	if event.ThreadID != nil && event.Phase == tracePhaseMetadata && event.Name == "thread_name" {
		if name := stringArg(event.Args, "name"); name != "" {
			a.threadNames[*event.ThreadID] = normalizeTraceThreadName(name)
		}
		return nil
	}
	if event.Phase != tracePhaseComplete || event.ThreadID == nil || event.Timestamp == nil || event.Duration == nil {
		return nil
	}
	if *event.Duration <= 0 || *event.Timestamp+*event.Duration < 0 {
		return nil
	}

	span := TimingSpan{
		ThreadID:      *event.ThreadID,
		Category:      event.Category,
		Name:          event.Name,
		Mnemonic:      stringArg(event.Args, "mnemonic"),
		Target:        stringArg(event.Args, "target"),
		Output:        event.Output,
		TimestampUsec: *event.Timestamp,
		DurationUsec:  *event.Duration,
	}
	a.spanCount++
	a.profileEndUsec = max(a.profileEndUsec, span.TimestampUsec+span.DurationUsec)
	a.spansByThreadID[span.ThreadID] = append(a.spansByThreadID[span.ThreadID], span)
	addDuration(a.durationByName, span.Name, span.DurationUsec)
	addDuration(a.durationByCat, span.Category, span.DurationUsec)
	addDuration(a.durationByKind, spanKind(span), span.DurationUsec)
	return nil
}

func (a *timingProfileAnalysis) report() *TimingProfileReport {
	for tid, spans := range a.spansByThreadID {
		threadName := a.threadNames[tid]
		for i := range spans {
			spans[i].ThreadName = threadName
		}
		a.spansByThreadID[tid] = spans
	}

	return &TimingProfileReport{
		EventCount:              a.eventCount,
		SpanCount:               a.spanCount,
		ProfileDurationUsec:     a.profileEndUsec,
		CriticalPath:            a.criticalPathSummary(),
		PhaseBreakdown:          phaseBreakdown(a.durationByName),
		ExecutionBreakdown:      executionBreakdown(a.durationByName, a.durationByCat),
		AggregateDurationByName: sortedAggregates(a.durationByName),
	}
}

func (a *timingProfileAnalysis) criticalPathSummary() CriticalPathSummary {
	var spans []TimingSpan
	for tid, threadSpans := range a.spansByThreadID {
		if a.threadNames[tid] != criticalPathThreadName {
			continue
		}
		spans = append(spans, threadSpans...)
	}
	if len(spans) == 0 {
		return CriticalPathSummary{}
	}
	sort.SliceStable(spans, func(i, j int) bool {
		if spans[i].TimestampUsec != spans[j].TimestampUsec {
			return spans[i].TimestampUsec < spans[j].TimestampUsec
		}
		return spans[i].DurationUsec > spans[j].DurationUsec
	})

	durationUsec := int64(0)
	byMnemonic := make(map[string]*TimingAggregate)
	byCategory := make(map[string]*TimingAggregate)
	for _, span := range spans {
		durationUsec += span.DurationUsec
		addDuration(byMnemonic, spanKind(span), span.DurationUsec)
		addDuration(byCategory, span.Category, span.DurationUsec)
	}

	longest := append([]TimingSpan(nil), spans...)
	sort.SliceStable(longest, func(i, j int) bool {
		if longest[i].DurationUsec != longest[j].DurationUsec {
			return longest[i].DurationUsec > longest[j].DurationUsec
		}
		return longest[i].TimestampUsec < longest[j].TimestampUsec
	})

	return CriticalPathSummary{
		Found:        true,
		DurationUsec: durationUsec,
		SpanCount:    len(spans),
		ByMnemonic:   sortedAggregates(byMnemonic),
		ByCategory:   sortedAggregates(byCategory),
		LongestSpans: longest,
		Spans:        spans,
	}
}

func readTraceProfileEvents(r io.Reader, consume func(traceProfileEvent) error) error {
	reader, err := maybeGunzip(r)
	if err != nil {
		return err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("read profile object: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("timing profile JSON must start with an object")
	}

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("read profile key: %w", err)
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("expected profile object key, got %v", token)
		}
		if key != "traceEvents" {
			var ignored json.RawMessage
			if err := decoder.Decode(&ignored); err != nil {
				return fmt.Errorf("skip profile field %q: %w", key, err)
			}
			continue
		}

		token, err = decoder.Token()
		if err != nil {
			return fmt.Errorf("read traceEvents array: %w", err)
		}
		if delim, ok := token.(json.Delim); !ok || delim != '[' {
			return fmt.Errorf("traceEvents must be an array")
		}
		for decoder.More() {
			var event traceProfileEvent
			if err := decoder.Decode(&event); err != nil {
				return fmt.Errorf("read trace event: %w", err)
			}
			if err := consume(event); err != nil {
				return err
			}
		}
		token, err = decoder.Token()
		if err != nil {
			return fmt.Errorf("close traceEvents array: %w", err)
		}
		if delim, ok := token.(json.Delim); !ok || delim != ']' {
			return fmt.Errorf("traceEvents array was not closed")
		}
	}
	return nil
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

func phaseBreakdown(durationByName map[string]*TimingAggregate) []TimingAggregate {
	launching := duration(durationByName, "Launch Blaze")
	total := duration(durationByName, "buildTargets")
	targets := duration(durationByName, "evaluateTargetPatterns")
	analysis := duration(durationByName, "runAnalysisPhase")
	building := max(int64(0), total-analysis-targets)
	return sortedNonZeroAggregates([]TimingAggregate{
		{Name: "Launch", DurationUsec: launching, Count: count(durationByName, "Launch Blaze")},
		{Name: "Evaluation", DurationUsec: targets, Count: count(durationByName, "evaluateTargetPatterns")},
		{Name: "Analysis", DurationUsec: analysis, Count: count(durationByName, "runAnalysisPhase")},
		{Name: "Execution", DurationUsec: building, Count: count(durationByName, "buildTargets")},
	})
}

func executionBreakdown(durationByName, durationByCategory map[string]*TimingAggregate) []TimingAggregate {
	localExecution := duration(durationByName, "subprocess.run") + duration(durationByCategory, "local action execution")
	merkleTree := duration(durationByName, "MerkleTreeComputer.buildForSpawn") + duration(durationByName, "MerkleTree.build(ActionInput)")
	return sortedNonZeroAggregates([]TimingAggregate{
		{Name: "Executing locally", DurationUsec: localExecution},
		{Name: "Action dependency checking", DurationUsec: duration(durationByCategory, "action dependency checking")},
		{Name: "Input mapping", DurationUsec: duration(durationByName, "AbstractSpawnStrategy.getInputMapping")},
		{Name: "Merkle tree building", DurationUsec: merkleTree},
		{Name: "Local sandbox creation", DurationUsec: duration(durationByName, "sandbox.createFileSystem")},
		{Name: "Local sandbox teardown", DurationUsec: duration(durationByName, "sandbox.delete")},
		{Name: "Executing remotely", DurationUsec: duration(durationByName, "execute remotely")},
		{Name: "Checking cache hits", DurationUsec: duration(durationByName, "check cache hit")},
		{Name: "Uploading missing inputs", DurationUsec: duration(durationByName, "upload missing inputs")},
		{Name: "Downloading outputs", DurationUsec: duration(durationByCategory, "remote output download")},
		{Name: "Uploading outputs", DurationUsec: duration(durationByName, "upload outputs")},
		{Name: "Detect modified output files", DurationUsec: duration(durationByName, "detectModifiedOutputFiles")},
		{Name: "Generating stable-status.txt", DurationUsec: duration(durationByName, "BazelWorkspaceStatusAction stable-status.txt")},
	})
}

func addDuration(aggregates map[string]*TimingAggregate, name string, durationUsec int64) {
	if name == "" {
		name = "(unknown)"
	}
	aggregate := aggregates[name]
	if aggregate == nil {
		aggregate = &TimingAggregate{Name: name}
		aggregates[name] = aggregate
	}
	aggregate.DurationUsec += durationUsec
	aggregate.Count++
}

func sortedAggregates(aggregates map[string]*TimingAggregate) []TimingAggregate {
	out := make([]TimingAggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		if aggregate.DurationUsec > 0 {
			out = append(out, *aggregate)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].DurationUsec != out[j].DurationUsec {
			return out[i].DurationUsec > out[j].DurationUsec
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func sortedNonZeroAggregates(aggregates []TimingAggregate) []TimingAggregate {
	out := aggregates[:0]
	for _, aggregate := range aggregates {
		if aggregate.DurationUsec > 0 {
			out = append(out, aggregate)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].DurationUsec != out[j].DurationUsec {
			return out[i].DurationUsec > out[j].DurationUsec
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func duration(aggregates map[string]*TimingAggregate, name string) int64 {
	aggregate := aggregates[name]
	if aggregate == nil {
		return 0
	}
	return aggregate.DurationUsec
}

func count(aggregates map[string]*TimingAggregate, name string) int {
	aggregate := aggregates[name]
	if aggregate == nil {
		return 0
	}
	return aggregate.Count
}

func spanKind(span TimingSpan) string {
	if span.Mnemonic != "" {
		return span.Mnemonic
	}
	if fields := strings.Fields(span.Name); len(fields) > 0 {
		return fields[0]
	}
	if span.Category != "" {
		return span.Category
	}
	return "(unknown)"
}

func stringArg(args map[string]any, key string) string {
	value, ok := args[key]
	if !ok || value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprint(value)
}

func normalizeTraceThreadName(name string) string {
	if strings.HasPrefix(name, "skyframe") {
		return strings.NewReplacer("skyframe-evaluator-", "skyframe evaluator ", "skyframe evaluator-", "skyframe evaluator ").Replace(name)
	}
	return name
}

func percent(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return 100 * float64(part) / float64(total)
}

func roundPercent(part, total int64) int {
	return int(math.Round(percent(part, total)))
}
