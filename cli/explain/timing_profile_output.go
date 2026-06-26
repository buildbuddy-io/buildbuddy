package explain

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

const (
	maxCriticalPathAggregates = 8
	maxCriticalPathSpans      = 10
	maxBreakdownRows          = 8
)

func WriteTimingProfileReport(w io.Writer, report *TimingProfileReport) {
	totalUsec := report.InvocationDurationUsec
	totalLabel := "invocation"
	if totalUsec <= 0 {
		totalUsec = report.ProfileDurationUsec
		totalLabel = "profile"
	}

	if report.InvocationID != "" {
		fmt.Fprintf(w, "Timing profile for invocation %s\n\n", report.InvocationID)
	}
	fmt.Fprintf(w, "Profile events: %d (%d duration spans)\n", report.EventCount, report.SpanCount)
	if totalUsec > 0 {
		fmt.Fprintf(w, "Build duration: %s (%s)\n", formatUsec(totalUsec), totalLabel)
	}
	if report.ProfileDurationUsec > 0 && report.ProfileDurationUsec != totalUsec {
		fmt.Fprintf(w, "Profile duration: %s\n", formatUsec(report.ProfileDurationUsec))
	}
	fmt.Fprintln(w)

	writeCriticalPathSummary(w, report.CriticalPath, totalUsec)
	writeAggregateSection(w, "Phase breakdown", report.PhaseBreakdown, totalUsec, maxBreakdownRows)
	writeAggregateSection(w, "Execution breakdown", report.ExecutionBreakdown, totalUsec, maxBreakdownRows)
}

func writeCriticalPathSummary(w io.Writer, summary CriticalPathSummary, totalUsec int64) {
	fmt.Fprintln(w, "Critical path")
	if !summary.Found {
		fmt.Fprintln(w, "  No Critical Path thread was found in this timing profile.")
		fmt.Fprintln(w)
		return
	}

	if totalUsec > 0 {
		fmt.Fprintf(w, "  Duration: %s (%d%% of build duration, %d spans)\n", formatUsec(summary.DurationUsec), roundPercent(summary.DurationUsec, totalUsec), summary.SpanCount)
	} else {
		fmt.Fprintf(w, "  Duration: %s (%d spans)\n", formatUsec(summary.DurationUsec), summary.SpanCount)
	}
	if len(summary.ByMnemonic) > 0 {
		top := summary.ByMnemonic[0]
		fmt.Fprintf(w, "  Dominant work: %s (%s, %d%% of critical path)\n", top.Name, formatUsec(top.DurationUsec), roundPercent(top.DurationUsec, summary.DurationUsec))
	}
	fmt.Fprintln(w)

	writeAggregateSection(w, "Top critical path mnemonics", summary.ByMnemonic, summary.DurationUsec, maxCriticalPathAggregates)
	writeSpanSection(w, "Longest critical path spans", summary.LongestSpans, summary.DurationUsec, maxCriticalPathSpans)
}

func writeAggregateSection(w io.Writer, title string, aggregates []TimingAggregate, totalUsec int64, limit int) {
	if len(aggregates) == 0 {
		return
	}
	fmt.Fprintln(w, title)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  Duration\tPct\tCount\tName")
	for _, aggregate := range limitAggregates(aggregates, limit) {
		fmt.Fprintf(tw, "  %s\t%s\t%d\t%s\n", formatUsec(aggregate.DurationUsec), formatPercent(aggregate.DurationUsec, totalUsec), aggregate.Count, aggregate.Name)
	}
	_ = tw.Flush()
	fmt.Fprintln(w)
}

func writeSpanSection(w io.Writer, title string, spans []TimingSpan, totalUsec int64, limit int) {
	if len(spans) == 0 {
		return
	}
	fmt.Fprintln(w, title)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  Duration\tPct\tMnemonic\tSpan")
	for _, span := range limitSpans(spans, limit) {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", formatUsec(span.DurationUsec), formatPercent(span.DurationUsec, totalUsec), spanKind(span), spanDescription(span))
	}
	_ = tw.Flush()
	fmt.Fprintln(w)
}

func limitAggregates(aggregates []TimingAggregate, limit int) []TimingAggregate {
	if limit <= 0 || len(aggregates) <= limit {
		return aggregates
	}
	return aggregates[:limit]
}

func limitSpans(spans []TimingSpan, limit int) []TimingSpan {
	if limit <= 0 || len(spans) <= limit {
		return spans
	}
	return spans[:limit]
}

func spanDescription(span TimingSpan) string {
	parts := []string{span.Name}
	if span.Target != "" {
		parts = append(parts, span.Target)
	} else if span.Output != "" {
		parts = append(parts, span.Output)
	}
	return strings.Join(parts, " ")
}

func formatPercent(part, total int64) string {
	if total <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", percent(part, total))
}

func formatUsec(usec int64) string {
	if usec <= 0 {
		return "0s"
	}
	d := time.Duration(usec) * time.Microsecond
	switch {
	case d < time.Millisecond:
		return d.String()
	case d < time.Second:
		return d.Round(time.Microsecond).String()
	default:
		return d.Round(time.Millisecond).String()
	}
}
