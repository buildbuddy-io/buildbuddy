package explain

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const testTimingProfile = `{
  "traceEvents": [
    {"name":"thread_name","ph":"M","pid":1,"tid":0,"args":{"name":"Critical Path"}},
    {"name":"thread_name","ph":"M","pid":1,"tid":1,"args":{"name":"Main Thread"}},
    {"cat":"build phase marker","name":"buildTargets","ph":"X","ts":0,"dur":3000000,"pid":1,"tid":1},
    {"cat":"build phase marker","name":"evaluateTargetPatterns","ph":"X","ts":100000,"dur":200000,"pid":1,"tid":1},
    {"cat":"build phase marker","name":"runAnalysisPhase","ph":"X","ts":300000,"dur":500000,"pid":1,"tid":1},
    {"cat":"remote action execution","name":"execute remotely","ph":"X","ts":800000,"dur":1000000,"pid":1,"tid":1},
    {"cat":"remote output download","name":"download outputs","ph":"X","ts":1800000,"dur":250000,"pid":1,"tid":1},
    {"cat":"action processing","name":"GoCompilePkg","ph":"X","ts":0,"dur":500000,"pid":1,"tid":0,"args":{"mnemonic":"GoCompilePkg","target":"//foo:lib"},"out":"foo.a"},
    {"cat":"action processing","name":"GoLink","ph":"X","ts":500000,"dur":1200000,"pid":1,"tid":0,"args":{"mnemonic":"GoLink","target":"//foo:bin"},"out":"foo"},
    {"cat":"remote output download","name":"download outputs","ph":"X","ts":1700000,"dur":300000,"pid":1,"tid":0}
  ]
}`

func TestAnalyzeTimingProfileCriticalPath(t *testing.T) {
	report, err := AnalyzeTimingProfile(strings.NewReader(testTimingProfile))
	if err != nil {
		t.Fatalf("AnalyzeTimingProfile() failed: %s", err)
	}

	if !report.CriticalPath.Found {
		t.Fatal("critical path was not found")
	}
	if got, want := report.CriticalPath.DurationUsec, int64(2000000); got != want {
		t.Fatalf("critical path duration = %d; want %d", got, want)
	}
	if got, want := report.ProfileDurationUsec, int64(3000000); got != want {
		t.Fatalf("profile duration = %d; want %d", got, want)
	}
	if got, want := report.CriticalPath.ByMnemonic[0].Name, "GoLink"; got != want {
		t.Fatalf("top critical path mnemonic = %q; want %q", got, want)
	}
	if got, want := report.CriticalPath.LongestSpans[0].Target, "//foo:bin"; got != want {
		t.Fatalf("longest span target = %q; want %q", got, want)
	}

	phaseDurations := aggregateDurations(report.PhaseBreakdown)
	if got, want := phaseDurations["Execution"], int64(2300000); got != want {
		t.Fatalf("execution phase duration = %d; want %d", got, want)
	}
	executionDurations := aggregateDurations(report.ExecutionBreakdown)
	if got, want := executionDurations["Executing remotely"], int64(1000000); got != want {
		t.Fatalf("remote execution duration = %d; want %d", got, want)
	}
	if got, want := executionDurations["Downloading outputs"], int64(550000); got != want {
		t.Fatalf("download duration = %d; want %d", got, want)
	}
}

func TestAnalyzeTimingProfileGzip(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(testTimingProfile)); err != nil {
		t.Fatalf("gzip write failed: %s", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close failed: %s", err)
	}

	report, err := AnalyzeTimingProfile(&buf)
	if err != nil {
		t.Fatalf("AnalyzeTimingProfile(gzip) failed: %s", err)
	}
	if got, want := report.CriticalPath.DurationUsec, int64(2000000); got != want {
		t.Fatalf("critical path duration = %d; want %d", got, want)
	}
}

func TestWriteTimingProfileReport(t *testing.T) {
	report, err := AnalyzeTimingProfile(strings.NewReader(testTimingProfile))
	if err != nil {
		t.Fatalf("AnalyzeTimingProfile() failed: %s", err)
	}
	report.InvocationID = "123"
	report.InvocationDurationUsec = 4000000

	var buf bytes.Buffer
	WriteTimingProfileReport(&buf, report)
	out := buf.String()
	for _, want := range []string{
		"Timing profile for invocation 123",
		"Critical path",
		"GoLink",
		"//foo:bin",
		"Phase breakdown",
		"Execution breakdown",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("report output did not contain %q:\n%s", want, out)
		}
	}
}

func TestFindTimingProfileLogUsesCustomProfilePath(t *testing.T) {
	inv := invocationWithLogs(
		"7.6.0",
		[]*bespb.File{
			{Name: "command.profile.gz", Uri: "bytestream://remote/blobs/abc/1"},
			{Name: "custom.profile.gz", Uri: "bytestream://remote/blobs/def/1"},
		},
	)
	inv.StructuredCommandLine = []*clpb.CommandLine{{
		CommandLineLabel: "canonical",
		Sections: []*clpb.CommandLineSection{{
			SectionLabel: "command options",
			SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{{
				OptionName:  "profile",
				OptionValue: "some\\windows\\path\\custom.profile.gz",
			}}}},
		}},
	}}

	got := findTimingProfileLog(inv)
	if got == nil || got.GetName() != "custom.profile.gz" {
		t.Fatalf("findTimingProfileLog() = %v; want custom.profile.gz", got)
	}
}

func TestFindTimingProfileLogPrefersBazel8CommandProfile(t *testing.T) {
	inv := invocationWithLogs(
		"8.0.0",
		[]*bespb.File{
			{Name: "custom.profile.gz", Uri: "bytestream://remote/blobs/def/1"},
			{Name: "command.profile.123.gz", Uri: "bytestream://remote/blobs/abc/1"},
		},
	)
	inv.StructuredCommandLine = []*clpb.CommandLine{{
		CommandLineLabel: "canonical",
		Sections: []*clpb.CommandLineSection{{
			SectionLabel: "command options",
			SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{{
				OptionName:  "profile",
				OptionValue: "custom.profile.gz",
			}}}},
		}},
	}}

	got := findTimingProfileLog(inv)
	if got == nil || got.GetName() != "command.profile.123.gz" {
		t.Fatalf("findTimingProfileLog() = %v; want command.profile.123.gz", got)
	}
}

func aggregateDurations(aggregates []TimingAggregate) map[string]int64 {
	out := make(map[string]int64)
	for _, aggregate := range aggregates {
		out[aggregate.Name] = aggregate.DurationUsec
	}
	return out
}

func invocationWithLogs(bazelVersion string, logs []*bespb.File) *inpb.Invocation {
	return &inpb.Invocation{
		Event: []*inpb.InvocationEvent{
			{BuildEvent: &bespb.BuildEvent{Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{BuildToolVersion: bazelVersion}}}},
			{BuildEvent: &bespb.BuildEvent{Payload: &bespb.BuildEvent_BuildToolLogs{BuildToolLogs: &bespb.BuildToolLogs{Log: logs}}}},
		},
	}
}
