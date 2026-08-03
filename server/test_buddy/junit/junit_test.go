package junit_test

import (
	"context"
	"strings"
	"testing"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/junit"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, xml string) *junit.Report {
	t.Helper()
	report, err := junit.Parse(context.Background(), strings.NewReader(xml), junit.Options{TargetLabel: "//pkg:test"})
	require.NoError(t, err)
	return report
}

func TestParse(t *testing.T) {
	report := parse(t, `<testsuite name="suite" time="1.5" failures="2" timestamp="2026-07-30T12:00:00Z">
  <testcase name="pass" classname="pkg.C" time="0.25"/>
  <testcase name="fail" classname="pkg.C" timestamp="2026-07-30T12:00:01.123456Z"><failure message="got 1, want 2">ignored body</failure></testcase>
  <testcase name="error" classname="pkg.C"><error message="panic"/></testcase>
  <testcase name="timeout" classname="pkg.C" status="timed_out"/>
  <testcase name="skip" classname="pkg.C"><skipped/></testcase>
</testsuite>`)
	require.Len(t, report.Cases, 5)
	assert.Equal(t, []tbpb.TestOutcome{
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN,
	}, []tbpb.TestOutcome{
		report.Cases[0].Outcome, report.Cases[1].Outcome, report.Cases[2].Outcome,
		report.Cases[3].Outcome, report.Cases[4].Outcome,
	})
	assert.Equal(t, `pass`, report.Cases[0].CaseName)
	assert.Equal(t, int64(250_000), report.Cases[0].DurationUsec)
	assert.Equal(t, "got 1, want 2", report.Cases[1].FailureMessage)
	assert.Equal(t, "panic", report.Cases[2].FailureMessage)
	assert.Equal(t, int64(1_500_000), report.DurationUsec)
	assert.Equal(t, int64(1_785_412_800_000_000), report.EventTimeUsec)
	assert.Equal(t, report.EventTimeUsec, report.Cases[0].EventTimeUsec)
	assert.Equal(t, int64(1_785_412_801_123_456), report.Cases[1].EventTimeUsec)
	assert.Equal(t, 0, report.Cases[0].OccurrenceIndex)
	assert.Equal(t, 1, report.Cases[1].OccurrenceIndex)
	assert.False(t, report.UnattributedFailure)

	unattributed := parse(t, `<testsuite errors="1"/>`)
	assert.True(t, unattributed.UnattributedFailure)
}

func TestSubtestNameIsPreserved(t *testing.T) {
	report := parse(t, `<testsuite>
		<testcase name="TestCaseName" classname="go.package"/>
		<testcase name="TestFirecrackerRunSimple/this test has spaces" classname="go.package"/>
		<testcase name="TestTruncateStringSlice/[ツ]/1" classname="go.package"/>
	</testsuite>`)
	require.Len(t, report.Cases, 3)
	assert.Equal(t, `TestCaseName`, report.Cases[0].CaseName)
	assert.Equal(t, `TestFirecrackerRunSimple/this test has spaces`, report.Cases[1].CaseName)
	assert.Equal(t, `TestTruncateStringSlice/[ツ]/1`, report.Cases[2].CaseName)
	assert.NotContains(t, report.Cases[1].CaseName, "\n")
}

func TestInvalidCasesProduceDiagnostics(t *testing.T) {
	report := parse(t, `<testsuite>
  <testcase/>
  <testcase name="bad-duration" time="-1"/>
  <testcase name="unknown" status="vendor-status" timestamp="not-a-time"/>
</testsuite>`)
	assert.Equal(t, 3, report.EncounteredCases)
	assert.Len(t, report.Cases, 2)
	// A diagnostic carries the case name when there is one, so a report can say
	// which test it is about rather than only how many were affected.
	assert.Equal(t, []junit.Diagnostic{
		{Code: junit.DiagnosticMissingName, CaseIndex: 0},
		{Code: junit.DiagnosticInvalidDuration, CaseIndex: 1, CaseName: "bad-duration"},
		{Code: junit.DiagnosticUnknownStatus, CaseIndex: 2, CaseName: "unknown"},
		{Code: junit.DiagnosticInvalidTimestamp, CaseIndex: 2, CaseName: "unknown"},
	}, report.Diagnostics)
	// The first case was dropped; the other two reported with a field ignored.
	assert.True(t, report.Diagnostics[0].Code.DropsCase())
	for _, diagnostic := range report.Diagnostics[1:] {
		assert.False(t, diagnostic.Code.DropsCase(), diagnostic.Code)
	}
}

func TestUnusableCaseNameIsRetainedForDiagnosis(t *testing.T) {
	report := parse(t, "<testsuite>\n  <testcase name=\"bad\tname\"/>\n</testsuite>")
	assert.Empty(t, report.Cases)
	require.Len(t, report.Diagnostics, 1)
	// The name cannot be an address, but it is what a human will search for.
	assert.Equal(t, junit.DiagnosticInvalidIdentity, report.Diagnostics[0].Code)
	assert.Equal(t, "bad\tname", report.Diagnostics[0].CaseName)
}

func TestNestedSuiteTimestampIsInherited(t *testing.T) {
	report := parse(t, `<testsuites>
  <testsuite name="outer" timestamp="2026-07-30T12:00:00">
    <testsuite name="inner" timestamp="2026-07-30T12:01:00Z">
      <testcase name="inner-case"/>
    </testsuite>
    <testcase name="outer-case"/>
  </testsuite>
</testsuites>`)
	require.Len(t, report.Cases, 2)
	assert.Equal(t, int64(1_785_412_800_000_000), report.EventTimeUsec)
	assert.Equal(t, int64(1_785_412_860_000_000), report.Cases[0].EventTimeUsec)
	assert.Equal(t, int64(1_785_412_800_000_000), report.Cases[1].EventTimeUsec)
}

func TestLimitsAndMalformedXML(t *testing.T) {
	_, err := junit.Parse(context.Background(), strings.NewReader(`<testsuite><testcase name="one"/><testcase name="two"/></testsuite>`), junit.Options{
		TargetLabel: "//pkg:test", Limits: junit.Limits{MaxCases: 1},
	})
	assert.True(t, status.IsResourceExhaustedError(err), err)

	_, err = junit.Parse(context.Background(), strings.NewReader(`<testsuite><testcase name="unterminated">`), junit.Options{TargetLabel: "//pkg:test"})
	assert.True(t, status.IsInvalidArgumentError(err), err)
}

func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := junit.Parse(ctx, strings.NewReader(`<testsuite/>`), junit.Options{TargetLabel: "//pkg:test"})
	assert.Error(t, err)
}
