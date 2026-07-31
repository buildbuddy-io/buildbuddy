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
	report := parse(t, `<testsuite name="suite" time="1.5" failures="2">
  <testcase name="pass" classname="pkg.C" time="0.25"/>
  <testcase name="fail" classname="pkg.C"><failure message="got 1, want 2">ignored body</failure></testcase>
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
	assert.False(t, report.UnattributedFailure)

	unattributed := parse(t, `<testsuite errors="1"/>`)
	assert.True(t, unattributed.UnattributedFailure)
}

func TestSubtestNameIsPreserved(t *testing.T) {
	report := parse(t, `<testsuite>
		<testcase name="TestCaseName" classname="go.package"/>
		<testcase name="TestFirecrackerRunSimple/this test has spaces" classname="go.package"/>
	</testsuite>`)
	require.Len(t, report.Cases, 2)
	assert.Equal(t, `TestCaseName`, report.Cases[0].CaseName)
	assert.Equal(t, `TestFirecrackerRunSimple/this test has spaces`, report.Cases[1].CaseName)
	assert.NotContains(t, report.Cases[1].CaseName, "\n")
}

func TestInvalidCasesProduceDiagnostics(t *testing.T) {
	report := parse(t, `<testsuite>
  <testcase/>
  <testcase name="bad-duration" time="-1"/>
  <testcase name="unknown" status="vendor-status"/>
</testsuite>`)
	assert.Equal(t, 3, report.EncounteredCases)
	assert.Len(t, report.Cases, 2)
	assert.Equal(t, []junit.Diagnostic{
		{Code: junit.DiagnosticMissingName, CaseIndex: 0},
		{Code: junit.DiagnosticInvalidDuration, CaseIndex: 1},
		{Code: junit.DiagnosticUnknownStatus, CaseIndex: 2},
	}, report.Diagnostics)
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
