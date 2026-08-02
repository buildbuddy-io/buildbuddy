package normalize_test

import (
	"testing"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/normalize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const repository = "git@github.com:buildbuddy-io/buildbuddy.git"

func testResult(outcome tbpb.TestOutcome) *tbpb.TestResult {
	return &tbpb.TestResult{
		Outcome: outcome, SourceUrl: "https://app.buildbuddy.io/invocation/one",
		EventTimeUsec: 1_000_000, ResultId: "result-1",
	}
}

func caseResult(outcome tbpb.TestOutcome) *tbpb.TestCaseResult {
	return &tbpb.TestCaseResult{
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, CaseName: "TestA",
		},
		Result: testResult(outcome),
	}
}

func targetResult(outcome tbpb.TestOutcome) *tbpb.TestTargetResult {
	return &tbpb.TestTargetResult{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, Result: testResult(outcome),
	}
}

func TestNormalizeCaseAndTarget(t *testing.T) {
	caseRecord := caseResult(tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	caseRecord.Result.DurationUsec = 123
	caseRecord.Result.FailureMessage = "got 1, want 2"
	targetRecord := targetResult(tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT)
	targetRecord.Result.DurationUsec = 456
	report, err := normalize.Normalize(repository, []*tbpb.TestCaseResult{caseRecord}, []*tbpb.TestTargetResult{targetRecord})
	require.NoError(t, err)
	require.Len(t, report.CaseResults, 1)
	require.Len(t, report.TargetResults, 1)
	assert.Equal(t, "https://github.com/buildbuddy-io/buildbuddy", report.RepositoryURL)
	assert.Equal(t, "TestA", report.CaseResults[0].Result.GetIdentity().GetCaseName())
	assert.Equal(t, "//pkg:test", report.CaseResults[0].Result.GetIdentity().GetTarget().GetTargetLabel())
	assert.Equal(t, "https://app.buildbuddy.io/invocation/one", report.CaseResults[0].Result.GetResult().GetSourceUrl())
	assert.Equal(t, int64(1_000_000), report.CaseResults[0].Result.GetResult().GetEventTimeUsec())
	assert.Equal(t, "result-1", report.CaseResults[0].Result.GetResult().GetResultId())
	assert.Equal(t, "got 1, want 2", report.CaseResults[0].Result.GetResult().GetFailureMessage())
	assert.Equal(t, tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, report.TargetResults[0].Result.GetResult().GetOutcome())
}

func TestRepeatedResultsArePreserved(t *testing.T) {
	record := caseResult(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	report, err := normalize.Normalize(repository, []*tbpb.TestCaseResult{record, record}, nil)
	require.NoError(t, err)
	assert.Len(t, report.CaseResults, 2)
	assert.Empty(t, report.Rejections)
}

func TestRepeatedResultsMayHaveDifferentOutcomes(t *testing.T) {
	report, err := normalize.Normalize(repository, []*tbpb.TestCaseResult{
		caseResult(tbpb.TestOutcome_TEST_OUTCOME_PASS), caseResult(tbpb.TestOutcome_TEST_OUTCOME_FAIL),
	}, nil)
	require.NoError(t, err)
	assert.Len(t, report.CaseResults, 2)
	assert.Zero(t, report.Rejected.Cases)
}

func TestRepeatedTargetResultsArePreserved(t *testing.T) {
	report, err := normalize.Normalize(repository, nil, []*tbpb.TestTargetResult{
		targetResult(tbpb.TestOutcome_TEST_OUTCOME_PASS), targetResult(tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT),
	})
	require.NoError(t, err)
	assert.Len(t, report.TargetResults, 2)
	assert.Zero(t, report.Rejected.Targets)
}

func TestValidation(t *testing.T) {
	_, err := normalize.Normalize("", nil, nil)
	assert.Error(t, err)

	invalid := caseResult(tbpb.TestOutcome(99))
	report, err := normalize.Normalize(repository, []*tbpb.TestCaseResult{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)

	invalid = caseResult(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	invalid.Result.SourceUrl = "not a URL"
	report, err = normalize.Normalize(repository, []*tbpb.TestCaseResult{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)

	invalid = caseResult(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	invalid.Result.ResultId = ""
	report, err = normalize.Normalize(repository, []*tbpb.TestCaseResult{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)

	invalid = caseResult(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	invalid.Result.EventTimeUsec = 0
	report, err = normalize.Normalize(repository, []*tbpb.TestCaseResult{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)
}
