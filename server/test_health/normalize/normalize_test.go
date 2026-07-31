package normalize_test

import (
	"testing"

	thpb "github.com/buildbuddy-io/buildbuddy/proto/test_health"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/normalize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func context() normalize.ReportContext {
	return normalize.ReportContext{
		RepositoryURL: "git@github.com:buildbuddy-io/buildbuddy.git",
		InvocationID:  "invocation-1",
		Source:        thpb.ResultSource_RESULT_SOURCE_POSTSUBMIT,
	}
}

func TestNormalizeCaseAndTarget(t *testing.T) {
	report, err := normalize.Normalize(context(), []normalize.CaseRecord{{
		TargetLabel: "//pkg:test", CaseName: "TestA", Outcome: thpb.TestOutcome_TEST_OUTCOME_FAIL,
		DurationUsec: 123, FailureMessage: "got 1, want 2",
	}}, []normalize.TargetRecord{{
		TargetLabel: "//pkg:test", Outcome: thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		DurationUsec: 456,
	}})
	require.NoError(t, err)
	require.Len(t, report.CaseResults, 1)
	require.Len(t, report.TargetResults, 1)
	assert.Equal(t, "https://github.com/buildbuddy-io/buildbuddy", report.Context.RepositoryURL)
	assert.Equal(t, "TestA", report.CaseResults[0].Result.GetIdentity().GetCaseName())
	assert.Equal(t, "//pkg:test", report.CaseResults[0].Result.GetIdentity().GetTarget().GetTargetLabel())
	assert.Equal(t, "invocation-1", report.CaseResults[0].Result.GetInvocationId())
	assert.Equal(t, "got 1, want 2", report.CaseResults[0].Result.GetFailureMessage())
	assert.Equal(t, thpb.TestOutcome_TEST_OUTCOME_TIMEOUT, report.TargetResults[0].Result.GetOutcome())
}

func TestRepeatedResultsArePreserved(t *testing.T) {
	record := normalize.CaseRecord{
		TargetLabel: "//pkg:test", CaseName: "TestA", Outcome: thpb.TestOutcome_TEST_OUTCOME_PASS,
	}
	report, err := normalize.Normalize(context(), []normalize.CaseRecord{record, record}, nil)
	require.NoError(t, err)
	assert.Len(t, report.CaseResults, 2)
	assert.Empty(t, report.Rejections)
}

func TestRepeatedResultsMayHaveDifferentOutcomes(t *testing.T) {
	first := normalize.CaseRecord{TargetLabel: "//pkg:test", CaseName: "TestA", Outcome: thpb.TestOutcome_TEST_OUTCOME_PASS}
	second := first
	second.Outcome = thpb.TestOutcome_TEST_OUTCOME_FAIL
	report, err := normalize.Normalize(context(), []normalize.CaseRecord{first, second}, nil)
	require.NoError(t, err)
	assert.Len(t, report.CaseResults, 2)
	assert.Zero(t, report.Rejected.Cases)
}

func TestRepeatedTargetResultsArePreserved(t *testing.T) {
	first := normalize.TargetRecord{
		TargetLabel: "//pkg:test", Outcome: thpb.TestOutcome_TEST_OUTCOME_PASS,
	}
	second := first
	second.Outcome = thpb.TestOutcome_TEST_OUTCOME_TIMEOUT
	report, err := normalize.Normalize(context(), nil, []normalize.TargetRecord{first, second})
	require.NoError(t, err)
	assert.Len(t, report.TargetResults, 2)
	assert.Zero(t, report.Rejected.Targets)
}

func TestValidation(t *testing.T) {
	_, err := normalize.Normalize(normalize.ReportContext{}, nil, nil)
	assert.Error(t, err)

	report, err := normalize.Normalize(context(), []normalize.CaseRecord{{
		TargetLabel: "//pkg:test", CaseName: "TestA", Outcome: thpb.TestOutcome(99),
	}}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)
}
