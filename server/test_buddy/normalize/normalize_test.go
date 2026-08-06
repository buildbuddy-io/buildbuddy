package normalize_test

import (
	"testing"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/normalize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const repository = "git@github.com:buildbuddy-io/buildbuddy.git"

func testObservation(outcome tbpb.TestOutcome) *tbpb.TestObservation {
	return &tbpb.TestObservation{
		Outcome: outcome, SourceUrl: "https://app.buildbuddy.io/invocation/one",
		EventTimeUsec: 1_000_000, ObservationId: "observation-1",
		Source:    tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR,
		CommitSha: "abc123",
	}
}

func caseObservation(outcome tbpb.TestOutcome) *tbpb.TestCaseObservation {
	return &tbpb.TestCaseObservation{
		Identity: &tbpb.TestCaseIdentity{
			Target: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, CaseName: "TestA",
		},
		Observation: testObservation(outcome),
	}
}

func targetObservation(outcome tbpb.TestOutcome) *tbpb.TestTargetObservation {
	return &tbpb.TestTargetObservation{
		Identity: &tbpb.TestTargetIdentity{TargetLabel: "//pkg:test"}, Observation: testObservation(outcome),
	}
}

func TestNormalizeCaseAndTarget(t *testing.T) {
	caseRecord := caseObservation(tbpb.TestOutcome_TEST_OUTCOME_FAIL)
	caseRecord.Observation.DurationUsec = 123
	caseRecord.Observation.FailureMessage = "got 1, want 2"
	targetRecord := targetObservation(tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT)
	targetRecord.Observation.DurationUsec = 456
	report, err := normalize.Normalize(repository, []*tbpb.TestCaseObservation{caseRecord}, []*tbpb.TestTargetObservation{targetRecord})
	require.NoError(t, err)
	require.Len(t, report.CaseObservations, 1)
	require.Len(t, report.TargetObservations, 1)
	assert.Equal(t, "https://github.com/buildbuddy-io/buildbuddy", report.RepositoryURL)
	assert.Equal(t, "TestA", report.CaseObservations[0].Observation.GetIdentity().GetCaseName())
	assert.Equal(t, "//pkg:test", report.CaseObservations[0].Observation.GetIdentity().GetTarget().GetTargetLabel())
	assert.Equal(t, "https://app.buildbuddy.io/invocation/one", report.CaseObservations[0].Observation.GetObservation().GetSourceUrl())
	assert.Equal(t, int64(1_000_000), report.CaseObservations[0].Observation.GetObservation().GetEventTimeUsec())
	assert.Equal(t, "observation-1", report.CaseObservations[0].Observation.GetObservation().GetObservationId())
	assert.Equal(t, tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR,
		report.CaseObservations[0].Observation.GetObservation().GetSource())
	assert.Equal(t, "abc123", report.CaseObservations[0].Observation.GetObservation().GetCommitSha())
	assert.Equal(t, "got 1, want 2", report.CaseObservations[0].Observation.GetObservation().GetFailureMessage())
	assert.Equal(t, tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT, report.TargetObservations[0].Observation.GetObservation().GetOutcome())
}

func TestRepeatedObservationsArePreserved(t *testing.T) {
	record := caseObservation(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	report, err := normalize.Normalize(repository, []*tbpb.TestCaseObservation{record, record}, nil)
	require.NoError(t, err)
	assert.Len(t, report.CaseObservations, 2)
	assert.Empty(t, report.Rejections)
}

func TestRepeatedObservationsMayHaveDifferentOutcomes(t *testing.T) {
	report, err := normalize.Normalize(repository, []*tbpb.TestCaseObservation{
		caseObservation(tbpb.TestOutcome_TEST_OUTCOME_PASS), caseObservation(tbpb.TestOutcome_TEST_OUTCOME_FAIL),
	}, nil)
	require.NoError(t, err)
	assert.Len(t, report.CaseObservations, 2)
	assert.Zero(t, report.Rejected.Cases)
}

func TestRepeatedTargetObservationsArePreserved(t *testing.T) {
	report, err := normalize.Normalize(repository, nil, []*tbpb.TestTargetObservation{
		targetObservation(tbpb.TestOutcome_TEST_OUTCOME_PASS), targetObservation(tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT),
	})
	require.NoError(t, err)
	assert.Len(t, report.TargetObservations, 2)
	assert.Zero(t, report.Rejected.Targets)
}

func TestValidation(t *testing.T) {
	_, err := normalize.Normalize("", nil, nil)
	assert.Error(t, err)

	invalid := caseObservation(tbpb.TestOutcome(99))
	report, err := normalize.Normalize(repository, []*tbpb.TestCaseObservation{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)

	invalid = caseObservation(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	invalid.Observation.Source = tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_UNKNOWN
	report, err = normalize.Normalize(repository, []*tbpb.TestCaseObservation{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)

	invalid = caseObservation(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	invalid.Observation.CommitSha = ""
	report, err = normalize.Normalize(repository, []*tbpb.TestCaseObservation{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)

	invalid = caseObservation(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	invalid.Observation.SourceUrl = "not a URL"
	report, err = normalize.Normalize(repository, []*tbpb.TestCaseObservation{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)

	invalid = caseObservation(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	invalid.Observation.ObservationId = ""
	report, err = normalize.Normalize(repository, []*tbpb.TestCaseObservation{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)

	invalid = caseObservation(tbpb.TestOutcome_TEST_OUTCOME_PASS)
	invalid.Observation.EventTimeUsec = 0
	report, err = normalize.Normalize(repository, []*tbpb.TestCaseObservation{invalid}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, report.Rejected.Cases)
}
