package normalize_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/normalize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
)

func request(observations ...*tbpb.TestObservation) *tbpb.ReportTestResultsRequest {
	return &tbpb.ReportTestResultsRequest{
		RepoUrl:      "git@github.com:buildbuddy-io/buildbuddy.git",
		Source:       tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR,
		SourceUrl:    "https://app.buildbuddy.io/invocation/one",
		CommitSha:    "abc123",
		Observations: observations,
	}
}

func observation(caseName string, outcome tbpb.TestOutcome) *tbpb.TestObservation {
	return &tbpb.TestObservation{
		Identity: &tbpb.TestIdentity{TargetLabel: "//pkg:test", CaseName: caseName},
		Outcome:  outcome, DurationUsec: 1_000, EventTimeUsec: 1_000_000,
		IdempotencyToken: "observation-1",
	}
}

func TestNormalize(t *testing.T) {
	report, err := normalize.Normalize(request(
		observation("", tbpb.TestOutcome_TEST_OUTCOME_PASS),
		observation("TestA", tbpb.TestOutcome_TEST_OUTCOME_FAIL),
	))
	require.NoError(t, err)
	require.Len(t, report.Observations, 2)
	assert.Equal(t, "https://github.com/buildbuddy-io/buildbuddy", report.RepositoryURL)
	assert.Equal(t, "https://app.buildbuddy.io/invocation/one", report.SourceURL)
	assert.Equal(t, "abc123", report.CommitSHA)
	assert.Equal(t, identity.Address{
		Repository:  "https://github.com/buildbuddy-io/buildbuddy",
		PackagePath: "pkg", TargetName: "test", CaseName: "TestA",
	}, report.Observations[1].Address)
	assert.Equal(t, "//pkg:test", report.Observations[1].Observation.GetIdentity().GetTargetLabel())
}

func TestNormalizeRejectsInvalidObservationsIndependently(t *testing.T) {
	invalid := observation("TestInvalid", tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN)
	invalid.IdempotencyToken = "invalid-token"
	report, err := normalize.Normalize(request(
		observation("TestValid", tbpb.TestOutcome_TEST_OUTCOME_PASS),
		invalid,
	))
	require.NoError(t, err)
	require.Len(t, report.Observations, 1)
	assert.Equal(t, 1, report.RejectedCount)
	require.Len(t, report.Rejections, 1)
	assert.Equal(t, 1, report.Rejections[0].RecordIndex)
	assert.Equal(t, "invalid-token", report.Rejections[0].IdempotencyToken)
	assert.Equal(t, "TestInvalid", report.Rejections[0].Identity.GetCaseName())
}

func TestNormalizeRejectsInvalidReportMetadata(t *testing.T) {
	for _, mutate := range []func(*tbpb.ReportTestResultsRequest){
		func(r *tbpb.ReportTestResultsRequest) { r.RepoUrl = "" },
		func(r *tbpb.ReportTestResultsRequest) {
			r.Source = tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_UNKNOWN
		},
		func(r *tbpb.ReportTestResultsRequest) { r.SourceUrl = "not a URL" },
		func(r *tbpb.ReportTestResultsRequest) { r.CommitSha = "" },
	} {
		report := request(observation("TestA", tbpb.TestOutcome_TEST_OUTCOME_PASS))
		mutate(report)
		_, err := normalize.Normalize(report)
		assert.Error(t, err)
	}
}

func TestNormalizeRejectsInvalidObservationFields(t *testing.T) {
	for _, mutate := range []func(*tbpb.TestObservation){
		func(o *tbpb.TestObservation) { o.Identity.TargetLabel = "relative:test" },
		func(o *tbpb.TestObservation) { o.Outcome = tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN },
		func(o *tbpb.TestObservation) { o.DurationUsec = -1 },
		func(o *tbpb.TestObservation) { o.EventTimeUsec = 0 },
		func(o *tbpb.TestObservation) { o.IdempotencyToken = "" },
		func(o *tbpb.TestObservation) { o.FailureMessage = strings.Repeat("x", 513) },
	} {
		input := observation("TestA", tbpb.TestOutcome_TEST_OUTCOME_PASS)
		mutate(input)
		report, err := normalize.Normalize(request(input))
		require.NoError(t, err)
		assert.Empty(t, report.Observations)
		assert.Equal(t, 1, report.RejectedCount)
	}
}

func TestNormalizeCapsRejectionSamples(t *testing.T) {
	request := request()
	for i := 0; i < 200; i++ {
		request.Observations = append(request.Observations,
			observation("TestInvalid", tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN))
	}
	report, err := normalize.Normalize(request)
	require.NoError(t, err)
	assert.Equal(t, 200, report.RejectedCount)
	assert.Len(t, report.Rejections, 100)
}
