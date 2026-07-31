package analyzer_test

import (
	"fmt"
	"testing"

	thpb "github.com/buildbuddy-io/buildbuddy/proto/test_health"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/analyzer"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func samples(outcomes ...thpb.TestOutcome) []analyzer.Sample {
	out := make([]analyzer.Sample, 0, len(outcomes))
	for i, outcome := range outcomes {
		out = append(out, analyzer.Sample{
			InvocationID: fmt.Sprintf("invocation-%d", i),
			Outcome:      outcome,
			Source:       thpb.ResultSource_RESULT_SOURCE_POSTSUBMIT,
		})
	}
	return out
}

func TestLinearUsesProcessingOrder(t *testing.T) {
	result, err := analyzer.Linear(samples(
		thpb.TestOutcome_TEST_OUTCOME_FAIL,
		thpb.TestOutcome_TEST_OUTCOME_PASS,
		thpb.TestOutcome_TEST_OUTCOME_FAIL,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, thpb.TestHealth_TEST_HEALTH_FLAKY, result.Health)
	assert.Equal(t, 2, result.Evidence.Failures)
	assert.Equal(t, "invocation-2", result.Evidence.LastInvocationID)
}

func TestLinearClassifiesHealthyAndInsufficient(t *testing.T) {
	healthy, err := analyzer.Linear(samples(
		thpb.TestOutcome_TEST_OUTCOME_PASS,
		thpb.TestOutcome_TEST_OUTCOME_PASS,
		thpb.TestOutcome_TEST_OUTCOME_PASS,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, thpb.TestHealth_TEST_HEALTH_HEALTHY, healthy.Health)

	insufficient, err := analyzer.Linear(nil, config.Default())
	require.NoError(t, err)
	assert.Equal(t, thpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, insufficient.Health)
}

func TestTimeoutCountsAsFailure(t *testing.T) {
	result, err := analyzer.Linear(samples(
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		thpb.TestOutcome_TEST_OUTCOME_PASS,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, thpb.TestHealth_TEST_HEALTH_FLAKY, result.Health)
	assert.Equal(t, 2, result.Evidence.Timeouts)
}

func TestTargetRequiresFiveTimeouts(t *testing.T) {
	four, err := analyzer.LinearTarget(samples(
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, thpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, four.Health)

	five, err := analyzer.LinearTarget(samples(
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, thpb.TestHealth_TEST_HEALTH_TIMEOUT, five.Health)
}

func TestTargetFailureUsesFailureThreshold(t *testing.T) {
	result, err := analyzer.LinearTarget(samples(
		thpb.TestOutcome_TEST_OUTCOME_FAIL,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, thpb.TestHealth_TEST_HEALTH_FLAKY, result.Health)
}

func TestUnknownAndExcludedSourcesAreIgnored(t *testing.T) {
	window := samples(
		thpb.TestOutcome_TEST_OUTCOME_PASS,
		thpb.TestOutcome_TEST_OUTCOME_PASS,
		thpb.TestOutcome_TEST_OUTCOME_PASS,
	)
	window[0].Outcome = thpb.TestOutcome_TEST_OUTCOME_UNKNOWN
	window[1].Source = thpb.ResultSource_RESULT_SOURCE_UNKNOWN
	result, err := analyzer.Linear(window, config.Default())
	require.NoError(t, err)
	assert.Equal(t, thpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, result.Health)
	assert.Equal(t, 2, result.Evidence.Ineligible)
}
