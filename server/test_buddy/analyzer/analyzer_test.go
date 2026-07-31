package analyzer_test

import (
	"testing"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/analyzer"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func samples(outcomes ...tbpb.TestOutcome) []analyzer.Sample {
	out := make([]analyzer.Sample, 0, len(outcomes))
	for _, outcome := range outcomes {
		out = append(out, analyzer.Sample{Outcome: outcome})
	}
	return out
}

func TestLinearUsesProcessingOrder(t *testing.T) {
	result, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, result.Health)
	assert.Equal(t, 2, result.Evidence.Failures)
}

func TestLinearDistinguishesFailingFromFlaky(t *testing.T) {
	failing, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, failing.Health)
	assert.Equal(t, analyzer.ReasonAllFailures, failing.Reason)

	flaky, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, flaky.Health)
}

func TestLinearClassifiesHealthyAndInsufficient(t *testing.T) {
	healthy, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_HEALTHY, healthy.Health)

	insufficient, err := analyzer.Linear(nil, config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, insufficient.Health)
}

func TestTimeoutCountsAsFailure(t *testing.T) {
	result, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, result.Health)
	assert.Equal(t, 2, result.Evidence.Timeouts)
}

func TestTargetRequiresFiveTimeouts(t *testing.T) {
	four, err := analyzer.LinearTarget(samples(
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, four.Health)

	five, err := analyzer.LinearTarget(samples(
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_TIMEOUT, five.Health)
}

func TestTargetFailureUsesFailureThreshold(t *testing.T) {
	result, err := analyzer.LinearTarget(samples(
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, result.Health)
}

func TestUnknownOutcomesAreIgnored(t *testing.T) {
	window := samples(
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
	)
	window[0].Outcome = tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN
	result, err := analyzer.Linear(window, config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, result.Health)
	assert.Equal(t, 1, result.Evidence.Ineligible)
}
