package analyzer_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/analyzer"
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
)

func samples(outcomes ...tbpb.TestOutcome) []analyzer.Sample {
	out := make([]analyzer.Sample, 0, len(outcomes))
	for _, outcome := range outcomes {
		out = append(out, analyzer.Sample{
			Outcome: outcome,
			Source:  tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR,
		})
	}
	return out
}

func TestLinearUsesProcessingOrder(t *testing.T) {
	cfg := &tbpb.TestAnalyzerConfig{
		WindowSize: 2, FailureCountThreshold: 1, TimeoutCountThreshold: 1,
	}
	result, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
	), cfg)
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_HEALTHY, result.Health)
	assert.Equal(t, 2, result.Evidence.Passes)
}

func TestLinearDistinguishesFailingFromFlaky(t *testing.T) {
	failing, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, failing.Health)
	assert.Equal(t, analyzer.ReasonAllNonPasses, failing.Reason)

	flaky, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, flaky.Health)
}

func TestLinearClassifiesHealthyAndInsufficient(t *testing.T) {
	healthy, err := analyzer.Linear(samples(tbpb.TestOutcome_TEST_OUTCOME_PASS), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_HEALTHY, healthy.Health)
	assert.Equal(t, analyzer.ReasonAllPasses, healthy.Reason)

	insufficient, err := analyzer.Linear(nil, config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, insufficient.Health)
}

func TestLinearRecoversBelowThreshold(t *testing.T) {
	cfg := &tbpb.TestAnalyzerConfig{
		WindowSize: 50, FailureCountThreshold: 2, TimeoutCountThreshold: 5,
	}
	result, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
	), cfg)
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_HEALTHY, result.Health)
	assert.Equal(t, analyzer.ReasonConsecutivePasses, result.Reason)
}

func TestLinearUsesSeparateTimeoutThreshold(t *testing.T) {
	fourTimeouts := samples(
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_PASS,
	)
	result, err := analyzer.Linear(fourTimeouts, config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA, result.Health)

	result, err = analyzer.Linear(append(fourTimeouts, analyzer.Sample{
		Outcome: tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		Source:  tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR,
	}), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FLAKY, result.Health)
	assert.Equal(t, analyzer.ReasonTimeoutsInWindow, result.Reason)
}

func TestLinearClassifiesAllTimeoutsAsFailing(t *testing.T) {
	result, err := analyzer.Linear(samples(
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT,
	), config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_FAILING, result.Health)
	assert.Equal(t, analyzer.ReasonAllNonPasses, result.Reason)
}

func TestLinearIgnoresIneligibleSamples(t *testing.T) {
	window := []analyzer.Sample{
		{Outcome: tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN, Source: tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR},
		{Outcome: tbpb.TestOutcome_TEST_OUTCOME_BROKEN, Source: tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR},
		{Outcome: tbpb.TestOutcome_TEST_OUTCOME_FAIL, Source: tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_UNKNOWN},
		{Outcome: tbpb.TestOutcome_TEST_OUTCOME_PASS, Source: tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR},
	}
	result, err := analyzer.Linear(window, config.Default())
	require.NoError(t, err)
	assert.Equal(t, tbpb.TestHealth_TEST_HEALTH_HEALTHY, result.Health)
	assert.Equal(t, 1, result.Evidence.EligibleSamples)
	assert.Equal(t, 3, result.Evidence.Ineligible)
}
