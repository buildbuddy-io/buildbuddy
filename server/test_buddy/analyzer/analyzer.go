// Package analyzer classifies a test from recently processed observations.
package analyzer

import (
	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/config"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
)

type Sample struct {
	Outcome tbpb.TestOutcome
	Source  tbpb.TestObservationSource
}

type Reason string

const (
	ReasonNoEligibleSamples Reason = "no_eligible_samples"
	ReasonAllNonPasses      Reason = "all_non_passes"
	ReasonFailuresInWindow  Reason = "failures_in_window"
	ReasonTimeoutsInWindow  Reason = "timeouts_in_window"
	ReasonAllPasses         Reason = "all_passes"
	ReasonConsecutivePasses Reason = "consecutive_passes"
	ReasonUncertain         Reason = "uncertain"
)

type Evidence struct {
	EligibleSamples   int
	Passes            int
	Failures          int
	Timeouts          int
	Ineligible        int
	ConsecutivePasses int
}

type Result struct {
	Health   tbpb.TestHealth
	Reason   Reason
	Evidence Evidence
}

func Linear(samples []Sample, cfg *tbpb.TestAnalyzerConfig) (Result, error) {
	if err := config.Validate(cfg); err != nil {
		return Result{}, err
	}
	eligible := make([]Sample, 0, len(samples))
	ineligible := 0
	for _, sample := range samples {
		if Eligible(sample) {
			eligible = append(eligible, sample)
		} else {
			ineligible++
		}
	}
	if extra := len(eligible) - int(cfg.GetWindowSize()); extra > 0 {
		eligible = eligible[extra:]
	}
	evidence := summarize(eligible)
	evidence.Ineligible = ineligible
	result := Result{Evidence: evidence}
	switch {
	case len(eligible) == 0:
		result.Health = tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA
		result.Reason = ReasonNoEligibleSamples
	case evidence.Failures+evidence.Timeouts == len(eligible):
		result.Health = tbpb.TestHealth_TEST_HEALTH_FAILING
		result.Reason = ReasonAllNonPasses
	case evidence.Failures >= int(cfg.GetFailureCountThreshold()):
		result.Health = tbpb.TestHealth_TEST_HEALTH_FLAKY
		result.Reason = ReasonFailuresInWindow
	case evidence.Timeouts >= int(cfg.GetTimeoutCountThreshold()):
		result.Health = tbpb.TestHealth_TEST_HEALTH_FLAKY
		result.Reason = ReasonTimeoutsInWindow
	case evidence.Failures+evidence.Timeouts == 0:
		result.Health = tbpb.TestHealth_TEST_HEALTH_HEALTHY
		result.Reason = ReasonAllPasses
	case evidence.ConsecutivePasses >= min(3, int(cfg.GetWindowSize())):
		result.Health = tbpb.TestHealth_TEST_HEALTH_HEALTHY
		result.Reason = ReasonConsecutivePasses
	default:
		result.Health = tbpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA
		result.Reason = ReasonUncertain
	}
	return result, nil
}

func Eligible(sample Sample) bool {
	if sample.Source != tbpb.TestObservationSource_TEST_OBSERVATION_SOURCE_MONITOR {
		return false
	}
	switch sample.Outcome {
	case tbpb.TestOutcome_TEST_OUTCOME_PASS,
		tbpb.TestOutcome_TEST_OUTCOME_FAIL,
		tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
		return true
	default:
		return false
	}
}

func summarize(samples []Sample) Evidence {
	evidence := Evidence{EligibleSamples: len(samples)}
	if len(samples) == 0 {
		return evidence
	}
	for _, sample := range samples {
		switch sample.Outcome {
		case tbpb.TestOutcome_TEST_OUTCOME_PASS:
			evidence.Passes++
		case tbpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
			evidence.Timeouts++
		case tbpb.TestOutcome_TEST_OUTCOME_FAIL:
			evidence.Failures++
		case tbpb.TestOutcome_TEST_OUTCOME_UNKNOWN,
			tbpb.TestOutcome_TEST_OUTCOME_BROKEN:
			continue
		}
	}
	for i := len(samples) - 1; i >= 0 && samples[i].Outcome == tbpb.TestOutcome_TEST_OUTCOME_PASS; i-- {
		evidence.ConsecutivePasses++
	}
	return evidence
}
