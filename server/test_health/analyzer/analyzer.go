// Package analyzer classifies a test from recently processed results.
package analyzer

import (
	thpb "github.com/buildbuddy-io/buildbuddy/proto/test_health"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/config"
)

type Sample struct {
	InvocationID string
	Outcome      thpb.TestOutcome
	Source       thpb.ResultSource
}

type Reason string

const (
	ReasonNoEligibleSamples Reason = "no_eligible_samples"
	ReasonFailuresInWindow  Reason = "failures_in_window"
	ReasonTimeoutsInWindow  Reason = "timeouts_in_window"
	ReasonConsecutivePasses Reason = "consecutive_passes"
	ReasonUncertain         Reason = "uncertain"
)

type Evidence struct {
	EligibleSamples     int
	Passes              int
	Failures            int
	Timeouts            int
	Ineligible          int
	ConsecutivePasses   int
	ConsecutiveFailures int
	FirstInvocationID   string
	LastInvocationID    string
}

type Result struct {
	Health   thpb.TestHealth
	Reason   Reason
	Evidence Evidence
}

func Linear(samples []Sample, cfg *thpb.TestAnalyzerConfig) (Result, error) {
	return linear(samples, cfg, false)
}

func LinearTarget(samples []Sample, cfg *thpb.TestAnalyzerConfig) (Result, error) {
	return linear(samples, cfg, true)
}

func linear(samples []Sample, cfg *thpb.TestAnalyzerConfig, target bool) (Result, error) {
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
		result.Health = thpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA
		result.Reason = ReasonNoEligibleSamples
	case !target && evidence.Failures >= int(cfg.GetFailureThreshold()):
		result.Health = thpb.TestHealth_TEST_HEALTH_FLAKY
		result.Reason = ReasonFailuresInWindow
	case target && evidence.Failures-evidence.Timeouts >= int(cfg.GetFailureThreshold()):
		result.Health = thpb.TestHealth_TEST_HEALTH_FLAKY
		result.Reason = ReasonFailuresInWindow
	case target && evidence.Timeouts >= int(cfg.GetTargetTimeoutThreshold()):
		result.Health = thpb.TestHealth_TEST_HEALTH_TIMEOUT
		result.Reason = ReasonTimeoutsInWindow
	case evidence.ConsecutivePasses >= min(3, int(cfg.GetWindowSize())):
		result.Health = thpb.TestHealth_TEST_HEALTH_HEALTHY
		result.Reason = ReasonConsecutivePasses
	default:
		result.Health = thpb.TestHealth_TEST_HEALTH_INSUFFICIENT_DATA
		result.Reason = ReasonUncertain
	}
	return result, nil
}

func Eligible(sample Sample) bool {
	if sample.Source != thpb.ResultSource_RESULT_SOURCE_PRESUBMIT && sample.Source != thpb.ResultSource_RESULT_SOURCE_POSTSUBMIT {
		return false
	}
	switch sample.Outcome {
	case thpb.TestOutcome_TEST_OUTCOME_PASS,
		thpb.TestOutcome_TEST_OUTCOME_FAIL,
		thpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
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
	evidence.FirstInvocationID = samples[0].InvocationID
	evidence.LastInvocationID = samples[len(samples)-1].InvocationID
	for _, sample := range samples {
		switch sample.Outcome {
		case thpb.TestOutcome_TEST_OUTCOME_PASS:
			evidence.Passes++
		case thpb.TestOutcome_TEST_OUTCOME_TIMEOUT:
			evidence.Timeouts++
			evidence.Failures++
		default:
			evidence.Failures++
		}
	}
	for i := len(samples) - 1; i >= 0 && samples[i].Outcome == thpb.TestOutcome_TEST_OUTCOME_PASS; i-- {
		evidence.ConsecutivePasses++
	}
	for i := len(samples) - 1; i >= 0 && samples[i].Outcome != thpb.TestOutcome_TEST_OUTCOME_PASS; i-- {
		evidence.ConsecutiveFailures++
	}
	return evidence
}
