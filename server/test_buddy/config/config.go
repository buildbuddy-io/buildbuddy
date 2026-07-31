// Package config defines TestBuddy analyzer configuration.
package config

import (
	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	minWindowSize = 50
	maxWindowSize = 100
)

func Default() *tbpb.TestAnalyzerConfig {
	return &tbpb.TestAnalyzerConfig{
		Analyzer: &tbpb.TestAnalyzerConfig_Linear{Linear: &tbpb.LinearAnalyzer{
			WindowSize:             50,
			FailureThreshold:       1,
			TargetTimeoutThreshold: 5,
		}},
	}
}

func Validate(cfg *tbpb.TestAnalyzerConfig) error {
	if cfg == nil {
		return status.InvalidArgumentError("analyzer configuration is required")
	}
	linear := cfg.GetLinear()
	if linear == nil {
		return status.InvalidArgumentError("linear analyzer configuration is required")
	}
	if linear.GetWindowSize() < minWindowSize || linear.GetWindowSize() > maxWindowSize {
		return status.InvalidArgumentErrorf("window_size must be between %d and %d", minWindowSize, maxWindowSize)
	}
	if linear.GetFailureThreshold() <= 0 || linear.GetFailureThreshold() > linear.GetWindowSize() {
		return status.InvalidArgumentError("failure_threshold must be between 1 and window_size")
	}
	if linear.GetTargetTimeoutThreshold() <= 0 || linear.GetTargetTimeoutThreshold() > linear.GetWindowSize() {
		return status.InvalidArgumentError("target_timeout_threshold must be between 1 and window_size")
	}
	return nil
}
