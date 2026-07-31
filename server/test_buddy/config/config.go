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
		WindowSize:             50,
		FailureThreshold:       1,
		TargetTimeoutThreshold: 5,
	}
}

func Validate(cfg *tbpb.TestAnalyzerConfig) error {
	if cfg == nil {
		return status.InvalidArgumentError("analyzer configuration is required")
	}
	if cfg.GetWindowSize() < minWindowSize || cfg.GetWindowSize() > maxWindowSize {
		return status.InvalidArgumentErrorf("window_size must be between %d and %d", minWindowSize, maxWindowSize)
	}
	if cfg.GetFailureThreshold() <= 0 || cfg.GetFailureThreshold() > cfg.GetWindowSize() {
		return status.InvalidArgumentError("failure_threshold must be between 1 and window_size")
	}
	if cfg.GetTargetTimeoutThreshold() <= 0 || cfg.GetTargetTimeoutThreshold() > cfg.GetWindowSize() {
		return status.InvalidArgumentError("target_timeout_threshold must be between 1 and window_size")
	}
	return nil
}
