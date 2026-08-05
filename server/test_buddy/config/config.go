// Package config defines TestBuddy analyzer configuration.
package config

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
)

const (
	minWindowSize = 1
	maxWindowSize = 100
)

func Default() *tbpb.TestAnalyzerConfig {
	return &tbpb.TestAnalyzerConfig{
		WindowSize:            50,
		FailureCountThreshold: 1,
		TimeoutCountThreshold: 5,
	}
}

func Validate(cfg *tbpb.TestAnalyzerConfig) error {
	if cfg == nil {
		return status.InvalidArgumentError("analyzer configuration is required")
	}
	if cfg.GetWindowSize() < minWindowSize || cfg.GetWindowSize() > maxWindowSize {
		return status.InvalidArgumentErrorf("window_size must be between %d and %d", minWindowSize, maxWindowSize)
	}
	if cfg.GetFailureCountThreshold() <= 0 || cfg.GetFailureCountThreshold() > cfg.GetWindowSize() {
		return status.InvalidArgumentError("failure_count_threshold must be between 1 and window_size")
	}
	if cfg.GetTimeoutCountThreshold() <= 0 || cfg.GetTimeoutCountThreshold() > cfg.GetWindowSize() {
		return status.InvalidArgumentError("timeout_count_threshold must be between 1 and window_size")
	}
	return nil
}
