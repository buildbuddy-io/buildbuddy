package config_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
)

func analyzerConfig(window, failures, timeouts int32) *tbpb.TestAnalyzerConfig {
	return &tbpb.TestAnalyzerConfig{
		WindowSize: window, FailureCountThreshold: failures, TimeoutCountThreshold: timeouts,
	}
}

func TestDefault(t *testing.T) {
	cfg := config.Default()
	require.NoError(t, config.Validate(cfg))
	assert.Equal(t, int32(50), cfg.GetWindowSize())
	assert.Equal(t, int32(1), cfg.GetFailureCountThreshold())
	assert.Equal(t, int32(5), cfg.GetTimeoutCountThreshold())
}

func TestValidate(t *testing.T) {
	assert.Error(t, config.Validate(nil))
	assert.NoError(t, config.Validate(analyzerConfig(1, 1, 1)))
	assert.Error(t, config.Validate(analyzerConfig(0, 1, 1)))
	assert.Error(t, config.Validate(analyzerConfig(101, 1, 5)))
	assert.Error(t, config.Validate(analyzerConfig(50, 51, 5)))
	assert.Error(t, config.Validate(analyzerConfig(50, 1, 51)))
}
