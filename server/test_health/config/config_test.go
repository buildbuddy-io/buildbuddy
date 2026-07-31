package config_test

import (
	"testing"

	thpb "github.com/buildbuddy-io/buildbuddy/proto/test_health"
	"github.com/buildbuddy-io/buildbuddy/server/test_health/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefault(t *testing.T) {
	cfg := config.Default()
	require.NoError(t, config.Validate(cfg))
	assert.Equal(t, int32(50), cfg.GetWindowSize())
	assert.Equal(t, int32(1), cfg.GetFailureThreshold())
	assert.Equal(t, int32(5), cfg.GetTargetTimeoutThreshold())
}

func TestValidate(t *testing.T) {
	assert.Error(t, config.Validate(nil))
	assert.Error(t, config.Validate(&thpb.TestAnalyzerConfig{}))
	assert.Error(t, config.Validate(&thpb.TestAnalyzerConfig{
		WindowSize: 49, FailureThreshold: 1, TargetTimeoutThreshold: 5,
	}))
	assert.Error(t, config.Validate(&thpb.TestAnalyzerConfig{
		WindowSize: 101, FailureThreshold: 1, TargetTimeoutThreshold: 5,
	}))
	assert.Error(t, config.Validate(&thpb.TestAnalyzerConfig{
		WindowSize: 50, FailureThreshold: 51, TargetTimeoutThreshold: 5,
	}))
	assert.Error(t, config.Validate(&thpb.TestAnalyzerConfig{
		WindowSize: 50, FailureThreshold: 1, TargetTimeoutThreshold: 51,
	}))
}
