package log_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"

	zl "github.com/rs/zerolog"
	zllog "github.com/rs/zerolog/log"
)

func TestConfigureLevel(t *testing.T) {
	flags.Set(t, "app.enable_structured_logging", false)
	flags.Set(t, "app.log_include_short_file_name", false)
	flags.Set(t, "app.log_enable_gcp_logging_format", false)
	flags.Set(t, "app.log_error_stack_traces", false)

	flags.Set(t, "app.log_level", "trace")
	log.Configure()
	assert.Equal(t, zl.TraceLevel, zllog.Logger.GetLevel())

	flags.Set(t, "app.log_level", "debug")
	log.Configure()
	assert.Equal(t, zl.DebugLevel, zllog.Logger.GetLevel())

	flags.Set(t, "app.log_level", "info")
	log.Configure()
	assert.Equal(t, zl.InfoLevel, zllog.Logger.GetLevel())

	flags.Set(t, "app.log_level", "warn")
	log.Configure()
	assert.Equal(t, zl.WarnLevel, zllog.Logger.GetLevel())

	flags.Set(t, "app.log_level", "error")
	log.Configure()
	assert.Equal(t, zl.ErrorLevel, zllog.Logger.GetLevel())

	flags.Set(t, "app.log_level", "fatal")
	log.Configure()
	assert.Equal(t, zl.FatalLevel, zllog.Logger.GetLevel())

	flags.Set(t, "app.log_level", "panic")
	log.Configure()
	assert.Equal(t, zl.PanicLevel, zllog.Logger.GetLevel())
}

func TestConfigureDisableStructuredLogging(t *testing.T) {
	flags.Set(t, "app.enable_structured_logging", false)
	log.Configure()
	// Check that Configure left these vars untouched
	assert.Equal(t, zl.LevelFieldName, "level")
	assert.Equal(t, zl.TimestampFieldName, "time")
}

func TestConfigureEnableStructuredLogging(t *testing.T) {
	flags.Set(t, "app.enable_structured_logging", true)
	log.Configure()
	// Check that Configure changed these vars to conform with GCP logging.
	assert.Equal(t, zl.LevelFieldName, "severity")
	assert.Equal(t, zl.TimestampFieldName, "timestamp")
}
