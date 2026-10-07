package log_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

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

func TestEveryDurationPerKey(t *testing.T) {
	// Capture the output of loggers derived from the global logger.
	buf := &bytes.Buffer{}
	originalLogger := zllog.Logger
	zllog.Logger = zl.New(buf)
	t.Cleanup(func() { zllog.Logger = originalLogger })

	const d = 50 * time.Millisecond
	l := log.NamedSubLogger("test").EveryDurationPerKey(d)
	count := func(msg string) int { return strings.Count(buf.String(), msg) }

	l.ForKey("a").Warningf("msg-a")
	l.ForKey("a").Warningf("msg-a")
	l.ForKey("b").Warningf("msg-b")
	assert.Equal(t, 1, count("msg-a"), "repeat for the same key within the duration is suppressed")
	assert.Equal(t, 1, count("msg-b"), "keys are sampled independently")

	time.Sleep(d + 10*time.Millisecond)
	l.ForKey("a").Warningf("msg-a")
	l.ForKey("a").Warningf("msg-a")
	assert.Equal(t, 2, count("msg-a"), "logs again once the duration has passed")
}
