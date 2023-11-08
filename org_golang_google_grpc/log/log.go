package log

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	LogLevel                = flag.String("grpc.log_level", "info", "The desired log level. Logs with a level >= this level will be emitted. One of {'fatal', 'error', 'warn', 'info', 'debug'}")
	EnableStructuredLogging = flag.Bool("grpc.enable_structured_logging", false, "If true, log messages will be json-formatted.")
	IncludeShortFileName    = flag.Bool("grpc.log_include_short_file_name", false, "If true, log messages will include shortened originating file name.")
)

const (
	ExecutionIDKey  = "execution_id"
	InvocationIDKey = "invocation_id"

	callerSkipFrameCount = 3
)

func init() {
	err := Configure()
	if err != nil {
		fmt.Printf("Error configuring logging: %v", err)
		os.Exit(1) // in case log.Fatalf does not work.
	}
}

func LocalWriter() io.Writer {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	output := &zerolog.ConsoleWriter{Out: os.Stderr}
	output.FormatCaller = func(i interface{}) string {
		s, ok := i.(string)
		if !ok {
			return ""
		}
		// max length based on "content_addressable_storage_server.go".
		// we're not going to have any file names longer than that... right?
		return fmt.Sprintf("%41s >", filepath.Base(s))
	}
	output.TimeFormat = "2006/01/02 15:04:05.000"
	// Skipping 3 frames prints the correct source file + line number, rather
	// than printing a line number in this file or in the zerolog library.
	return output
}

func StructuredWriter() io.Writer {
	// These overrides configure the logger to emit structured
	// events compatible with GCP's logging infrastructure.
	zerolog.LevelFieldName = "severity"
	zerolog.TimestampFieldName = "timestamp"
	zerolog.TimeFieldFormat = time.RFC3339Nano
	return os.Stdout
}

func NewConsoleWriter() io.Writer {
	if *EnableStructuredLogging {
		return StructuredWriter()
	}
	return LocalWriter()
}

func Configure() error {
	writers := []io.Writer{}
	// The ConsoleWriter comes last in the MultiLevelWriter because it writes to
	// its sub-writers in sequence, and consoleWriter will exit after logging when
	// we log fatal errors.
	logger := zerolog.New(zerolog.MultiLevelWriter(append(writers, NewConsoleWriter())...)).With().Timestamp().Logger()
	if l, err := zerolog.ParseLevel(*LogLevel); err != nil {
		return err
	} else {
		logger = logger.Level(l)
	}
	if *IncludeShortFileName {
		logger = logger.With().CallerWithSkipFrameCount(callerSkipFrameCount).Logger()
	}
	log.Logger = logger
	return nil
}

type Logger struct {
	zl zerolog.Logger
}

// Debug logs to the DEBUG log.
func (l *Logger) Debug(message string) {
	l.zl.Debug().Msg(message)
}

// Debugf logs to the DEBUG log. Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.zl.Debug().Msgf(format, args...)
}

// CtxDebugf logs to the DEBUG log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func (l *Logger) CtxDebugf(ctx context.Context, format string, args ...interface{}) {
	e := l.zl.Debug()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
}

// Info logs to the INFO log.
func (l *Logger) Info(message string) {
	l.zl.Info().Msg(message)
}

// Infof logs to the INFO log. Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.zl.Info().Msgf(format, args...)
}

// CtxInfof logs to the INFO log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func (l *Logger) CtxInfof(ctx context.Context, format string, args ...interface{}) {
	e := l.zl.Info()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
}

// Warning logs to the WARNING log.
func (l *Logger) Warning(message string) {
	l.zl.Warn().Msg(message)
}

// Warningf logs to the WARNING log. Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Warningf(format string, args ...interface{}) {
	l.zl.Warn().Msgf(format, args...)
}

// CtxWarningf logs to the WARNING log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func (l *Logger) CtxWarningf(ctx context.Context, format string, args ...interface{}) {
	e := l.zl.Warn()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
}

// Error logs to the ERROR log.
func (l *Logger) Error(message string) {
	l.zl.Error().Msg(message)
}

// Errorf logs to the ERROR log. Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.zl.Error().Msgf(format, args...)
}

// CtxErrorf logs to the ERROR log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func (l *Logger) CtxErrorf(ctx context.Context, format string, args ...interface{}) {
	e := l.zl.Error()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
}

// Fatal logs to the FATAL log. Arguments are handled in the manner of fmt.Print.
// It calls os.Exit() with exit code 1.
func (l *Logger) Fatal(message string) {
	log.Fatal().Msg(message)
	// Make sure fatal logs will exit.
	os.Exit(1)
}

// Fatalf logs to the FATAL log. Arguments are handled in the manner of fmt.Printf.
// It calls os.Exit() with exit code 1.
func (l *Logger) Fatalf(format string, args ...interface{}) {
	log.Fatal().Msgf(format, args...)
	// Make sure fatal logs will exit.
	os.Exit(1)
}

func NamedSubLogger(name string) Logger {
	return Logger{
		zl: log.Logger.With().Str("name", name).Logger(),
	}
}

func enrichEventFromContext(ctx context.Context, e *zerolog.Event) {
	// Not supposed to happen, but let's not panic if it does.
	if ctx == nil {
		return
	}

	if m, ok := ctx.Value(logMetaKey).(*logMeta); ok {
		for m != nil {
			e.Str(m.key, m.value)
			m = m.prev
		}
	}
}

type logMeta struct {
	prev       *logMeta
	key, value string
}

const logMetaKey = "log-meta"

func EnrichContext(ctx context.Context, key, value string) context.Context {
	prev, _ := ctx.Value(logMetaKey).(*logMeta)
	return context.WithValue(ctx, logMetaKey, &logMeta{prev, key, value})
}

// Zerolog convenience wrapper below here:
// DEPRECATED: use log.Info instead!
func Print(message string) {
	log.Info().Msg(message)
}

// DEPRECATED: use log.Infof instead!
func Printf(format string, v ...interface{}) {
	log.Info().Msgf(format, v...)
}

// Debug logs to the DEBUG log.
func Debug(message string) {
	log.Debug().Msg(message)
}

// Debugf logs to the DEBUG log. Arguments are handled in the manner of fmt.Printf.
func Debugf(format string, args ...interface{}) {
	log.Debug().Msgf(format, args...)
}

// CtxDebug logs to the DEBUG log.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxDebug(ctx context.Context, message string) {
	e := log.Debug()
	enrichEventFromContext(ctx, e)
	e.Msg(message)
}

// CtxDebugf logs to the DEBUG log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxDebugf(ctx context.Context, format string, args ...interface{}) {
	e := log.Debug()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
}

// Info logs to the INFO log.
func Info(message string) {
	log.Info().Msg(message)
}

// Infof logs to the INFO log. Arguments are handled in the manner of fmt.Printf.
func Infof(format string, args ...interface{}) {
	log.Info().Msgf(format, args...)
}

// CtxInfo logs to the INFO log.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxInfo(ctx context.Context, message string) {
	e := log.Info()
	enrichEventFromContext(ctx, e)
	e.Msg(message)
}

// CtxInfof logs to the INFO log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxInfof(ctx context.Context, format string, args ...interface{}) {
	e := log.Info()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
}

// Warning logs to the WARNING log.
func Warning(message string) {
	log.Warn().Msg(message)
}

// Warningf logs to the WARNING log. Arguments are handled in the manner of fmt.Printf.
func Warningf(format string, args ...interface{}) {
	log.Warn().Msgf(format, args...)
}

// CtxWarning logs to the WARNING log.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxWarning(ctx context.Context, message string) {
	e := log.Warn()
	enrichEventFromContext(ctx, e)
	e.Msg(message)
}

// CtxWarningf logs to the WARNING log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxWarningf(ctx context.Context, format string, args ...interface{}) {
	e := log.Warn()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
}

// Error logs to the ERROR log.
func Error(message string) {
	log.Error().Msg(message)
}

// Errorf logs to the ERROR log. Arguments are handled in the manner of fmt.Printf.
func Errorf(format string, args ...interface{}) {
	log.Error().Msgf(format, args...)
}

// CtxError logs to the ERROR log.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxError(ctx context.Context, message string) {
	e := log.Error()
	enrichEventFromContext(ctx, e)
	e.Msg(message)
}

// CtxErrorf logs to the ERROR log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxErrorf(ctx context.Context, format string, args ...interface{}) {
	e := log.Error()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
}

// Fatal logs to the FATAL log. Arguments are handled in the manner of fmt.Print.
// It calls os.Exit() with exit code 1.
func Fatal(message string) {
	log.Fatal().Msg(message)
	// Make sure fatal logs will exit.
	os.Exit(1)
}

// Fatalf logs to the FATAL log. Arguments are handled in the manner of fmt.Printf.
// It calls os.Exit() with exit code 1.
func Fatalf(format string, args ...interface{}) {
	log.Fatal().Msgf(format, args...)
	// Make sure fatal logs will exit.
	os.Exit(1)
}

// CtxFatalf logs to the FATAL log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
// It calls os.Exit() with exit code 1.
func CtxFatalf(ctx context.Context, format string, args ...interface{}) {
	e := log.Fatal()
	enrichEventFromContext(ctx, e)
	e.Msgf(format, args...)
	// Make sure fatal logs will exit.
	os.Exit(1)
}

type logWriter struct {
	ctx    context.Context
	prefix string
}

func (w *logWriter) Write(b []byte) (int, error) {
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		CtxInfof(w.ctx, "%s%s", w.prefix, line)
	}
	return len(b), nil
}

// Writer returns a writer that outputs written data to the log with each line
// prepended with the given prefix.
func Writer(prefix string) io.Writer {
	return &logWriter{ctx: context.Background(), prefix: prefix}
}

// CtxWriter returns a writer that outputs written data to the log with each
// line prepended with the given prefix. Logs are enriched with information from
// the context (e.g. invocation_id, request_id).
func CtxWriter(ctx context.Context, prefix string) io.Writer {
	return &logWriter{ctx: ctx, prefix: prefix}
}

const LogTraceHeader = "x-buildbuddy-log-trace-id"

// CtxDebugf logs to the DEBUG log. Arguments are handled in the manner of
// fmt.Printf.
// Logs are enriched with information from the context
// (e.g. invocation_id, request_id)
func CtxTracef(ctx context.Context, format string, args ...interface{}) {
	if _, ok := ctx.Value(LogTraceHeader).(string); ok {
		e := log.Info()
		enrichEventFromContext(ctx, e)
		e.Msgf(format, args...)
	}
}
