package log

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/buildbuddy-io/buildbuddy/server/util/log/gcp"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

var (
	LogLevel                = flag.String("app.log_level", "info", "The desired log level. Logs with a level >= this level will be emitted. One of {'fatal', 'error', 'warn', 'info', 'debug'}")
	EnableStructuredLogging = flag.Bool("app.enable_structured_logging", false, "If true, log messages will be json-formatted.")
	IncludeShortFileName    = flag.Bool("app.log_include_short_file_name", false, "If true, log messages will include shortened originating file name.")
	EnableGCPLoggingFormat  = flag.Bool("app.log_enable_gcp_logging_format", false, "If true, the output structured logs will be compatible with format expected by GCP Logging.")
	LogErrorStackTraces     = flag.Bool("app.log_error_stack_traces", false, "If true, stack traces will be printed for errors that have them.")
)

const (
	ExecutionIDKey  = "execution_id"
	InvocationIDKey = "invocation_id"

	callerSkipFrameCount = 3
)

func formatDuration(dur time.Duration) string {
	switch {
	case dur < time.Millisecond:
		return fmt.Sprintf("%d us", dur.Microseconds())
	case dur < time.Second:
		return fmt.Sprintf("%d ms", dur.Milliseconds())
	case dur < time.Minute:
		return fmt.Sprintf("%2.2f s", dur.Seconds())
	default:
		return fmt.Sprintf("%d ms", dur.Milliseconds())
	}
}

func isExpectedGRPCError(code codes.Code) bool {
	switch code {
	case codes.OK, codes.NotFound, codes.AlreadyExists, codes.Canceled, codes.Unavailable, codes.ResourceExhausted:
		// Common codes we see in normal operation.
		return true
	default:
		// Less common codes.
		return false
	}
}

func fmtErr(err error) string {
	code := gstatus.Code(err)
	if isExpectedGRPCError(code) {
		// Common codes we see in normal operation. Just show the code.
		return code.String()
	} else {
		// Less common codes: show the full error.
		return err.Error()
	}
}

func LogGRPCRequest(ctx context.Context, fullMethod string, dur time.Duration, err error) {
	if log.Logger.GetLevel() > zerolog.DebugLevel {
		return
	}
	// ByteStream and DistributedCache services share some method names.
	// We disambiguate them in the logs by adding a D prefix to DistributedCache methods.
	fullMethod = strings.Replace(fullMethod, "distributed_cache.DistributedCache/", "D", 1)
	shortPath := "/" + path.Base(fullMethod)
	CtxDebugf(ctx, "%s %s %s [%s]", "gRPC", shortPath, fmtErr(err), formatDuration(dur))
	if *LogErrorStackTraces {
		if se, ok := err.(interface {
			StackTrace() status.StackTrace
		}); ok {
			stackBuf := ""
			for _, f := range se.StackTrace() {
				stackBuf += fmt.Sprintf("%+s:%d\n", f, f)
			}
			CtxDebugf(ctx, stackBuf)
		}
	}
}

func LogHTTPRequest(ctx context.Context, url string, dur time.Duration, statusCode int) {
	if log.Logger.GetLevel() > zerolog.InfoLevel {
		return
	}
	CtxDebugf(ctx, "HTTP %q %d %s [%s]", url, statusCode, http.StatusText(statusCode), formatDuration(dur))
}

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

type gcpLoggingCallerHook struct{}

func (h gcpLoggingCallerHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	// +1 to skip the hook frame.
	_, file, line, ok := runtime.Caller(callerSkipFrameCount + 1)
	if !ok {
		return
	}
	sourceLocation := zerolog.Dict().Str("file", filepath.Base(file)).Str("line", strconv.Itoa(line))
	e.Dict(gcp.SourceLocationFieldName, sourceLocation)
}

func Configure() error {
	writers := []io.Writer{}
	if logWriter, err := gcp.NewLogWriter(); err != nil {
		return err
	} else if logWriter != nil {
		writers = append(writers, logWriter)
	}
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
		if *EnableStructuredLogging && *EnableGCPLoggingFormat {
			logger = logger.Hook(gcpLoggingCallerHook{})
		} else {
			logger = logger.With().CallerWithSkipFrameCount(callerSkipFrameCount).Logger()
		}
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

// Info logs to the INFO log.
func (l *Logger) Info(message string) {
	l.zl.Info().Msg(message)
}

// Infof logs to the INFO log. Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.zl.Info().Msgf(format, args...)
}

// Warning logs to the WARNING log.
func (l *Logger) Warning(message string) {
	l.zl.Warn().Msg(message)
}

// Warningf logs to the WARNING log. Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Warningf(format string, args ...interface{}) {
	l.zl.Warn().Msgf(format, args...)
}

// Error logs to the ERROR log.
func (l *Logger) Error(message string) {
	l.zl.Error().Msg(message)
}

// Errorf logs to the ERROR log. Arguments are handled in the manner of fmt.Printf.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.zl.Error().Msgf(format, args...)
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
	prefix string
}

func (w *logWriter) Write(b []byte) (int, error) {
	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		Infof("%s%s", w.prefix, line)
	}
	return len(b), nil
}

// Writer returns a writer that outputs written data to the log with each line
// prepended with the given prefix.
func Writer(prefix string) io.Writer {
	return &logWriter{prefix: prefix}
}
