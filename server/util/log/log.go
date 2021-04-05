package log

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

func fmtErr(err error) string {
	code := status.Code(err)
	if code == codes.Unknown {
		return err.Error()
	}
	return code.String()
}

func getRequestMetadata(ctx context.Context) *repb.RequestMetadata {
	if grpcMD, ok := metadata.FromIncomingContext(ctx); ok {
		rmdVals := grpcMD["build.bazel.remote.execution.v2.requestmetadata-bin"]
		for _, rmdVal := range rmdVals {
			rmd := &repb.RequestMetadata{}
			if err := proto.Unmarshal([]byte(rmdVal), rmd); err == nil {
				return rmd
			}
		}
	}
	return nil
}

func getInvocationIDFromMD(ctx context.Context) string {
	iid := ""
	if rmd := getRequestMetadata(ctx); rmd != nil {
		iid = rmd.GetToolInvocationId()
	}
	return iid
}

func LogGRPCRequest(ctx context.Context, fullMethod string, dur time.Duration, err error) {
	reqID, _ := uuid.GetFromContext(ctx) // Ignore error, we're logging anyway.
	shortPath := "/" + path.Base(fullMethod)
	if iid := getInvocationIDFromMD(ctx); iid != "" {
		Printf("%s %s %s %s %s [%s]", "gRPC", reqID, iid, shortPath, fmtErr(err), formatDuration(dur))
	} else {
		Printf("%s %s %s %s [%s]", "gRPC", reqID, shortPath, fmtErr(err), formatDuration(dur))
	}
}

func LogHTTPRequest(ctx context.Context, url string, dur time.Duration, statusCode int) {
	reqID, _ := uuid.GetFromContext(ctx) // Ignore error, we're logging anyway.
	Printf("HTTP %s %q %d %s [%s]", reqID, url, statusCode, http.StatusText(statusCode), formatDuration(dur))
}

func init() {
	// Start us in a "nice" configuration, in case any logging
	// is done before Configure is called.
	Configure("info", false, false)
}

func LocalLogger(level string) zerolog.Logger {
	output := zerolog.ConsoleWriter{Out: os.Stdout}
	output.FormatCaller = func(i interface{}) string {
		s, ok := i.(string)
		if !ok {
			return ""
		}
		// max length based on "content_addressable_storage_server.go".
		// we're not going to have any file names longer than that... right?
		return fmt.Sprintf("%41s >", filepath.Base(s))
	}
	zerolog.TimeFieldFormat = time.RFC3339Nano
	output.TimeFormat = "2006/01/02 15:04:05.000"
	// Skipping 3 frames prints the correct source file + line number, rather
	// than printing a line number in this file or in the zerolog library.
	return zerolog.New(output).With().Timestamp().Logger()
}

func StructuredLogger() zerolog.Logger {
	// These overrides configure the logger to emit structured
	// events compatible with GCP's logging infrastructure.
	zerolog.LevelFieldName = "severity"
	zerolog.TimestampFieldName = "timestamp"
	zerolog.TimeFieldFormat = time.RFC3339Nano
	return log.Logger
}

func Configure(level string, enableFileName, enableStructured bool) error {
	var logger zerolog.Logger
	if enableStructured {
		logger = StructuredLogger()
	} else {
		logger = LocalLogger(level)
	}
	intLogLevel := zerolog.InfoLevel
	if level != "" {
		if l, err := zerolog.ParseLevel(level); err == nil {
			intLogLevel = l
		} else {
			return err
		}
	}
	zerolog.SetGlobalLevel(intLogLevel)
	if enableFileName {
		logger = logger.With().CallerWithSkipFrameCount(3).Logger()
	}
	log.Logger = logger
	return nil
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

// Info logs to the INFO log.
func Info(message string) {
	log.Info().Msg(message)
}

// Infof logs to the INFO log. Arguments are handled in the manner of fmt.Printf.
func Infof(format string, args ...interface{}) {
	log.Info().Msgf(format, args...)
}

// Warning logs to the WARNING log.
func Warning(message string) {
	log.Warn().Msg(message)
}

// Warningf logs to the WARNING log. Arguments are handled in the manner of fmt.Printf.
func Warningf(format string, args ...interface{}) {
	log.Warn().Msgf(format, args...)
}

// Error logs to the ERROR log.
func Error(message string) {
	log.Error().Msg(message)
}

// Errorf logs to the ERROR log. Arguments are handled in the manner of fmt.Printf.
func Errorf(format string, args ...interface{}) {
	log.Error().Msgf(format, args...)
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
