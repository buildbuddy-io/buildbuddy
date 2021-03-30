package log

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
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


func Configure(level int) {
	output := zerolog.ConsoleWriter{Out: os.Stdout}
	// 2021/03/30 15:29:37 gRPC 7c3b8119-aae8-4e8f-9842-15730bd15d00 7080eda9-e50a-4370-b821-5e634d66bb26 /FindMissingBlobs OK [49 us]
	log.Logger = zerolog.New(output).With().Timestamp().Logger()
}

// TODO(tylerw): move this to main or libmain?
func init() {
	Configure(0)
}

// Zerolog convenience wrapper here:

func Print(message string) {
	log.Info().Msg(message)
}
func Printf(format string, v ...interface{}) {
	log.Info().Msgf(format, v...)
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
