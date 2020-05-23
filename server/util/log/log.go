package log

import (
	"context"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func LogGRPCRequest(ctx context.Context, fullMethod string, dur time.Duration, err error) {
	reqID, _ := uuid.GetFromContext(ctx) // Ignore error, we're logging anyway.
	shortPath := "/" + path.Base(fullMethod)
	log.Printf("%s %s %q %s [%s]", "gRPC", reqID, shortPath, fmtErr(err), formatDuration(dur))
}

func LogHTTPRequest(ctx context.Context, url string, dur time.Duration, err error) {
	reqID, _ := uuid.GetFromContext(ctx) // Ignore error, we're logging anyway.
	log.Printf("%s %s %q %s [%s]", "HTTP", reqID, url, fmtErr(err), formatDuration(dur))
}
