package log

import (
	"context"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/golang/protobuf/proto"

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
		if len(rmdVals) == 1 {
			rmd := &repb.RequestMetadata{}
			if err := proto.Unmarshal([]byte(rmdVals[0]), rmd); err == nil {
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
		log.Printf("%s %s %s %s %s [%s]", "gRPC", reqID, iid, shortPath, fmtErr(err), formatDuration(dur))
	} else {
		log.Printf("%s %s %s %s [%s]", "gRPC", reqID, shortPath, fmtErr(err), formatDuration(dur))
	}
}

func LogHTTPRequest(ctx context.Context, url string, dur time.Duration, err error) {
	reqID, _ := uuid.GetFromContext(ctx) // Ignore error, we're logging anyway.
	log.Printf("%s %s %q %s [%s]", "HTTP", reqID, url, fmtErr(err), formatDuration(dur))
}
