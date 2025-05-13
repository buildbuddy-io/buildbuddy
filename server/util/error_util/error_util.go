package error_util

import (
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	gstatus "google.golang.org/grpc/status"
)

const (
	// snapshotNotFoundErrorReason is returned when the requested snapshot
	// could not be found.
	snapshotNotFoundErrorReason = "SNAPSHOT_NOT_FOUND"

	// requestExecutorNotFoundError is returned when the requested executor
	// could not be found.
	requestExecutorNotFoundError = "requested executor not found"
)

func SnapshotNotFoundError(msg string) error {
	info := &errdetails.ErrorInfo{Reason: snapshotNotFoundErrorReason}
	status := gstatus.New(codes.NotFound, msg)
	if d, err := status.WithDetails(info); err != nil {
		alert.UnexpectedEvent("failed_to_set_status_details", "Failed to set gRPC status details for SnapshotNotFoundError")
		return status.Err()
	} else {
		return d.Err()
	}
}

func IsSnapshotNotFoundError(err error) bool {
	for _, detail := range gstatus.Convert(err).Proto().GetDetails() {
		info := &errdetails.ErrorInfo{}
		if err := detail.UnmarshalTo(info); err != nil {
			// not an ErrorInfo detail; ignore.
			continue
		}
		if info.GetReason() == snapshotNotFoundErrorReason {
			return true
		}
	}
	return false
}

func RequestedExecutorNotFoundError() error {
	return status.UnavailableError(requestExecutorNotFoundError)
}

func IsRequestedExecutorNotFoundError(err error) bool {
	return err != nil && strings.Contains(err.Error(), requestExecutorNotFoundError)
}
