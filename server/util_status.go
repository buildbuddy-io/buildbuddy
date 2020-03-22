package util_status

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func OK() error {
	return status.Error(codes.OK, "")
}

func CanceledError(msg string) error {
	return status.Error(codes.Canceled, msg)
}
func UnknownError(msg string) error {
	return status.Error(codes.Unknown, msg)
}
func InvalidArgumentError(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}
func DeadlineExceededError(msg string) error {
	return status.Error(codes.DeadlineExceeded, msg)
}
func NotFoundError(msg string) error {
	return status.Error(codes.NotFound, msg)
}
func AlreadyExistsError(msg string) error {
	return status.Error(codes.AlreadyExists, msg)
}
func PermissionDeniedError(msg string) error {
	return status.Error(codes.PermissionDenied, msg)
}
func ResourceExhaustedError(msg string) error {
	return status.Error(codes.ResourceExhausted, msg)
}
func FailedPreconditionError(msg string) error {
	return status.Error(codes.FailedPrecondition, msg)
}
func AbortedError(msg string) error {
	return status.Error(codes.Aborted, msg)
}
func OutOfRangeError(msg string) error {
	return status.Error(codes.OutOfRange, msg)
}
func UnimplementedError(msg string) error {
	return status.Error(codes.Unimplemented, msg)
}
func InternalError(msg string) error {
	return status.Error(codes.Internal, msg)
}
func UnavailableError(msg string) error {
	return status.Error(codes.Unavailable, msg)
}
func DataLossError(msg string) error {
	return status.Error(codes.DataLoss, msg)
}
func UnauthenticatedError(msg string) error {
	return status.Error(codes.Unauthenticated, msg)
}
