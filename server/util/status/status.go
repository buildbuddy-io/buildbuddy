package status

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func OK() error {
	return status.Error(codes.OK, "")
}
func CanceledError(msg string) error {
	return status.Error(codes.Canceled, msg)
}
func IsCanceledError(err error) bool {
	return status.Code(err) == codes.Canceled
}
func CanceledErrorf(format string, a ...interface{}) error {
	return CanceledError(fmt.Sprintf(format, a...))
}
func UnknownError(msg string) error {
	return status.Error(codes.Unknown, msg)
}
func IsUnknownError(err error) bool {
	return status.Code(err) == codes.Unknown
}
func UnknownErrorf(format string, a ...interface{}) error {
	return UnknownError(fmt.Sprintf(format, a...))
}
func InvalidArgumentError(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}
func IsInvalidArgumentError(err error) bool {
	return status.Code(err) == codes.InvalidArgument
}
func InvalidArgumentErrorf(format string, a ...interface{}) error {
	return InvalidArgumentError(fmt.Sprintf(format, a...))
}
func DeadlineExceededError(msg string) error {
	return status.Error(codes.DeadlineExceeded, msg)
}
func IsDeadlineExceededError(err error) bool {
	return status.Code(err) == codes.DeadlineExceeded
}
func DeadlineExceededErrorf(format string, a ...interface{}) error {
	return DeadlineExceededError(fmt.Sprintf(format, a...))
}
func NotFoundError(msg string) error {
	return status.Error(codes.NotFound, msg)
}
func IsNotFoundError(err error) bool {
	return status.Code(err) == codes.NotFound
}
func NotFoundErrorf(format string, a ...interface{}) error {
	return NotFoundError(fmt.Sprintf(format, a...))
}
func AlreadyExistsError(msg string) error {
	return status.Error(codes.AlreadyExists, msg)
}
func IsAlreadyExistsError(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}
func AlreadyExistsErrorf(format string, a ...interface{}) error {
	return AlreadyExistsError(fmt.Sprintf(format, a...))
}
func PermissionDeniedError(msg string) error {
	return status.Error(codes.PermissionDenied, msg)
}
func IsPermissionDeniedError(err error) bool {
	return status.Code(err) == codes.PermissionDenied
}
func PermissionDeniedErrorf(format string, a ...interface{}) error {
	return PermissionDeniedError(fmt.Sprintf(format, a...))
}
func ResourceExhaustedError(msg string) error {
	return status.Error(codes.ResourceExhausted, msg)
}
func IsResourceExhaustedError(err error) bool {
	return status.Code(err) == codes.ResourceExhausted
}
func ResourceExhaustedErrorf(format string, a ...interface{}) error {
	return ResourceExhaustedError(fmt.Sprintf(format, a...))
}
func FailedPreconditionError(msg string) error {
	return status.Error(codes.FailedPrecondition, msg)
}
func IsFailedPreconditionError(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}
func FailedPreconditionErrorf(format string, a ...interface{}) error {
	return FailedPreconditionError(fmt.Sprintf(format, a...))
}
func AbortedError(msg string) error {
	return status.Error(codes.Aborted, msg)
}
func IsAbortedError(err error) bool {
	return status.Code(err) == codes.Aborted
}
func AbortedErrorf(format string, a ...interface{}) error {
	return AbortedError(fmt.Sprintf(format, a...))
}
func OutOfRangeError(msg string) error {
	return status.Error(codes.OutOfRange, msg)
}
func IsOutOfRangeError(err error) bool {
	return status.Code(err) == codes.OutOfRange
}
func OutOfRangeErrorf(format string, a ...interface{}) error {
	return OutOfRangeError(fmt.Sprintf(format, a...))
}
func UnimplementedError(msg string) error {
	return status.Error(codes.Unimplemented, msg)
}
func IsUnimplementedError(err error) bool {
	return status.Code(err) == codes.Unimplemented
}
func UnimplementedErrorf(format string, a ...interface{}) error {
	return UnimplementedError(fmt.Sprintf(format, a...))
}
func InternalError(msg string) error {
	return status.Error(codes.Internal, msg)
}
func IsInternalError(err error) bool {
	return status.Code(err) == codes.Internal
}
func InternalErrorf(format string, a ...interface{}) error {
	return InternalError(fmt.Sprintf(format, a...))
}
func UnavailableError(msg string) error {
	return status.Error(codes.Unavailable, msg)
}
func IsUnavailableError(err error) bool {
	return status.Code(err) == codes.Unavailable
}
func UnavailableErrorf(format string, a ...interface{}) error {
	return UnavailableError(fmt.Sprintf(format, a...))
}
func DataLossError(msg string) error {
	return status.Error(codes.DataLoss, msg)
}
func IsDataLossError(err error) bool {
	return status.Code(err) == codes.DataLoss
}
func DataLossErrorf(format string, a ...interface{}) error {
	return DataLossError(fmt.Sprintf(format, a...))
}
func UnauthenticatedError(msg string) error {
	return status.Error(codes.Unauthenticated, msg)
}
func IsUnauthenticatedError(err error) bool {
	return status.Code(err) == codes.Unauthenticated
}
func UnauthenticatedErrorf(format string, a ...interface{}) error {
	return UnauthenticatedError(fmt.Sprintf(format, a...))
}
