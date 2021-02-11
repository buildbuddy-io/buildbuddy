package status

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Wrap(err error, msg string) error {
	if err == nil {
		return nil
	}
	code := status.Code(err)
	cause := err.Error()
	if cause != "" {
		cause = ": " + cause
	}
	return status.Errorf(code, "%s%s", msg, cause)
}

func Wrapf(err error, format string, a ...interface{}) error {
	return Wrap(err, fmt.Sprintf(format, a...))
}

func OK() error {
	return status.Error(codes.OK, "")
}

func CanceledError(msg string) error {
	return status.Error(codes.Canceled, msg)
}

func CanceledErrorf(format string, a ...interface{}) error {
	return CanceledError(fmt.Sprintf(format, a...))
}

func UnknownError(msg string) error {
	return status.Error(codes.Unknown, msg)
}

func UnknownErrorf(format string, a ...interface{}) error {
	return UnknownError(fmt.Sprintf(format, a...))
}

func InvalidArgumentError(msg string) error {
	return status.Error(codes.InvalidArgument, msg)
}

func InvalidArgumentErrorf(format string, a ...interface{}) error {
	return InvalidArgumentError(fmt.Sprintf(format, a...))
}

func DeadlineExceededError(msg string) error {
	return status.Error(codes.DeadlineExceeded, msg)
}

func DeadlineExceededErrorf(format string, a ...interface{}) error {
	return DeadlineExceededError(fmt.Sprintf(format, a...))
}

func NotFoundError(msg string) error {
	return status.Error(codes.NotFound, msg)
}

func NotFoundErrorf(format string, a ...interface{}) error {
	return NotFoundError(fmt.Sprintf(format, a...))
}

func AlreadyExistsError(msg string) error {
	return status.Error(codes.AlreadyExists, msg)
}

func AlreadyExistsErrorf(format string, a ...interface{}) error {
	return AlreadyExistsError(fmt.Sprintf(format, a...))
}

func PermissionDeniedError(msg string) error {
	return status.Error(codes.PermissionDenied, msg)
}

func PermissionDeniedErrorf(format string, a ...interface{}) error {
	return PermissionDeniedError(fmt.Sprintf(format, a...))
}

func ResourceExhaustedError(msg string) error {
	return status.Error(codes.ResourceExhausted, msg)
}

func ResourceExhaustedErrorf(format string, a ...interface{}) error {
	return ResourceExhaustedError(fmt.Sprintf(format, a...))
}

func FailedPreconditionError(msg string) error {
	return status.Error(codes.FailedPrecondition, msg)
}

func FailedPreconditionErrorf(format string, a ...interface{}) error {
	return FailedPreconditionError(fmt.Sprintf(format, a...))
}

func AbortedError(msg string) error {
	return status.Error(codes.Aborted, msg)
}

func AbortedErrorf(format string, a ...interface{}) error {
	return AbortedError(fmt.Sprintf(format, a...))
}

func OutOfRangeError(msg string) error {
	return status.Error(codes.OutOfRange, msg)
}

func OutOfRangeErrorf(format string, a ...interface{}) error {
	return OutOfRangeError(fmt.Sprintf(format, a...))
}

func UnimplementedError(msg string) error {
	return status.Error(codes.Unimplemented, msg)
}

func UnimplementedErrorf(format string, a ...interface{}) error {
	return UnimplementedError(fmt.Sprintf(format, a...))
}

func InternalError(msg string) error {
	return status.Error(codes.Internal, msg)
}

func InternalErrorf(format string, a ...interface{}) error {
	return InternalError(fmt.Sprintf(format, a...))
}

func UnavailableError(msg string) error {
	return status.Error(codes.Unavailable, msg)
}

func UnavailableErrorf(format string, a ...interface{}) error {
	return UnavailableError(fmt.Sprintf(format, a...))
}

func DataLossError(msg string) error {
	return status.Error(codes.DataLoss, msg)
}

func DataLossErrorf(format string, a ...interface{}) error {
	return DataLossError(fmt.Sprintf(format, a...))
}

func UnauthenticatedError(msg string) error {
	return status.Error(codes.Unauthenticated, msg)
}

func UnauthenticatedErrorf(format string, a ...interface{}) error {
	return UnauthenticatedError(fmt.Sprintf(format, a...))
}
