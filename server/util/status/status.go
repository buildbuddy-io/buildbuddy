package status

import (
	"context"
	"fmt"
	"runtime"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const stackDepth = 10

type wrappedError struct {
	error
	*stack
}

func (w *wrappedError) GRPCStatus() *status.Status {
	if se, ok := w.error.(interface {
		GRPCStatus() *status.Status
	}); ok {
		return se.GRPCStatus()
	}
	return status.New(codes.Unknown, "")
}

type StackTrace = errors.StackTrace
type stack []uintptr

func (s *stack) StackTrace() StackTrace {
	f := make([]errors.Frame, len(*s))
	for i := 0; i < len(f); i++ {
		f[i] = errors.Frame((*s)[i])
	}
	return f
}

func callers() *stack {
	var pcs [stackDepth]uintptr
	n := runtime.Callers(3, pcs[:])
	var st stack = pcs[0:n]
	return &st
}

func makeStatusError(code codes.Code, msg string) error {
	return &wrappedError{
		status.Error(code, msg),
		callers(),
	}
}

func OK() error {
	return status.Error(codes.OK, "")
}
func CanceledError(msg string) error {
	return makeStatusError(codes.Canceled, msg)
}
func IsCanceledError(err error) bool {
	return status.Code(err) == codes.Canceled
}
func CanceledErrorf(format string, a ...interface{}) error {
	return CanceledError(fmt.Sprintf(format, a...))
}
func UnknownError(msg string) error {
	return makeStatusError(codes.Unknown, msg)
}
func IsUnknownError(err error) bool {
	return status.Code(err) == codes.Unknown
}
func UnknownErrorf(format string, a ...interface{}) error {
	return UnknownError(fmt.Sprintf(format, a...))
}
func InvalidArgumentError(msg string) error {
	return makeStatusError(codes.InvalidArgument, msg)
}
func IsInvalidArgumentError(err error) bool {
	return status.Code(err) == codes.InvalidArgument
}
func InvalidArgumentErrorf(format string, a ...interface{}) error {
	return InvalidArgumentError(fmt.Sprintf(format, a...))
}
func DeadlineExceededError(msg string) error {
	return makeStatusError(codes.DeadlineExceeded, msg)
}
func IsDeadlineExceededError(err error) bool {
	return status.Code(err) == codes.DeadlineExceeded
}
func DeadlineExceededErrorf(format string, a ...interface{}) error {
	return DeadlineExceededError(fmt.Sprintf(format, a...))
}
func NotFoundError(msg string) error {
	return makeStatusError(codes.NotFound, msg)
}
func IsNotFoundError(err error) bool {
	return status.Code(err) == codes.NotFound
}
func NotFoundErrorf(format string, a ...interface{}) error {
	return NotFoundError(fmt.Sprintf(format, a...))
}
func AlreadyExistsError(msg string) error {
	return makeStatusError(codes.AlreadyExists, msg)
}
func IsAlreadyExistsError(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}
func AlreadyExistsErrorf(format string, a ...interface{}) error {
	return AlreadyExistsError(fmt.Sprintf(format, a...))
}
func PermissionDeniedError(msg string) error {
	return makeStatusError(codes.PermissionDenied, msg)
}
func IsPermissionDeniedError(err error) bool {
	return status.Code(err) == codes.PermissionDenied
}
func PermissionDeniedErrorf(format string, a ...interface{}) error {
	return PermissionDeniedError(fmt.Sprintf(format, a...))
}
func ResourceExhaustedError(msg string) error {
	return makeStatusError(codes.ResourceExhausted, msg)
}
func IsResourceExhaustedError(err error) bool {
	return status.Code(err) == codes.ResourceExhausted
}
func ResourceExhaustedErrorf(format string, a ...interface{}) error {
	return ResourceExhaustedError(fmt.Sprintf(format, a...))
}
func FailedPreconditionError(msg string) error {
	return makeStatusError(codes.FailedPrecondition, msg)
}
func IsFailedPreconditionError(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}
func FailedPreconditionErrorf(format string, a ...interface{}) error {
	return FailedPreconditionError(fmt.Sprintf(format, a...))
}
func AbortedError(msg string) error {
	return makeStatusError(codes.Aborted, msg)
}
func IsAbortedError(err error) bool {
	return status.Code(err) == codes.Aborted
}
func AbortedErrorf(format string, a ...interface{}) error {
	return AbortedError(fmt.Sprintf(format, a...))
}
func OutOfRangeError(msg string) error {
	return makeStatusError(codes.OutOfRange, msg)
}
func IsOutOfRangeError(err error) bool {
	return status.Code(err) == codes.OutOfRange
}
func OutOfRangeErrorf(format string, a ...interface{}) error {
	return OutOfRangeError(fmt.Sprintf(format, a...))
}
func UnimplementedError(msg string) error {
	return makeStatusError(codes.Unimplemented, msg)
}
func IsUnimplementedError(err error) bool {
	return status.Code(err) == codes.Unimplemented
}
func UnimplementedErrorf(format string, a ...interface{}) error {
	return UnimplementedError(fmt.Sprintf(format, a...))
}
func InternalError(msg string) error {
	return makeStatusError(codes.Internal, msg)
}
func IsInternalError(err error) bool {
	return status.Code(err) == codes.Internal
}
func InternalErrorf(format string, a ...interface{}) error {
	return InternalError(fmt.Sprintf(format, a...))
}
func UnavailableError(msg string) error {
	return makeStatusError(codes.Unavailable, msg)
}
func IsUnavailableError(err error) bool {
	return status.Code(err) == codes.Unavailable
}
func UnavailableErrorf(format string, a ...interface{}) error {
	return UnavailableError(fmt.Sprintf(format, a...))
}
func DataLossError(msg string) error {
	return makeStatusError(codes.DataLoss, msg)
}
func IsDataLossError(err error) bool {
	return status.Code(err) == codes.DataLoss
}
func DataLossErrorf(format string, a ...interface{}) error {
	return DataLossError(fmt.Sprintf(format, a...))
}
func UnauthenticatedError(msg string) error {
	return makeStatusError(codes.Unauthenticated, msg)
}
func IsUnauthenticatedError(err error) bool {
	return status.Code(err) == codes.Unauthenticated
}
func UnauthenticatedErrorf(format string, a ...interface{}) error {
	return UnauthenticatedError(fmt.Sprintf(format, a...))
}

// Wrap adds additional context to an error, preserving the underlying status code.
func WrapError(err error, msg string) error {
	return makeStatusError(status.Code(err), fmt.Sprintf("%s: %s", msg, Message(err)))
}

// Wrapf is the "Printf" version of `Wrap`.
func WrapErrorf(err error, format string, a ...interface{}) error {
	return WrapError(err, fmt.Sprintf(format, a...))
}

// Message extracts the error message from a given error, which for gRPC errors
// is just the "desc" part of the error.
func Message(err error) string {
	if err == nil {
		return ""
	}
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

// FromContextError converts ctx.Err() to the equivalent gRPC status error.
func FromContextError(ctx context.Context) error {
	s := status.FromContextError(ctx.Err())
	return status.ErrorProto(s.Proto())
}

// MetricsLabel returns an appropriate value for StatusHumanReadableLabel given
// an error from a gRPC request (which may be `nil`). See
// `StatusHumanReadableLabel` in server/metrics/metrics.go
func MetricsLabel(err error) string {
	// Check for client context errors first (context canceled, deadline
	// exceeded).
	s := status.FromContextError(err)
	if s.Code() != codes.Unknown {
		return s.Code().String()
	}
	// Return response error code (or network-level/framework-internal etc.
	// errors).
	return status.Code(err).String()
}
