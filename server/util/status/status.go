package status

import (
	"context"
	stderrors "errors"
	"flag"
	"fmt"
	"log"
	"runtime"

	"github.com/pkg/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

var LogErrorStackTraces = flag.Bool("app.log_error_stack_traces", false, "If true, stack traces will be printed for errors that have them.")

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

func (w *wrappedError) Unwrap() error {
	return w.error
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

// statusError wraps an error with a gRPC status code while preserving the
// underlying error for errors.Is() checks.
type statusError struct {
	code    codes.Code
	err     error
	details []protoadapt.MessageV1
}

func (e *statusError) Error() string {
	return e.GRPCStatus().String()
}

func (e *statusError) Unwrap() error {
	return e.err
}

func (e *statusError) withDetails(details ...protoadapt.MessageV1) {
	e.details = details
}

func (e *statusError) GRPCStatus() *status.Status {
	s := status.New(e.code, e.err.Error())
	if len(e.details) > 0 {
		var err error
		s, err = s.WithDetails(e.details...)
		if err != nil {
			return status.New(codes.Internal, fmt.Sprintf("add error details to error: %s", err))
		}
		return s
	}
	return s
}

// WrapWithCode wraps an error with a gRPC status code while preserving the
// underlying error for errors.Is() checks. This allows the error to have
// both a specific status code AND maintain its identity for error comparison.
func WrapWithCode(err error, code codes.Code) error {
	return &statusError{
		code: code,
		err:  err,
	}
}

func makeStatusErrorFromMessage(code codes.Code, msg string, details ...protoadapt.MessageV1) error {
	return makeStatusError(code, stderrors.New(msg), details...)
}

func makeStatusError(code codes.Code, err error, details ...protoadapt.MessageV1) error {
	statusErr := &statusError{
		code: code,
		err:  err,
	}

	if len(details) > 0 {
		statusErr.details = details
	}

	if !*LogErrorStackTraces {
		return statusErr
	}
	return &wrappedError{
		statusErr,
		callers(),
	}
}

func OK() error {
	return status.Error(codes.OK, "")
}
func CanceledError(msg string) error {
	return makeStatusErrorFromMessage(codes.Canceled, msg)
}
func IsCanceledError(err error) bool {
	return status.Code(err) == codes.Canceled
}
func CanceledErrorf(format string, a ...interface{}) error {
	return CanceledError(fmt.Sprintf(format, a...))
}
func UnknownError(msg string) error {
	return makeStatusErrorFromMessage(codes.Unknown, msg)
}
func IsUnknownError(err error) bool {
	return status.Code(err) == codes.Unknown
}
func UnknownErrorf(format string, a ...interface{}) error {
	return UnknownError(fmt.Sprintf(format, a...))
}
func InvalidArgumentError(msg string) error {
	return makeStatusErrorFromMessage(codes.InvalidArgument, msg)
}
func IsInvalidArgumentError(err error) bool {
	return status.Code(err) == codes.InvalidArgument
}
func InvalidArgumentErrorf(format string, a ...interface{}) error {
	return InvalidArgumentError(fmt.Sprintf(format, a...))
}
func DeadlineExceededError(msg string) error {
	return makeStatusErrorFromMessage(codes.DeadlineExceeded, msg)
}
func IsDeadlineExceededError(err error) bool {
	return status.Code(err) == codes.DeadlineExceeded
}
func DeadlineExceededErrorf(format string, a ...interface{}) error {
	return DeadlineExceededError(fmt.Sprintf(format, a...))
}
func NotFoundError(msg string) error {
	return makeStatusErrorFromMessage(codes.NotFound, msg)
}
func IsNotFoundError(err error) bool {
	return status.Code(err) == codes.NotFound
}
func NotFoundErrorf(format string, a ...interface{}) error {
	return NotFoundError(fmt.Sprintf(format, a...))
}
func AlreadyExistsError(msg string) error {
	return makeStatusErrorFromMessage(codes.AlreadyExists, msg)
}
func IsAlreadyExistsError(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}
func AlreadyExistsErrorf(format string, a ...interface{}) error {
	return AlreadyExistsError(fmt.Sprintf(format, a...))
}
func PermissionDeniedError(msg string) error {
	return makeStatusErrorFromMessage(codes.PermissionDenied, msg)
}
func IsPermissionDeniedError(err error) bool {
	return status.Code(err) == codes.PermissionDenied
}
func PermissionDeniedErrorf(format string, a ...interface{}) error {
	return PermissionDeniedError(fmt.Sprintf(format, a...))
}
func ResourceExhaustedError(msg string) error {
	return makeStatusErrorFromMessage(codes.ResourceExhausted, msg)
}
func IsResourceExhaustedError(err error) bool {
	return status.Code(err) == codes.ResourceExhausted
}
func ResourceExhaustedErrorf(format string, a ...interface{}) error {
	return ResourceExhaustedError(fmt.Sprintf(format, a...))
}
func FailedPreconditionError(msg string) error {
	return makeStatusErrorFromMessage(codes.FailedPrecondition, msg)
}
func IsFailedPreconditionError(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}
func FailedPreconditionErrorf(format string, a ...interface{}) error {
	return FailedPreconditionError(fmt.Sprintf(format, a...))
}
func AbortedError(msg string) error {
	return makeStatusErrorFromMessage(codes.Aborted, msg)
}
func IsAbortedError(err error) bool {
	return status.Code(err) == codes.Aborted
}
func AbortedErrorf(format string, a ...interface{}) error {
	return AbortedError(fmt.Sprintf(format, a...))
}
func OutOfRangeError(msg string) error {
	return makeStatusErrorFromMessage(codes.OutOfRange, msg)
}
func IsOutOfRangeError(err error) bool {
	return status.Code(err) == codes.OutOfRange
}
func OutOfRangeErrorf(format string, a ...interface{}) error {
	return OutOfRangeError(fmt.Sprintf(format, a...))
}
func UnimplementedError(msg string) error {
	return makeStatusErrorFromMessage(codes.Unimplemented, msg)
}
func IsUnimplementedError(err error) bool {
	return status.Code(err) == codes.Unimplemented
}
func UnimplementedErrorf(format string, a ...interface{}) error {
	return UnimplementedError(fmt.Sprintf(format, a...))
}
func InternalError(msg string) error {
	return makeStatusErrorFromMessage(codes.Internal, msg)
}
func IsInternalError(err error) bool {
	return status.Code(err) == codes.Internal
}
func InternalErrorf(format string, a ...interface{}) error {
	return InternalError(fmt.Sprintf(format, a...))
}
func UnavailableError(msg string) error {
	return makeStatusErrorFromMessage(codes.Unavailable, msg)
}
func IsUnavailableError(err error) bool {
	return status.Code(err) == codes.Unavailable
}
func UnavailableErrorf(format string, a ...interface{}) error {
	return UnavailableError(fmt.Sprintf(format, a...))
}
func DataLossError(msg string) error {
	return makeStatusErrorFromMessage(codes.DataLoss, msg)
}
func IsDataLossError(err error) bool {
	return status.Code(err) == codes.DataLoss
}
func DataLossErrorf(format string, a ...interface{}) error {
	return DataLossError(fmt.Sprintf(format, a...))
}
func UnauthenticatedError(msg string) error {
	return makeStatusErrorFromMessage(codes.Unauthenticated, msg)
}
func IsUnauthenticatedError(err error) bool {
	return status.Code(err) == codes.Unauthenticated
}
func UnauthenticatedErrorf(format string, a ...interface{}) error {
	return UnauthenticatedError(fmt.Sprintf(format, a...))
}

// WrapError prepends additional context to an error description, preserving the
// underlying status code and error details.
func WrapError(err error, msg string) error {
	if err == nil {
		return nil
	}
	var statusErr *statusError
	if errors.As(err, &statusErr) {
		statusErr.err = fmt.Errorf("%s: %w", msg, statusErr.err)
		return statusErr
	}

	s, isStatusErr := status.FromError(err)
	// Preserve any details from the original error.
	decodedDetails := s.Details()
	details := make([]protoadapt.MessageV1, len(decodedDetails))
	for i, detail := range decodedDetails {
		if err, ok := detail.(error); ok {
			return InternalErrorf("unmarshal status detail: %v", err)
		}
		if pb, ok := detail.(protoadapt.MessageV1); ok {
			details[i] = pb
		} else {
			return InternalErrorf("unmarshal status detail: unrecognized detail type %T", detail)
		}
	}

	var errWithContext error
	if isStatusErr {
		errWithContext = fmt.Errorf("%s: %s", msg, s.Message())
	} else {
		errWithContext = fmt.Errorf("%s: %w", msg, err)
	}
	return makeStatusError(status.Code(err), errWithContext, details...)
}

// Wrapf is the "Printf" version of `Wrap`.
func WrapErrorf(err error, format string, a ...interface{}) error {
	return WrapError(err, fmt.Sprintf(format, a...))
}

// WithReason returns a new error with a reason code attached to the given
// error. The reason code should be a unique, constant string identifier in
// UPPER_SNAKE_CASE that identifies the proximate cause of an error. This can be
// useful in cases where additional granularity is needed than just the gRPC
// status code.
func WithReason(err error, reason string) error {
	info := &errdetails.ErrorInfo{
		Reason: reason,
		Domain: "buildbuddy.io",
	}

	var statusErr *statusError
	if errors.As(err, &statusErr) {
	} else {
		statusErr = &statusError{
			code: status.Code(err),
			err:  err,
		}
	}
	statusErr.details = append(statusErr.details, info)
	return statusErr
}

// Message extracts the error message from a given error, which for gRPC errors
// is just the "desc" part of the error.
func Message(err error) string {
	if err == nil {
		return ""
	}

	var statusErr *statusError
	if errors.As(err, &statusErr) {
		log.Printf("returned statusErr.err.Error()")
		return statusErr.err.Error()
	}

	if s, ok := status.FromError(err); ok {
		log.Printf("returned s.Message()")
		return s.Message()
	}
	log.Printf("returned err.Error()")
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
