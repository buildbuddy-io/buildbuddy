package status

import (
	"fmt"
	"runtime"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	stackframe "github.com/go-errors/errors"
)

const maxStackDepth = 50

type stackedError struct {
	error
	stack  []uintptr
	frames []stackframe.StackFrame
}

func wrap(e interface{}) *stackedError {
	if e == nil {
		return nil
	}

	var err error

	switch e := e.(type) {
	case *stackedError:
		return e
	case error:
		err = e
	default:
		err = fmt.Errorf("%v", e)
	}

	stack := make([]uintptr, maxStackDepth)
	length := runtime.Callers(2, stack[:])
	return &stackedError{
		error: err,
		stack: stack[:length],
	}
}

func (err *stackedError) stackFrames() []stackframe.StackFrame {
	if err.frames == nil {
		err.frames = make([]stackframe.StackFrame, len(err.stack))
		for i, pc := range err.stack {
			err.frames[i] = stackframe.NewStackFrame(pc)
		}
	}
	return err.frames
}

// Error prints the normal error message + code.
func (err *stackedError) Error() string {
	return err.error.Error()
}

// ErrorWithStack prints the error message + code followed
// by a stack trace.
func (err *stackedError) ErrorWithStack() string {
	msg := err.error.Error()
	msg += "\n"
	for _, frame := range err.stackFrames() {
		msg += frame.String()
	}
	return msg
}

func assemble(c codes.Code, msg string) *stackedError {
	return wrap(status.Error(c, msg))
}

func OK() error {
	return assemble(codes.OK, "")
}

func CanceledError(msg string) error {
	return assemble(codes.Canceled, msg)
}

func CanceledErrorf(format string, a ...interface{}) error {
	return CanceledError(fmt.Sprintf(format, a...))
}

func UnknownError(msg string) error {
	return assemble(codes.Unknown, msg)
}

func UnknownErrorf(format string, a ...interface{}) error {
	return UnknownError(fmt.Sprintf(format, a...))
}

func InvalidArgumentError(msg string) error {
	return assemble(codes.InvalidArgument, msg)
}

func InvalidArgumentErrorf(format string, a ...interface{}) error {
	return InvalidArgumentError(fmt.Sprintf(format, a...))
}

func DeadlineExceededError(msg string) error {
	return assemble(codes.DeadlineExceeded, msg)
}

func DeadlineExceededErrorf(format string, a ...interface{}) error {
	return DeadlineExceededError(fmt.Sprintf(format, a...))
}

func NotFoundError(msg string) error {
	return assemble(codes.NotFound, msg)
}

func NotFoundErrorf(format string, a ...interface{}) error {
	return NotFoundError(fmt.Sprintf(format, a...))
}

func AlreadyExistsError(msg string) error {
	return assemble(codes.AlreadyExists, msg)
}

func AlreadyExistsErrorf(format string, a ...interface{}) error {
	return AlreadyExistsError(fmt.Sprintf(format, a...))
}

func PermissionDeniedError(msg string) error {
	return assemble(codes.PermissionDenied, msg)
}

func PermissionDeniedErrorf(format string, a ...interface{}) error {
	return PermissionDeniedError(fmt.Sprintf(format, a...))
}

func ResourceExhaustedError(msg string) error {
	return assemble(codes.ResourceExhausted, msg)
}

func ResourceExhaustedErrorf(format string, a ...interface{}) error {
	return ResourceExhaustedError(fmt.Sprintf(format, a...))
}

func FailedPreconditionError(msg string) error {
	return assemble(codes.FailedPrecondition, msg)
}

func FailedPreconditionErrorf(format string, a ...interface{}) error {
	return FailedPreconditionError(fmt.Sprintf(format, a...))
}

func AbortedError(msg string) error {
	return assemble(codes.Aborted, msg)
}

func AbortedErrorf(format string, a ...interface{}) error {
	return AbortedError(fmt.Sprintf(format, a...))
}

func OutOfRangeError(msg string) error {
	return assemble(codes.OutOfRange, msg)
}

func OutOfRangeErrorf(format string, a ...interface{}) error {
	return OutOfRangeError(fmt.Sprintf(format, a...))
}

func UnimplementedError(msg string) error {
	return assemble(codes.Unimplemented, msg)
}

func UnimplementedErrorf(format string, a ...interface{}) error {
	return UnimplementedError(fmt.Sprintf(format, a...))
}

func InternalError(msg string) error {
	return assemble(codes.Internal, msg)
}

func InternalErrorf(format string, a ...interface{}) error {
	return InternalError(fmt.Sprintf(format, a...))
}

func UnavailableError(msg string) error {
	return assemble(codes.Unavailable, msg)
}

func UnavailableErrorf(format string, a ...interface{}) error {
	return UnavailableError(fmt.Sprintf(format, a...))
}

func DataLossError(msg string) error {
	return assemble(codes.DataLoss, msg)
}

func DataLossErrorf(format string, a ...interface{}) error {
	return DataLossError(fmt.Sprintf(format, a...))
}

func UnauthenticatedError(msg string) error {
	return assemble(codes.Unauthenticated, msg)
}

func UnauthenticatedErrorf(format string, a ...interface{}) error {
	return UnauthenticatedError(fmt.Sprintf(format, a...))
}
