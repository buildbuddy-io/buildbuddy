package status_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	gstatus "google.golang.org/grpc/status"
)

func TestStatusIs(t *testing.T) {
	innerErr := errors.New("inner error")
	err := status.CanceledErrorf("Canceled: %w", innerErr)
	assert.True(t, status.IsCanceledError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.UnknownErrorf("Unknown: %w", innerErr)
	assert.True(t, status.IsUnknownError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.InvalidArgumentErrorf("InvalidArgument: %w", innerErr)
	assert.True(t, status.IsInvalidArgumentError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.DeadlineExceededErrorf("DeadlineExceeded: %w", innerErr)
	assert.True(t, status.IsDeadlineExceededError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.NotFoundErrorf("NotFound: %w", innerErr)
	assert.True(t, status.IsNotFoundError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.AlreadyExistsErrorf("AlreadyExists: %w", innerErr)
	assert.True(t, status.IsAlreadyExistsError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.PermissionDeniedErrorf("PermissionDenied: %w", innerErr)
	assert.True(t, status.IsPermissionDeniedError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.ResourceExhaustedErrorf("ResourceExhausted: %w", innerErr)
	assert.True(t, status.IsResourceExhaustedError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.FailedPreconditionErrorf("FailedPrecondition: %w", innerErr)
	assert.True(t, status.IsFailedPreconditionError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.AbortedErrorf("Aborted: %w", innerErr)
	assert.True(t, status.IsAbortedError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.OutOfRangeErrorf("OutOfRange: %w", innerErr)
	assert.True(t, status.IsOutOfRangeError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.UnimplementedErrorf("Unimplemented: %w", innerErr)
	assert.True(t, status.IsUnimplementedError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.InternalErrorf("Internal: %w", innerErr)
	assert.True(t, status.IsInternalError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.UnavailableErrorf("Unavailable: %w", innerErr)
	assert.True(t, status.IsUnavailableError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.DataLossErrorf("DataLoss: %w", innerErr)
	assert.True(t, status.IsDataLossError(err))
	assert.True(t, errors.Is(err, innerErr))
	err = status.UnauthenticatedErrorf("Unauthenticated: %w", innerErr)
	assert.True(t, status.IsUnauthenticatedError(err))
	assert.True(t, errors.Is(err, innerErr))
}

func TestHasStacktrace(t *testing.T) {
	*status.LogErrorStackTraces = true
	err := status.FailedPreconditionError("FailedPrecondition")
	se, ok := err.(interface {
		StackTrace() pkgerrors.StackTrace
	})
	assert.True(t, ok)
	stackTrace := se.StackTrace()
	assert.NotNil(t, stackTrace)
}

func TestNoStacktrace(t *testing.T) {
	*status.LogErrorStackTraces = false
	err := status.FailedPreconditionError("FailedPrecondition")
	_, ok := err.(interface {
		StackTrace() pkgerrors.StackTrace
	})
	assert.False(t, ok)
}

func TestWrapErrorPreservesWrappedStatusCodeAndMessage(t *testing.T) {
	{
		inner := status.FailedPreconditionError("inner error")
		wrapped := status.WrapError(inner, "outer context")
		assert.True(t, errors.Is(wrapped, inner))
		assert.True(t, status.IsFailedPreconditionError(wrapped))
		assert.Equal(t, "outer context: inner error", status.Message(wrapped))
		assert.Equal(t, status.FailedPreconditionError("outer context: inner error").Error(), wrapped.Error())
	}

	{
		inner := gstatus.New(codes.FailedPrecondition, "inner error")
		wrapped := status.WrapError(inner.Err(), "outer context")
		assert.Equal(t, status.FailedPreconditionError("outer context: inner error").Error(), wrapped.Error())
	}
}

func TestWrapErrorHandlesNonStatusError(t *testing.T) {
	inner := fmt.Errorf("normal error")
	wrapped := status.WrapError(inner, "outer context")
	assert.True(t, errors.Is(wrapped, inner), "wrapped error should be inner error")
	assert.True(t, status.IsUnknownError(wrapped), "wrapped error should be unknown")
	assert.Equal(t, "outer context: normal error", status.Message(wrapped))
	assert.Equal(t, status.UnknownError("outer context: normal error").Error(), wrapped.Error())
}

func TestWrapErrorPreservesNilError(t *testing.T) {
	wrapped := status.WrapError(nil, "outer context")
	assert.Equal(t, nil, wrapped)
}

func TestWrapErrorPreservesDetails(t *testing.T) {
	inner := status.FailedPreconditionError("inner error")
	inner = status.WithReason(inner, "TEST_REASON")
	outer := status.WrapError(inner, "outer context")
	toplevel := status.WrapError(outer, "toplevel context")

	assert.True(t, errors.Is(toplevel, inner))
	assert.True(t, status.IsFailedPreconditionError(toplevel))
	assert.Equal(t, "toplevel context: outer context: inner error", status.Message(toplevel))
	// Top-level error should preserve the reason.
	foundReason := false
	for _, detail := range gstatus.Convert(toplevel).Details() {
		if errInfo, ok := detail.(*errdetails.ErrorInfo); ok {
			assert.Equal(t, "TEST_REASON", errInfo.Reason)
			foundReason = true
		}
	}
	assert.True(t, foundReason, "could not find reason in wrapped error")
}

func TestWithCodePreservesErrorIdentity(t *testing.T) {
	baseErr := errors.New("base error")
	wrappedErr := status.WithCode(baseErr, codes.Unavailable)

	// Verify status code is set correctly
	assert.True(t, status.IsUnavailableError(wrappedErr))

	// Verify error identity is preserved for errors.Is check
	assert.True(t, errors.Is(wrappedErr, baseErr))

	// Verify error message is preserved
	assert.Equal(t, "base error", status.Message(wrappedErr))
}

func TestWithCodeWithNestedErrors(t *testing.T) {
	baseErr := errors.New("base error")
	nestedErr := fmt.Errorf("nested: %w", baseErr)
	wrappedErr := status.WithCode(nestedErr, codes.Internal)

	// Verify status code is set correctly
	assert.True(t, status.IsInternalError(wrappedErr))

	// Verify error identity is preserved through the chain
	assert.True(t, errors.Is(wrappedErr, nestedErr))
	assert.True(t, errors.Is(wrappedErr, baseErr))

	// Verify error message includes nesting
	assert.Equal(t, "nested: base error", status.Message(wrappedErr))
}
