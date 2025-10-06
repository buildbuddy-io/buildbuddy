package status_test

import (
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	gstatus "google.golang.org/grpc/status"
)

func TestStatusIs(t *testing.T) {
	err := status.CanceledErrorf("Canceled")
	assert.True(t, status.IsCanceledError(err))
	err = status.UnknownErrorf("Unknown")
	assert.True(t, status.IsUnknownError(err))
	err = status.InvalidArgumentErrorf("nvalidArgument")
	assert.True(t, status.IsInvalidArgumentError(err))
	err = status.DeadlineExceededErrorf("DeadlineExceeded")
	assert.True(t, status.IsDeadlineExceededError(err))
	err = status.NotFoundErrorf("NotFound")
	assert.True(t, status.IsNotFoundError(err))
	err = status.AlreadyExistsErrorf("AlreadyExists")
	assert.True(t, status.IsAlreadyExistsError(err))
	err = status.PermissionDeniedErrorf("PermissionDenied")
	assert.True(t, status.IsPermissionDeniedError(err))
	err = status.ResourceExhaustedErrorf("ResourceExhausted")
	assert.True(t, status.IsResourceExhaustedError(err))
	err = status.FailedPreconditionErrorf("FailedPrecondition")
	assert.True(t, status.IsFailedPreconditionError(err))
	err = status.AbortedErrorf("Aborted")
	assert.True(t, status.IsAbortedError(err))
	err = status.OutOfRangeErrorf("OutOfRange")
	assert.True(t, status.IsOutOfRangeError(err))
	err = status.UnimplementedErrorf("Unimplemented")
	assert.True(t, status.IsUnimplementedError(err))
	err = status.InternalErrorf("Internal")
	assert.True(t, status.IsInternalError(err))
	err = status.UnavailableErrorf("Unavailable")
	assert.True(t, status.IsUnavailableError(err))
	err = status.DataLossErrorf("DataLoss")
	assert.True(t, status.IsDataLossError(err))
	err = status.UnauthenticatedErrorf("Unauthenticated")
	assert.True(t, status.IsUnauthenticatedError(err))
}

func TestHasStacktrace(t *testing.T) {
	*status.LogErrorStackTraces = true
	err := status.FailedPreconditionError("FailedPrecondition")
	se, ok := err.(interface {
		StackTrace() errors.StackTrace
	})
	assert.True(t, ok)
	stackTrace := se.StackTrace()
	assert.NotNil(t, stackTrace)
}

func TestNoStacktrace(t *testing.T) {
	*status.LogErrorStackTraces = false
	err := status.FailedPreconditionError("FailedPrecondition")
	_, ok := err.(interface {
		StackTrace() errors.StackTrace
	})
	assert.False(t, ok)
}

func TestWrapErrorPreservesWrappedStatusCodeAndMessage(t *testing.T) {
	{
		inner := status.FailedPreconditionError("inner error")
		wrapped := status.WrapError(inner, "outer context")
		assert.True(t, stderrors.Is(wrapped, inner))
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
	assert.True(t, stderrors.Is(wrapped, inner), "wrapped error should be inner error")
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

	assert.True(t, stderrors.Is(toplevel, inner))
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

func TestWrapWithCodePreservesErrorIdentity(t *testing.T) {
	baseErr := errors.New("base error")
	wrappedErr := status.WrapWithCode(baseErr, codes.Unavailable)

	// Verify status code is set correctly
	assert.True(t, status.IsUnavailableError(wrappedErr))

	// Verify error identity is preserved for errors.Is check
	assert.True(t, stderrors.Is(wrappedErr, baseErr))

	// Verify error message is preserved
	assert.Equal(t, "base error", status.Message(wrappedErr))
}

func TestWrapWithCodeWithNestedErrors(t *testing.T) {
	baseErr := errors.New("base error")
	nestedErr := fmt.Errorf("nested: %w", baseErr)
	wrappedErr := status.WrapWithCode(nestedErr, codes.Internal)

	// Verify status code is set correctly
	assert.True(t, status.IsInternalError(wrappedErr))

	// Verify error identity is preserved through the chain
	assert.True(t, stderrors.Is(wrappedErr, nestedErr))
	assert.True(t, stderrors.Is(wrappedErr, baseErr))

	// Verify error message includes nesting
	assert.Equal(t, "nested: base error", status.Message(wrappedErr))
}
