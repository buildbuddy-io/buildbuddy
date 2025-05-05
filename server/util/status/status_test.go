package status_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

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
	inner := status.FailedPreconditionError("inner error")
	wrapped := status.WrapError(inner, "outer context")
	assert.Equal(t, status.FailedPreconditionError("outer context: inner error"), wrapped)
}

func TestWrapErrorPreservesDetails(t *testing.T) {
	inner := status.FailedPreconditionError("inner error")
	inner = status.WithReason(inner, "TEST_REASON")
	outer := status.WrapError(inner, "outer context")
	toplevel := status.WrapError(outer, "toplevel context")

	assert.Equal(t, "rpc error: code = FailedPrecondition desc = toplevel context: outer context: inner error", toplevel.Error())
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
