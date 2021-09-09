package status_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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
	err := status.FailedPreconditionError("FailedPrecondition")
	se, ok := err.(interface {
		StackTrace() errors.StackTrace
	})
	assert.True(t, ok)
	stackTrace := se.StackTrace()
	assert.NotNil(t, stackTrace)
}
