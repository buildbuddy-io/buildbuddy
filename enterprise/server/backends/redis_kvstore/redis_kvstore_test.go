package redis_kvstore_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_kvstore"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

func TestReplaceSuffix(t *testing.T) {
	ctx := t.Context()
	store := redis_kvstore.New(testredis.Start(t).Client())

	// Write an initial value.
	require.NoError(t, store.Set(ctx, "key", []byte("hello")))

	// Writing a suffix at the end of the value should append to it.
	require.NoError(t, store.ReplaceSuffix(ctx, "key", 5, 5, []byte(" world")))
	val, err := store.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "hello world", string(val))

	// Writing a suffix at an offset within the value should overwrite the
	// bytes from that offset onward.
	require.NoError(t, store.ReplaceSuffix(ctx, "key", 11, 6, []byte("gopher")))
	val, err = store.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "hello gopher", string(val))

	// Writing a shorter suffix should truncate the value.
	require.NoError(t, store.ReplaceSuffix(ctx, "key", 12, 6, []byte("go")))
	val, err = store.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "hello go", string(val))

	// Writing a suffix with a stale expected length should fail with
	// FailedPrecondition and leave the value unchanged.
	err = store.ReplaceSuffix(ctx, "key", 5, 5, []byte("!!!"))
	require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got %v", err)
	val, err = store.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, "hello go", string(val))

	// An offset past the end of the value is invalid.
	err = store.ReplaceSuffix(ctx, "key", 8, 9, []byte("x"))
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %v", err)

	// When the expected length is stale and the offset is also out of range,
	// the offset is validated first, so the call should fail with
	// InvalidArgument rather than FailedPrecondition.
	err = store.ReplaceSuffix(ctx, "key", 5, 6, []byte("x"))
	require.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got %v", err)
}

func TestReplaceSuffix_MissingKey(t *testing.T) {
	ctx := t.Context()
	store := redis_kvstore.New(testredis.Start(t).Client())

	// Writing a suffix to a key that doesn't exist (for example, because it
	// was evicted) should fail with FailedPrecondition and not create the key.
	err := store.ReplaceSuffix(ctx, "missing", 5, 0, []byte("data"))
	require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got %v", err)
	_, err = store.Get(ctx, "missing")
	require.True(t, status.IsNotFoundError(err), "expected NotFound, got %v", err)
}
