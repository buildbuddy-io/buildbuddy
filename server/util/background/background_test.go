package background_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/stretchr/testify/require"
)

func TestExtendContextForFinalization_CancelContextBeforeExtending(t *testing.T) {
	ctx := context.Background()

	// Set a deadline ~infinitely far in the future
	ctx, cancel := context.WithTimeout(ctx, 1e6*time.Hour)
	defer cancel()

	// Cancel the original context before the timeout has been reached
	cancel()

	// Extend the canceled context with a very short grace period
	ctx, cancel = background.ExtendContextForFinalization(ctx, 1*time.Nanosecond)
	defer cancel()

	<-ctx.Done() // should return ~immediately
	require.Equal(t, ctx.Err(), context.DeadlineExceeded)
}

func TestExtendContextForFinalization_CancelContextAfterExtending(t *testing.T) {
	ctx := context.Background()

	// Set a deadline ~infinitely far in the future
	ctx, cancel := context.WithTimeout(ctx, 1e6*time.Hour)
	defer cancel()
	cancelOriginal := cancel

	// Extend the canceled context with a very short grace period
	ctx, cancel = background.ExtendContextForFinalization(ctx, 1*time.Nanosecond)
	defer cancel()

	// Cancel the original context; deadline extension starts now.
	cancelOriginal()

	<-ctx.Done() // should return ~immediately
	require.Equal(t, ctx.Err(), context.DeadlineExceeded)
}

func TestExtendContextForFinalization_ManualCancel(t *testing.T) {
	ctx := context.Background()

	// Extend the deadline ~infinitely
	ctx, cancel := background.ExtendContextForFinalization(ctx, 1e6*time.Hour)
	defer cancel()

	// Cancel the extended context manually
	cancel()

	<-ctx.Done() // should return ~immediately
	require.Equal(t, ctx.Err(), context.Canceled)
}
