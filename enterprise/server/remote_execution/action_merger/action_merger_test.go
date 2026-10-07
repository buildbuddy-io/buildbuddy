package action_merger

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/require"
)

type testSchedulerService struct {
	interfaces.SchedulerService
	existsTask func(context.Context, string) (bool, error)
}

func (s *testSchedulerService) ExistsTask(ctx context.Context, taskID string) (bool, error) {
	return s.existsTask(ctx, taskID)
}

type observedContext struct {
	context.Context
	observed chan<- struct{}
	once     sync.Once
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { c.observed <- struct{}{} })
	return c.Context.Done()
}

func awaitSignals(t *testing.T, ch <-chan struct{}, count int) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range count {
		select {
		case <-ch:
		case <-timer.C:
			t.Fatal("timed out waiting for callers")
		}
	}
}

func awaitResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for result")
		return nil
	}
}

func TestWaitForTaskToExistSharesConcurrentChecks(t *testing.T) {
	const callers = 100
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var checks atomic.Int64
	scheduler := &testSchedulerService{existsTask: func(ctx context.Context, taskID string) (bool, error) {
		checks.Add(1)
		select {
		case <-release:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}}

	observed := make(chan struct{}, callers)
	results := make(chan error, callers)
	for range callers {
		go func() {
			ctx := &observedContext{Context: t.Context(), observed: observed}
			results <- waitForTaskToExist(ctx, scheduler, "execution")
		}()
	}
	// The wait path reads Done after registering the caller. Keep the scheduler
	// check blocked until every caller is waiting so the test is deterministic.
	awaitSignals(t, observed, callers)
	unblock()
	for range callers {
		require.NoError(t, awaitResult(t, results))
	}
	require.Equal(t, int64(1), checks.Load())
}

func TestWaitForTaskToExistCallerCancellationDoesNotCancelOtherCallers(t *testing.T) {
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	started := make(chan context.Context, 2)
	var checks atomic.Int64
	scheduler := &testSchedulerService{existsTask: func(ctx context.Context, taskID string) (bool, error) {
		checks.Add(1)
		started <- ctx
		select {
		case <-release:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}}

	observed := make(chan struct{}, 2)
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	defer cancelFirst()
	firstResult := make(chan error, 1)
	go func() {
		ctx := &observedContext{Context: firstCtx, observed: observed}
		firstResult <- waitForTaskToExist(ctx, scheduler, "execution")
	}()
	var checkCtx context.Context
	select {
	case checkCtx = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduler check")
	}
	secondResult := make(chan error, 1)
	go func() {
		ctx := &observedContext{Context: t.Context(), observed: observed}
		secondResult <- waitForTaskToExist(ctx, scheduler, "execution")
	}()
	awaitSignals(t, observed, 2)

	cancelFirst()
	require.ErrorIs(t, awaitResult(t, firstResult), context.Canceled)
	require.NoError(t, checkCtx.Err())
	unblock()
	require.NoError(t, awaitResult(t, secondResult))
	require.Equal(t, int64(1), checks.Load())
}
