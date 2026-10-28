package canary_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/stretchr/testify/require"
)

const period = 10 * time.Millisecond

// calls records the argument of each lateFn and doneFn call.
type calls struct {
	late chan time.Duration
	done chan time.Duration
}

func start(period time.Duration) (*calls, canary.CancelFunc) {
	c := &calls{
		late: make(chan time.Duration, 1000),
		done: make(chan time.Duration, 1000),
	}
	cancel := canary.StartWithLateFn(period,
		func(d time.Duration) { c.late <- d },
		func(d time.Duration) { c.done <- d })
	return c, cancel
}

func (c *calls) waitLate(t *testing.T) time.Duration {
	t.Helper()
	select {
	case d := <-c.late:
		return d
	case <-time.After(10 * time.Second):
		t.Fatal("lateFn was not called")
		return 0
	}
}

func TestLateFnFiresEveryPeriodThenDoneFnOnCancel(t *testing.T) {
	c, cancel := start(period)
	var prev time.Duration
	for i := 1; i <= 3; i++ {
		d := c.waitLate(t)
		require.GreaterOrEqual(t, d, time.Duration(i)*period)
		require.Greater(t, d, prev)
		prev = d
	}
	cancel()
	require.Len(t, c.done, 1)
	require.Greater(t, <-c.done, prev)

	// A firing that had already started before cancel may still complete,
	// but nothing re-arms the timer after that.
	before := len(c.late)
	time.Sleep(10 * period)
	require.LessOrEqual(t, len(c.late), before+1)
}

func TestCancelWithinPeriodSkipsDoneFn(t *testing.T) {
	c, cancel := start(time.Hour)
	cancel()
	require.Empty(t, c.late)
	require.Empty(t, c.done)
}

func TestCancelIsIdempotent(t *testing.T) {
	c, cancel := start(period)
	c.waitLate(t)
	cancel()
	cancel()
	require.Len(t, c.done, 1)
}

func TestCancelDuringLateFnStopsTimer(t *testing.T) {
	inLate := make(chan struct{})
	release := make(chan struct{})
	var late atomic.Int32
	done := make(chan time.Duration, 1)
	cancel := canary.StartWithLateFn(period,
		func(time.Duration) {
			if late.Add(1) == 1 {
				close(inLate)
				<-release
			}
		},
		func(d time.Duration) { done <- d })
	<-inLate

	// cancel must not wait for the in-flight lateFn to return.
	canceled := make(chan struct{})
	go func() {
		cancel()
		close(canceled)
	}()
	select {
	case <-canceled:
	case <-time.After(10 * time.Second):
		t.Fatal("cancel blocked on lateFn")
	}
	require.Len(t, done, 1)

	// The timer armed after lateFn returns must be stopped, so lateFn never
	// runs again.
	close(release)
	time.Sleep(10 * period)
	require.Equal(t, int32(1), late.Load())
}
