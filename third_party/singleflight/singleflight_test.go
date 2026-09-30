package singleflight

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWait_WaitsForCallsAbandonedByTheirCallers(t *testing.T) {
	var g Group[string, int]
	started := make(chan struct{})
	release := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	_, _, err := g.Do(ctx, "key", func(context.Context) (int, error) {
		close(started)
		<-release
		return 1, nil
	})
	require.ErrorIs(t, err, context.Canceled)

	// The caller has given up, but the function is still running.
	waited := make(chan struct{})
	go func() {
		g.Wait()
		close(waited)
	}()
	select {
	case <-waited:
		require.FailNow(t, "Wait returned while a function was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-waited
}
