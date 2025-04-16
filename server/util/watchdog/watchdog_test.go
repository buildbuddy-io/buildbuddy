package watchdog_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/watchdog"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestWatchdogExpires(t *testing.T) {
	clock := clockwork.NewFakeClock()
	ttl := time.Hour

	wdt := watchdog.NewWithClock(clock, ttl)
	clock.Advance(time.Minute)
	require.True(t, wdt.Live())

	clock.Advance(ttl)
	require.False(t, wdt.Live())
}

func TestWatchdogReset(t *testing.T) {
	clock := clockwork.NewFakeClock()
	ttl := time.Hour

	wdt := watchdog.NewWithClock(clock, ttl)

	for range 10 {
		clock.Advance(59 * time.Minute)
		require.True(t, wdt.Live())
		wdt.Reset()
	}
	require.True(t, wdt.Live())
}

func TestDisabledWatchdogIsValidForever(t *testing.T) {
	clock := clockwork.NewFakeClock()
	wdt := watchdog.NewWithClock(clock, 0)

	for range 100 {
		clock.Advance(100 * 365 * 24 * time.Hour)
		require.True(t, wdt.Live())
	}
	require.True(t, wdt.Live())
}

func TestCrossThreadUsage(t *testing.T) {
	clock := clockwork.NewFakeClock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wdt := watchdog.NewWithClock(clock, time.Hour)
	go func() {
		select {
		case <-ctx.Done():
			break
		default:
			if !wdt.Live() {
				panic("watchdog timer expired!")
			}
		}
	}()

	for range 100 {
		clock.Advance(30 * time.Minute)
		require.True(t, wdt.Live())
		wdt.Reset()
	}
}
