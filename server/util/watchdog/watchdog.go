// Package watchdog implements a simple watchdog timer which expires if not
// reset periodically. https://en.wikipedia.org/wiki/Watchdog_timer
package watchdog

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

// Timer is a watchdog timer. It is designed to be reset periodically
// to avoid expiration.
//
// Example usage:
//
//	wdt := watchdog.NewTimer(time.Minute)
//	go func() {
//	  if !wdt.Live() {
//	    panic("watchdog timer expired!")
//	  }
//	)()
//	for {
//	   doExpensiveOperation()
//	   wdt.Reset()
//	}
type Timer struct {
	start    time.Time
	lifetime time.Duration
	clock    clockwork.Clock
	mu       *sync.Mutex
}

func New(lifetime time.Duration) *Timer {
	return NewWithClock(clockwork.NewRealClock(), lifetime)
}

func NewWithClock(clock clockwork.Clock, lifetime time.Duration) *Timer {
	wdt := &Timer{
		clock:    clock,
		start:    clock.Now(),
		lifetime: lifetime,
		mu:       &sync.Mutex{},
	}
	return wdt
}

func (t *Timer) nextReset() time.Time {
	return t.start.Add(t.lifetime)
}

// Reset() resets the watchdog timer (even if it has expired), so that it will
// not expire for another lifetime.
func (t *Timer) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.start = t.clock.Now().Add(t.lifetime)
}

// Statusz implements the statusz handler to display a watchdog timer's state.
// Example usage:
//
//	wdt := watchdog.NewTimer(time.Minute)
//	statusz.AddSection("watchdog", "Watchdog Timer", wdt)
func (t *Timer) Statusz(ctx context.Context) string {
	t.mu.Lock()
	defer t.mu.Unlock()

	var remainingTime string
	if t.lifetime == 0 {
		remainingTime = "∞"
	} else {
		remainingTime = t.nextReset().Sub(t.clock.Now()).String()
	}
	return fmt.Sprintf("Time to live: %s", remainingTime)
}

// Live() returns a boolean indicating if the watchdog timer is still alive
// (true) or has expired (false).
func (t *Timer) Live() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.lifetime == 0 {
		return true
	}
	return t.clock.Now().Before(t.nextReset())
}
