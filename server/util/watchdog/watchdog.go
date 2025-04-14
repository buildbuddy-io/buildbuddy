package watchdog

import (
	"context"
	"fmt"
	"time"

	"github.com/jonboulle/clockwork"
)

type Timer struct {
	start    time.Time
	lifetime time.Duration
	clock    clockwork.Clock
}

func New(lifetime time.Duration) *Timer {
	return NewWithClock(clockwork.NewRealClock(), lifetime)
}

func NewWithClock(clock clockwork.Clock, lifetime time.Duration) *Timer {
	wdt := &Timer{
		clock:    clock,
		start:    clock.Now(),
		lifetime: lifetime,
	}
	return wdt
}

func (t *Timer) nextReset() time.Time {
	return t.start.Add(t.lifetime)
}
func (t *Timer) Reset() {
	t.start = t.clock.Now().Add(t.lifetime)
}

func (t *Timer) Statusz(ctx context.Context) string {
	var remainingTime string
	if t.lifetime == 0 {
		remainingTime = "∞"
	} else {
		remainingTime = t.nextReset().Sub(t.clock.Now()).String()
	}
	return fmt.Sprintf("Time to live: %s", remainingTime)
}

func (t *Timer) Live() bool {
	if t.lifetime == 0 {
		return true
	}
	return t.clock.Now().Before(t.nextReset())
}
