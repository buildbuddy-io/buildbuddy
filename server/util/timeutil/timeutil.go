package timeutil

import (
	"fmt"
	"time"
)

// ShortFormatDuration returns a human-readable, short, and approximate
// representation of the given duration.
func ShortFormatDuration(d time.Duration) string {
	if d > 24*time.Hour {
		return "> 1d"
	}
	if d >= 1*time.Hour {
		return fmt.Sprintf("%dh", d/time.Hour)
	}
	if d >= 1*time.Minute {
		return fmt.Sprintf("%dm", d/time.Minute)
	}
	if d >= 1*time.Second {
		return fmt.Sprintf("%ds", d/time.Second)
	}
	if d >= 1*time.Millisecond {
		return fmt.Sprintf("%dms", d/time.Millisecond)
	}
	return "< 1ms"
}

// Clock wraps the top-level `time` API in an interface so that time can be
// controlled in tests.
type Clock interface {
	Now() time.Time
}

type clock struct{}

func (*clock) Now() time.Time {
	return time.Now()
}

// NewClock returns a clock reflecting the actual time.
func NewClock() Clock {
	return &clock{}
}
