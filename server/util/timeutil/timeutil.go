package timeutil

import (
	"fmt"
	"time"
)

// FromMillis converts a Unix timestamp in milliseconds to a Time.
func FromMillis(timestampMillis int64) time.Time {
	return time.Unix(0, timestampMillis*int64(time.Millisecond))
}

// FromUsec converts a Unix timestamp in microseconds to a Time.
func FromUsec(timestampUsec int64) time.Time {
	return time.Unix(0, timestampUsec*int64(time.Microsecond))
}

// ToMillis converts a Time to a timestamp in milliseconds since the Unix epoch.
func ToMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

// ToUsec converts a Time to a timestamp in microseconds since the Unix epoch.
func ToUsec(t time.Time) int64 {
	return t.UnixNano() / int64(time.Microsecond)
}

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
