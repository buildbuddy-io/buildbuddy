package timeutil

import (
	"time"
)

// UnixMillis converts a Unix timestamp in milliseconds to a `time.Time`.
func UnixMillis(timestamp int64) time.Time {
	sec := timestamp / 1e3
	nsec := (timestamp - sec*1e3) * 1e6
	return time.Unix(sec, nsec)
}
