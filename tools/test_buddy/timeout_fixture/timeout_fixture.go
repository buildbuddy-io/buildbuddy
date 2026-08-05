// Package timeout_fixture provides an intentional TestBuddy reporting fixture.
package timeout_fixture

import "time"

func SleepDuration() time.Duration {
	return 10 * time.Minute
}
