package testclock

import (
	"time"
)

type TestClock struct {
	now time.Time
}

func StartingAt(t time.Time) *TestClock {
	return &TestClock{now: t}
}

func (c *TestClock) Now() time.Time {
	return c.now
}

// Set updates the time returned by Now().
func (c *TestClock) Set(t time.Time) {
	c.now = t
}
