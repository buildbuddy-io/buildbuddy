package canary

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	enableCanaries = flag.Bool("app.enable_canaries", true, "If true, enable slow function canaries")
)

type CancelFunc func()
type CanaryFunc func(timeTaken time.Duration)

// StartWithLateFn starts a canary that will call lateFn every expectedDuration
// period (if not canceled) and once when canceled.
func StartWithLateFn(expectedDuration time.Duration, lateFn CanaryFunc, doneFn CanaryFunc) CancelFunc {
	if !*enableCanaries {
		return func() {}
	}

	done := make(chan struct{})
	canceled := false
	cancel := func() {
		if !canceled {
			close(done)
			canceled = true
		}
	}

	start := time.Now()
	go func() {
		t := time.NewTimer(expectedDuration)
		defer t.Stop()
		for {
			select {
			case <-done:
				if time.Since(start) > expectedDuration {
					doneFn(time.Since(start))
				}
				return
			case <-t.C:
				lateFn(time.Since(start))
			}
		}
	}()
	return cancel
}

// Start starts a canary that will warn about the blocked function after
// expectedDuration has passed. This is primarily useful when debugging
// something -- you can add a canary and it will fire if the function does not
// finish when expected.
//
// Example:
//
//	defer canary.Start(100 * time.Millisecond)()
func Start(expectedDuration time.Duration) CancelFunc {
	location := "unknown"
	if pc, _, _, ok := runtime.Caller(1); ok {
		details := runtime.FuncForPC(pc)
		_, line := details.FileLine(pc)
		location = fmt.Sprintf("%s (line %d)", details.Name(), line)
	}

	lateFunc := func(taken time.Duration) {
		log.Warningf("%s still running after %s; should have finished in %s", location, taken, expectedDuration)
	}

	doneFunc := func(taken time.Duration) {
		log.Warningf("%s finished. Took %s; should have finished in %s", location, taken, expectedDuration)
	}
	return StartWithLateFn(expectedDuration, lateFunc, doneFunc)
}
