package canary

import (
	"flag"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	enableCanaries = flag.Bool("app.enable_canaries", true, "If true, enable slow function canaries")
)

type CancelFunc func()
type CanaryFunc func(timeTaken time.Duration)

// StartWithLateFn starts a canary that will call lateFn every expectedDuration
// period (if not canceled) and doneFn once when canceled, if more than
// expectedDuration has elapsed by then.
//
// lateFn and doneFn may overlap if the canary is canceled while lateFn is
// running. Only the first call to the CancelFunc has any effect.
func StartWithLateFn(expectedDuration time.Duration, lateFn CanaryFunc, doneFn CanaryFunc) CancelFunc {
	if !*enableCanaries {
		return func() {}
	}

	start := time.Now()
	// The timer, or nil once canceled.
	var timer atomic.Pointer[time.Timer]
	fire := func() {
		t := timer.Load()
		if t == nil {
			return // canceled
		}
		lateFn(time.Since(start))
		t.Reset(expectedDuration)
		// cancel may have run during lateFn, while the timer was not armed.
		if timer.Load() == nil {
			t.Stop()
		}
	}
	// Publish the timer before arming it: if it fired first, fire would see
	// nil and the canary would silently stop.
	t := time.AfterFunc(time.Duration(math.MaxInt64), fire)
	timer.Store(t)
	t.Reset(expectedDuration)

	return func() {
		t := timer.Swap(nil) // lets fire know we cancelled.
		if t == nil {
			return // already canceled
		}
		t.Stop()
		if elapsed := time.Since(start); elapsed > expectedDuration {
			doneFn(elapsed)
		}
	}
}

// Start starts a canary that will warn about the blocked function after
// expectedDuration has passed. This is primarily useful when debugging
// something -- you can add a canary and it will fire if the function does not
// finish when expected.
//
// Example:
//
//	defer canary.Start("tag", 100 * time.Millisecond)()
func Start(tag string, expectedDuration time.Duration) CancelFunc {
	location := "unknown"
	if pc, _, _, ok := runtime.Caller(1); ok {
		details := runtime.FuncForPC(pc)
		_, line := details.FileLine(pc)
		location = fmt.Sprintf("%s (line %d)", details.Name(), line)
	}

	lateFunc := func(taken time.Duration) {
		log.Warningf("%s: %s still running after %s; should have finished in %s", tag, location, taken, expectedDuration)
	}

	doneFunc := func(taken time.Duration) {
		log.Warningf("%s: %s finished. Took %s; should have finished in %s", tag, location, taken, expectedDuration)
	}
	return StartWithLateFn(expectedDuration, lateFunc, doneFunc)
}
