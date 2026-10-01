// Package testleak checks that tests don't leave goroutines running.
package testleak

import (
	"testing"

	"go.uber.org/goleak"
)

// Check fails the test if any goroutine started after Check is called is still
// running once the test's other cleanups have finished. Cleanups run in
// reverse order, so call Check before setting up anything that registers a
// cleanup, such as servers or test environments.
//
// opts are passed to goleak, and are typically goleak.IgnoreTopFunction or
// goleak.IgnoreAnyFunction options listing known leaks.
func Check(t testing.TB, opts ...goleak.Option) {
	opts = append(opts, goleak.IgnoreCurrent())
	t.Cleanup(func() {
		if err := goleak.Find(opts...); err != nil {
			t.Errorf("goroutines leaked by %s: %s", t.Name(), err)
		}
	})
}
