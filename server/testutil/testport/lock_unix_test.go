//go:build unix

package testport

import (
	"runtime"
	"testing"
	"time"
)

func TestPortLockSurvivesGC(t *testing.T) {
	port := FindFree(t)
	if lockPort(port) {
		t.Fatal("port lease did not acquire a lock")
	}
	for range 10 {
		runtime.GC()
		// Give file finalizers a chance to run.
		time.Sleep(10 * time.Millisecond)
		if lockPort(port) {
			t.Fatal("garbage collection released a live port lease")
		}
	}
}
