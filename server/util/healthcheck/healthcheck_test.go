package healthcheck

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestHandleSignals_ReturnsAfterShutdownWithoutSignal(t *testing.T) {
	hc := NewHealthChecker("test")
	signalChan := make(chan os.Signal, 1)
	returned := make(chan struct{})
	go func() {
		hc.handleSignals(signalChan)
		close(returned)
	}()

	hc.Shutdown()
	hc.WaitForGracefulShutdown()

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("handleSignals did not return after shutdown")
	}
}

func TestHandleSignals_SignalStartsShutdown(t *testing.T) {
	hc := NewHealthChecker("test")
	signalChan := make(chan os.Signal, 1)
	// After a signal, handleSignals keeps handling signals until the process
	// exits, so it doesn't return.
	go hc.handleSignals(signalChan)

	signalChan <- syscall.SIGTERM

	select {
	case <-hc.done:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish after a signal")
	}
}
