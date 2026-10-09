package healthcheck_test

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testleak"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/stretchr/testify/require"
)

func TestShutdown_StopsBackgroundGoroutines(t *testing.T) {
	testleak.CheckGoroutines(t)

	hc := healthcheck.NewHealthChecker("test")
	hc.Shutdown()
	hc.WaitForGracefulShutdown()
}

func TestSignal_StartsShutdown(t *testing.T) {
	hc := healthcheck.NewHealthChecker("test")

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))

	done := make(chan struct{})
	go func() {
		hc.WaitForGracefulShutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish after SIGTERM")
	}
}
