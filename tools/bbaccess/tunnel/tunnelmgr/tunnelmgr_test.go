package tunnelmgr

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/config"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/credentials"
	"github.com/stretchr/testify/require"
)

// unreachableGateway returns a target that accepts nothing, so registration
// fails quickly.
func unreachableGateway(t *testing.T) config.Zone {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return config.Zone{Suffix: "foo.bb.internal", Gateway: "grpc://" + addr}
}

// TestDialFailsWhenGatewayIsUnreachable is a regression test for a self
// deadlock: the manager used to hold the tunnel's lock while dropping the
// failed tunnel, and dropping takes that same lock. Every caller hung forever
// the first time a gateway was unreachable — the most ordinary failure there
// is (VPN off, gateway down, wrong target).
func TestDialFailsWhenGatewayIsUnreachable(t *testing.T) {
	mgr := New(credentials.APIKey("api-key"), time.Minute)
	t.Cleanup(mgr.Close)

	zone := unreachableGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := mgr.Dial(ctx, zone, "host.foo.bb.internal", 22)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("Dial hung instead of returning an error")
	}
}

// TestConcurrentDialsToAFailingGatewayAllReturn covers the same path with
// several callers racing, which is what happens when a burst of DNS queries
// prewarms a gateway that is down.
func TestConcurrentDialsToAFailingGatewayAllReturn(t *testing.T) {
	mgr := New(credentials.APIKey("api-key"), time.Minute)
	t.Cleanup(mgr.Close)

	zone := unreachableGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			_, err := mgr.Dial(ctx, zone, "host.foo.bb.internal", 22)
			errs <- err
		})
	}

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(90 * time.Second):
		t.Fatal("concurrent dials hung")
	}

	close(errs)
	for err := range errs {
		require.Error(t, err)
	}
	// A failed tunnel must not be cached, or every later attempt inherits the
	// original failure without retrying.
	require.Empty(t, mgr.Status())
}

// TestRetryIsSkippedForPolicyDenials: a gateway that answers "not allowed" is
// working correctly, so re-registering would just produce a second denial and
// a second audit log entry.
func TestRetryIsSkippedForPolicyDenials(t *testing.T) {
	denial := status.PermissionDeniedError("target is not allowed")
	require.False(t, isRetriable(denial), "a relay response means the relay answered; the tunnel is fine")
	require.False(t, isRetriable(fmt.Errorf("dialing through relay: %w", denial)), "wrapped denials count too")
	require.True(t, isRetriable(context.DeadlineExceeded), "a timeout could be a stale tunnel worth rebuilding")
}
