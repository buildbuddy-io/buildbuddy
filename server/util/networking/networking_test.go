package networking_test

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const (
	tapDeviceName = "vmtap0"
	tapDeviceMac  = "7a:a8:fa:dc:76:b7"
	tapIP         = "192.168.241.1"
	tapAddr       = tapIP + "/29"
	vmIP          = "192.168.241.2"
	vmAddr        = vmIP + "/29"
	vmIface       = "eth0"
)

func TestHostNetAllocator(t *testing.T) {
	const n = 1000
	a := &networking.HostNetAllocator{}
	var nets [n]*networking.HostNet
	var err error
	uniqueCIDRs := map[string]struct{}{}

	// Reserve all possible CIDRs
	for i := 0; i < n; i++ {
		nets[i], err = a.Get()
		require.NoError(t, err, "Get(%d)", i)
		uniqueCIDRs[nets[i].String()] = struct{}{}
	}

	// All CIDRs should be unique
	require.Equal(t, n, len(uniqueCIDRs))

	// Spot check some CIDRs / IPs for expected values
	assert.Equal(t, "192.168.0.5/30", nets[0].String())
	assert.Equal(t, "192.168.0.3", nets[0].CloneIP())
	assert.Equal(t, "192.168.0.13/30", nets[1].String())
	assert.Equal(t, "192.168.0.11", nets[1].CloneIP())
	assert.Equal(t, "192.168.33.69/30", nets[n-2].String())
	assert.Equal(t, "192.168.33.67", nets[n-2].CloneIP())
	assert.Equal(t, "192.168.33.77/30", nets[n-1].String())
	assert.Equal(t, "192.168.33.75", nets[n-1].CloneIP())

	// Clone IPs should be valid
	for i := range nets {
		require.False(t, strings.HasSuffix(nets[i].CloneIP(), ".0"), "invalid clone IP %s", nets[i].CloneIP())
		require.False(t, strings.HasSuffix(nets[i].CloneIP(), ".255"), "invalid clone IP %s", nets[i].CloneIP())
	}

	// Attempting to get a new host net should now fail
	for i := 0; i < n; i++ {
		net, err := a.Get()
		require.Error(t, err)
		require.Nil(t, net)
	}

	// Unlock an arbitrary network - subsequent Get() should then return the
	// newly unlocked address
	i := rand.Intn(n)
	unlockedCIDR := nets[i].String()
	unlockedCloneIP := nets[i].CloneIP()
	nets[i].Unlock()

	net, err := a.Get()
	require.NoError(t, err)
	require.Equal(t, unlockedCIDR, net.String())
	require.Equal(t, unlockedCloneIP, net.CloneIP())
}

func TestConcurrentSetupAndCleanup(t *testing.T) {
	checkPermissions(t)

	ctx := context.Background()
	eg, gCtx := errgroup.WithContext(ctx)
	eg.SetLimit(8)
	for i := 0; i < 20; i++ {
		// Note: gCtx is only used for short-circuiting this loop.
		// Each goroutine is allowed to run to completion, to avoid leaving
		// things in a messy state.
		if gCtx.Err() != nil {
			break
		}
		eg.Go(func() error {
			id := uuid.New()
			if err := networking.CreateNetNamespace(ctx, id); err != nil {
				return err
			}
			if err := networking.CreateTapInNamespace(ctx, id, tapDeviceName); err != nil {
				return err
			}
			if err := networking.ConfigureTapInNamespace(ctx, id, tapDeviceName, tapAddr); err != nil {
				return err
			}
			if err := networking.BringUpTapInNamespace(ctx, id, tapDeviceName); err != nil {
				return err
			}
			vethPair, err := networking.SetupVethPair(ctx, id)
			if err != nil {
				return err
			}
			if err := networking.ConfigureNATForTapInNamespace(ctx, vethPair, vmIP); err != nil {
				return err
			}

			// Cleanup
			var errs []error
			if err := vethPair.Cleanup(ctx); err != nil {
				errs = append(errs, err)
			}
			if err := networking.RemoveNetNamespace(ctx, id); err != nil {
				errs = append(errs, err)
			}
			if len(errs) > 0 {
				return fmt.Errorf("cleanup failed: %v", errs)
			}
			return nil
		})
	}
	err := eg.Wait()
	require.NoError(t, err)
}

func checkPermissions(t *testing.T) {
	if b, err := exec.Command("sudo", "--non-interactive", "ip", "link").CombinedOutput(); err != nil {
		t.Logf("'sudo ip' failed: %q", strings.TrimSpace(string(b)))
		t.Skipf("test requires passwordless sudo for 'ip' command - run ./tools/enable_local_firecracker.sh")
	}
}
