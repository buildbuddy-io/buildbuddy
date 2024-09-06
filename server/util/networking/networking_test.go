package networking_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testnetworking"
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
		uniqueCIDRs[nets[i].HostIPWithCIDR()] = struct{}{}
	}

	// All CIDRs should be unique
	require.Equal(t, n, len(uniqueCIDRs))

	// Spot check some CIDRs / IPs for expected values
	assert.Equal(t, "192.168.0.5", nets[0].HostIP())
	assert.Equal(t, "192.168.0.5/30", nets[0].HostIPWithCIDR())
	assert.Equal(t, "192.168.0.6", nets[0].NamespacedIP())
	assert.Equal(t, "192.168.0.6/30", nets[0].NamespacedIPWithCIDR())

	assert.Equal(t, "192.168.0.13", nets[1].HostIP())
	assert.Equal(t, "192.168.0.14", nets[1].NamespacedIP())

	assert.Equal(t, "192.168.33.69", nets[n-2].HostIP())
	assert.Equal(t, "192.168.33.70", nets[n-2].NamespacedIP())

	assert.Equal(t, "192.168.33.77", nets[n-1].HostIP())
	assert.Equal(t, "192.168.33.78", nets[n-1].NamespacedIP())

	// Clone IPs should be valid
	for i := range nets {
		require.False(t, strings.HasSuffix(nets[i].NamespacedIP(), ".0"), "invalid clone IP %s", nets[i].NamespacedIP())
		require.False(t, strings.HasSuffix(nets[i].NamespacedIP(), ".255"), "invalid clone IP %s", nets[i].NamespacedIP())
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
	unlockedCIDR := nets[i].HostIPWithCIDR()
	unlockedCloneIP := nets[i].NamespacedIP()
	nets[i].Unlock()

	net, err := a.Get()
	require.NoError(t, err)
	require.Equal(t, unlockedCIDR, net.HostIPWithCIDR())
	require.Equal(t, unlockedCloneIP, net.NamespacedIP())
}

func TestConcurrentSetupAndCleanup(t *testing.T) {
	testnetworking.Setup(t)

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

func TestContainerNetworking(t *testing.T) {
	testnetworking.Setup(t)

	// Enable IP forwarding and masquerading so that we can test external
	// traffic.
	ctx := context.Background()
	err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0)
	require.NoError(t, err)
	err = networking.EnableMasquerading(ctx)
	require.NoError(t, err)

	c1 := createContainerNetwork(ctx, t)
	c2 := createContainerNetwork(ctx, t)

	// Containers should be able to reach the loopback interface.
	netnsExec(t, c1.NetNamespace(), `ping -c 1 -W 1 127.0.0.1`)
	netnsExec(t, c2.NetNamespace(), `ping -c 1 -W 1 127.0.0.1`)

	// Containers should be able to reach external IPs.
	netnsExec(t, c1.NetNamespace(), `ping -c 1 -W 3 8.8.8.8`)
	netnsExec(t, c2.NetNamespace(), `ping -c 1 -W 3 8.8.8.8`)

	// DNS should work from inside the netns.
	netnsExec(t, c1.NetNamespace(), `ping -c 1 -W 3 example.com`)

	// Containers should not be able to reach each other.
	netnsExec(t, c1.NetNamespace(), `echo 'Pinging c1' && if ping -c 1 -W 1 `+c2.HostNetwork().NamespacedIP()+` ; then exit 1; fi`)
	netnsExec(t, c2.NetNamespace(), `echo 'Pinging c2' && if ping -c 1 -W 1 `+c1.HostNetwork().NamespacedIP()+` ; then exit 1; fi`)

	// Compute an IP that is likely on the same network as the default route IP,
	// e.g. if the default gateway IP is 192.168.0.1 then we want something like
	// 192.168.0.2 here.
	defaultIP, err := networking.DefaultIP(ctx)
	require.NoError(t, err)
	ipOnDefaultNet := net.IP(append([]byte{}, defaultIP...))
	ipOnDefaultNet[3] = byte((int(ipOnDefaultNet[3])+1)%255 + 1)

	// Negative connectivity tests with src IP spoofing
	for _, test := range []struct {
		name, src, dst string
	}{
		{name: "ping c2 namespaced IP as c2 host IP", src: c2.HostNetwork().HostIP(), dst: c2.HostNetwork().NamespacedIP()},
		{name: "ping c2 host IP as default net IP", src: ipOnDefaultNet.String(), dst: c2.HostNetwork().HostIP()},
		{name: "ping c2 namespaced IP as default net IP", src: ipOnDefaultNet.String(), dst: c2.HostNetwork().NamespacedIP()},
		{name: "ping external internet as default net IP", src: ipOnDefaultNet.String(), dst: "8.8.8.8"},
		{name: "ping c2 namespaced IP from private range 172.18.x.x", src: "172.18.0.2", dst: c2.HostNetwork().NamespacedIP()},
		{name: "ping c2 namespaced IP from private range 10.x.x.x", src: "10.0.0.2", dst: c2.HostNetwork().NamespacedIP()},
		{name: "ping c2 namespaced IP as arbitrary IP", src: "177.21.42.2", dst: c2.HostNetwork().NamespacedIP()},
	} {
		netnsExec(t, c1.NetNamespace(), `
			VETH=$(ip link show | grep veth | perl -pe 's/^\d+: (.*?)@.*/\1/')
			if test -z "$VETH"; then
				echo >&2 'Could not find veth device' in namespace
				exit 1
			fi
			ip addr flush dev "$VETH"
			ip addr add `+test.src+` dev "$VETH"
			ip route add default via `+test.src+`

			echo >&2 "Checking that the src IP was correctly assigned to the namespaced veth device..."
			ping -c 1 -W 1 `+test.src+`

			if ping -c 1 -W 1 `+test.dst+` ; then
				echo >&2 "Network not properly configured: was unexpectedly able to reach `+test.dst+` from c1 by setting namespaced veth IP to `+test.src+` (`+test.name+`)"
				exit 1
			fi
		`)
	}
}

func TestContainerNetworkPool(t *testing.T) {
	testnetworking.Setup(t)

	ctx := context.Background()
	err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0)
	require.NoError(t, err)
	err = networking.EnableMasquerading(ctx)
	require.NoError(t, err)
	poolSizeLimit := -1 // use default
	pool := networking.NewContainerNetworkPool(poolSizeLimit)

	cn := createContainerNetwork(ctx, t)

	netnsExec(t, cn.NetNamespace(), `ping -c 1 -W 3 8.8.8.8`)

	ok := pool.Add(ctx, cn)
	require.True(t, ok, "add to pool")
	cn = pool.Get(ctx)
	require.NotNil(t, cn, "take from pool")

	netnsExec(t, cn.NetNamespace(), `ping -c 1 -W 3 8.8.8.8`)
}

func BenchmarkCreateContainerNetwork_Unpooled(b *testing.B) {
	ctx := context.Background()
	var eg errgroup.Group
	eg.SetLimit(runtime.NumCPU())

	for i := 0; i < b.N; i++ {
		eg.Go(func() error {
			n, err := networking.CreateContainerNetwork(ctx, false /*=loopbackOnly*/)
			require.NoError(b, err)
			err = n.Cleanup(ctx)
			require.NoError(b, err)
			return nil
		})
	}

	eg.Wait()
}

func BenchmarkCreateContainerNetwork_Pooled(b *testing.B) {
	ctx := context.Background()
	var eg errgroup.Group
	eg.SetLimit(runtime.NumCPU())

	sizeLimit := -1 // use default
	pool := networking.NewContainerNetworkPool(sizeLimit)

	for i := 0; i < b.N; i++ {
		eg.Go(func() error {
			n := pool.Get(ctx)
			if n == nil {
				var err error
				n, err = networking.CreateContainerNetwork(ctx, false /*=loopbackOnly*/)
				require.NoError(b, err)
			}
			if !pool.Add(ctx, n) {
				err := n.Cleanup(ctx)
				require.NoError(b, err)
			}
			return nil
		})
	}

	eg.Wait()

	err := pool.Shutdown(ctx)
	require.NoError(b, err)
}

func createContainerNetwork(ctx context.Context, t *testing.T) *networking.ContainerNetwork {
	c, err := networking.CreateContainerNetwork(ctx, false /*=loopbackOnly*/)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Cleanup(context.Background())
		require.NoError(t, err)
	})
	return c
}

func netnsExec(t *testing.T, ns, script string) string {
	b, err := exec.Command("ip", "netns", "exec", ns, "sh", "-eu", "-c", script).CombinedOutput()
	require.NoError(t, err, "%s", string(b))
	return string(b)
}
