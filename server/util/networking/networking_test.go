package networking_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testnetworking"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
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
	a, err := networking.NewHostNetAllocator("192.168.0.0/16")
	require.NoError(t, err)
	var nets [n]*networking.HostNet
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
	ctx := context.Background()
	testnetworking.Setup(t)

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
			network, err := networking.CreateVMNetwork(ctx, tapDeviceName, tapAddr, vmIP)
			if err != nil {
				return fmt.Errorf("create VM network: %w", err)
			}
			if err := network.Cleanup(ctx); err != nil {
				return fmt.Errorf("cleanup VM network: %w", err)
			}
			return nil
		})
	}
	err := eg.Wait()
	require.NoError(t, err)
}

func TestContainerNetworking(t *testing.T) {
	testnetworking.Setup(t)

	ctx := context.Background()
	err := networking.EnableMasquerading(ctx)
	require.NoError(t, err)

	defaultIP, err := networking.DefaultIP(ctx)
	require.NoError(t, err)

	c1 := createContainerNetwork(ctx, t)
	c2 := createContainerNetwork(ctx, t)

	// Containers should be able to reach the loopback interface.
	netnsExec(t, c1.NamespacePath(), `ping -c 1 -W 1 127.0.0.1`)
	netnsExec(t, c2.NamespacePath(), `ping -c 1 -W 1 127.0.0.1`)

	// Containers should be able to reach external IPs.
	netnsExec(t, c1.NamespacePath(), `ping -c 1 -W 3 8.8.8.8`)
	netnsExec(t, c2.NamespacePath(), `ping -c 1 -W 3 8.8.8.8`)

	// DNS should work from inside the netns.
	netnsExec(t, c1.NamespacePath(), `ping -c 1 -W 3 google.com`)

	// Containers should not be able to reach each other.
	netnsExec(t, c1.NamespacePath(), `echo 'Pinging c1' && if ping -c 1 -W 1 `+c2.HostNetwork().NamespacedIP()+` ; then exit 1; fi`)
	netnsExec(t, c2.NamespacePath(), `echo 'Pinging c2' && if ping -c 1 -W 1 `+c1.HostNetwork().NamespacedIP()+` ; then exit 1; fi`)

	// Containers should not be able to reach the default interface IP.
	netnsExec(t, c1.NamespacePath(), `if ping -c 1 -W 1 `+defaultIP.String()+` ; then exit 1; fi`)

	// Compute an IP that is likely on the same network as the default route IP,
	// e.g. if the default gateway IP is 192.168.0.1 then we want something like
	// 192.168.0.2 here.
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
		netnsExec(t, c1.NamespacePath(), `
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

func TestAllowTrafficToHostDefaultIP(t *testing.T) {
	flags.Set(t, "executor.task_allowed_private_ips", []string{"default"})
	testnetworking.Setup(t)
	ctx := context.Background()
	err := networking.EnableMasquerading(ctx)
	require.NoError(t, err)
	defaultIP, err := networking.DefaultIP(ctx)
	require.NoError(t, err)

	// Create container network
	c1 := createContainerNetwork(ctx, t)

	// Should be able to reach host's default IP when flag is enabled
	netnsExec(t, c1.NamespacePath(), fmt.Sprintf("ping -c 1 -W 1 %s", defaultIP))
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

	netnsExec(t, cn.NamespacePath(), `ping -c 1 -W 3 8.8.8.8`)

	ok := pool.Add(ctx, cn)
	require.True(t, ok, "add to pool")
	cn = pool.Get(ctx)
	require.NotNil(t, cn, "take from pool")

	netnsExec(t, cn.NamespacePath(), `ping -c 1 -W 3 8.8.8.8`)
}

func TestNetworkStats(t *testing.T) {
	// Set this to true to enable packet capture for debugging. If an assertion
	// fails about metrics for a given interface, a packet capture from that
	// interface will be written as test output, which can be viewed with
	// wireshark to diagnose the unexpected behavior.
	//
	// NOTE: Set back to false before merging, since this adds overhead.
	const enablePcap = false

	// Max number of test cases that can be run concurrently (this test runs
	// each test case in serial, then runs all test cases concurrently).
	const maxConcurrency = 8

	// Start a tx/rx test server listening on the default interface since we
	// need an IP to be able to reach it from within the netns.
	server := httptest.NewUnstartedServer(http.HandlerFunc(trafficTestHandler))
	server.Listener = defaultInterfaceListener(t)
	server.Start()
	defer server.Close()
	// Allow traffic on the default interface.
	flags.Set(t, "executor.task_allowed_private_ips", []string{"default"})
	// Enable stats.
	flags.Set(t, "executor.network_stats_enabled", true)
	testnetworking.Setup(t)

	overrideSysctlsForTest(t, map[string]string{
		// Set large network buffer sizes to help reduce TCP re-transmissions,
		// which throw off stats.
		"net.core.rmem_max": "134217728",
		"net.core.wmem_max": "134217728",
		"net.ipv4.tcp_rmem": "4096 16384 67108864",
		"net.ipv4.tcp_wmem": "4096 16384 67108864",
	})

	type testCase struct {
		name         string
		tx           int64
		rx           int64
		pool         bool
		loopbackOnly bool
	}
	testCases := []testCase{
		{
			name: "no traffic",
			tx:   0,
			rx:   0,
		},
		{
			name: "no traffic with pooling",
			tx:   0,
			rx:   0,
			pool: true,
		},
		{
			name: "tx traffic",
			tx:   100_000,
			rx:   0,
		},
		{
			name: "tx traffic with pooling",
			tx:   0,
			rx:   0,
			pool: true,
		},
		{
			name: "rx traffic",
			tx:   0,
			rx:   100_000,
		},
		{
			name: "rx traffic pooled",
			tx:   0,
			rx:   100_000,
		},
		{
			name: "tx and rx traffic",
			tx:   50_000,
			rx:   100_000,
		},
		{
			name: "tx and rx traffic with pooling",
			tx:   50_000,
			rx:   100_000,
			pool: true,
		},
		{
			name:         "loopback only",
			tx:           0,
			rx:           0,
			loopbackOnly: true,
		},
	}

	getPool := func(ctx context.Context, t *testing.T) *networking.ContainerNetworkPool {
		pool := networking.NewContainerNetworkPool(maxConcurrency /*=poolSize*/)
		t.Cleanup(func() {
			err := pool.Shutdown(ctx)
			require.NoError(t, err)
		})
		return pool
	}

	runTestCase := func(ctx context.Context, t *testing.T, pool *networking.ContainerNetworkPool, tc testCase, attemptNumber int) {
		// We don't pool loopback-only networks in practice - sanity check that
		// we're not inadvertently testing this.
		require.False(t, tc.loopbackOnly && tc.pool, "loopback-only networks should not be pooled")

		var cn *networking.ContainerNetwork
		if tc.pool {
			cn = pool.Get(ctx)
		}
		if cn == nil {
			n, err := networking.CreateContainerNetwork(ctx, tc.loopbackOnly)
			require.NoError(t, err)
			cn = n
		}

		// Read stats initially; should be 0, even if we just took from
		// the pool.
		{
			stats, err := cn.Stats(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(0), stats.GetBytesReceived(), "%s (attempt %d): should have received 0 bytes initially", tc.name, attemptNumber)
			require.Equal(t, int64(0), stats.GetBytesSent(), "%s (attempt %d): should have sent 0 bytes initially", tc.name, attemptNumber)
			require.Equal(t, int64(0), stats.GetPacketsReceived(), "%s (attempt %d): should have received 0 packets initially", tc.name, attemptNumber)
			require.Equal(t, int64(0), stats.GetPacketsSent(), "%s (attempt %d): should have sent 0 packets initially", tc.name, attemptNumber)
		}

		var pcap *testnetworking.PacketCapture
		pcapBuf := &bytes.Buffer{}
		hostDevice := cn.HostDevice()
		if enablePcap && !tc.loopbackOnly {
			p, err := testnetworking.StartPacketCapture(hostDevice, pcapBuf)
			require.NoError(t, err)
			pcap = p
		}

		// If external networking is enabled, make a request to the test
		// server from within the namespace, sending and/or receiving
		// the expected amount of traffic.
		if !tc.loopbackOnly && (tc.tx > 0 || tc.rx > 0) {
			netnsExec(t, cn.NamespacePath(), `
				yes | head -c `+fmt.Sprint(tc.tx)+` |
					curl --data-binary @- `+server.URL+`?rx=`+fmt.Sprint(tc.rx)+`
			`)
		}

		// Get stats and verify that they match the expected values
		// within a reasonable tolerance.
		stats, err := cn.Stats(ctx)
		require.NoError(t, err)
		ok := true
		ok = ok && assert.GreaterOrEqual(t, stats.GetBytesReceived(), tc.rx, "%s (attempt %d): should have received at least %d bytes", tc.name, attemptNumber, tc.rx)
		ok = ok && assert.GreaterOrEqual(t, stats.GetBytesSent(), tc.tx, "%s (attempt %d): should have sent at least %d bytes", tc.name, attemptNumber, tc.tx)
		// Note: these tolerances are somewhat large to account for protocol
		// overhead (HTTP headers, TCP/IP headers, TCP acks, etc.) as well as
		// TCP re-transmissions which can happen if the machine is under heavy
		// load. If these assertions fail, consider setting enablePcap to true
		// to confirm whether it's a real issue, or just due to something like
		// TCP re-transmissions.
		ok = ok && assert.InDelta(t, tc.rx, stats.GetBytesReceived(), 16*1024, "%s (attempt %d): bytes received should be within 4KB of expected", tc.name, attemptNumber)
		ok = ok && assert.InDelta(t, tc.tx, stats.GetBytesSent(), 16*1024, "%s (attempt %d): bytes sent should be within 4KB of expected", tc.name, attemptNumber)
		if tc.tx > 0 {
			// Transmitted some bytes; should have sent several packets.
			ok = ok && assert.GreaterOrEqual(t, stats.GetPacketsSent(), int64(10), "%s (attempt %d): packets sent should be at least 1", tc.name, attemptNumber)
		} else if tc.rx > 0 {
			// Received some bytes; should have sent several ACK packets.
			ok = ok && assert.GreaterOrEqual(t, stats.GetPacketsSent(), int64(10), "%s (attempt %d): packets sent should be at least 1", tc.name, attemptNumber)
		} else {
			// No traffic; should have very few packets sent (might have some
			// ARP packets etc.)
			ok = ok && assert.LessOrEqual(t, stats.GetPacketsSent(), int64(10), "%s (attempt %d): packets sent should be smaller than 10", tc.name, attemptNumber)
		}
		if tc.rx > 0 {
			// Received some bytes; should have received several packets.
			ok = ok && assert.GreaterOrEqual(t, stats.GetPacketsReceived(), int64(10), "%s (attempt %d): packets received should be at least 1", tc.name, attemptNumber)
		} else if tc.tx > 0 {
			// Sent some bytes; should have received several ACK packets.
			ok = ok && assert.GreaterOrEqual(t, stats.GetPacketsReceived(), int64(10), "%s (attempt %d): packets received should be at least 1", tc.name, attemptNumber)
		} else {
			// No traffic; should have very few packets received (might have
			// some ARP packets etc.)
			ok = ok && assert.LessOrEqual(t, stats.GetPacketsReceived(), int64(10), "%s (attempt %d): packets received should be smaller than 10", tc.name, attemptNumber)
		}

		if pcap != nil {
			err = pcap.Stop()
			require.NoError(t, err)
			err = pcap.Wait()
			require.NoError(t, err)
		}
		// Write packet capture for the interface (for debugging purposes) if
		// the reported stats don't match the expected values.
		if pcap != nil && !ok {
			pcapPath := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR") + "/" + fmt.Sprintf("%s-%d-%s.pcap", strings.ReplaceAll(tc.name, " ", "_"), attemptNumber, hostDevice)
			err := os.WriteFile(pcapPath, pcapBuf.Bytes(), 0644)
			require.NoError(t, err)
			t.Logf("Wrote pcap to %s", pcapPath)
		}

		// Release the network.
		if !tc.loopbackOnly {
			if ok := pool.Add(ctx, cn); !ok {
				err := cn.Cleanup(ctx)
				require.NoError(t, err)
			}
		} else {
			err := cn.Cleanup(ctx)
			require.NoError(t, err)
		}
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			pool := getPool(ctx, t)
			// Run each test case multiple times. If pooling is enabled, then
			// this is testing that we're only reporting incremental metrics
			// rather than cumulative. If pooling is disabled, this is just
			// exercising the code a bit more.
			for attemptNumber := 1; attemptNumber <= 3; attemptNumber++ {
				runTestCase(ctx, t, pool, tc, attemptNumber)
			}
		})
	}

	// Run all tests again, this time running all test cases concurrently. This
	// is testing that traffic from one test case doesn't pollute the results
	// for other test cases, which could conceivably happen if our packet
	// routing isn't set up correctly.
	t.Run("all test cases concurrently", func(t *testing.T) {
		ctx := context.Background()
		pool := getPool(ctx, t)
		var eg errgroup.Group
		eg.SetLimit(maxConcurrency)
		for attemptNumber := 1; attemptNumber < 10; attemptNumber++ {
			rand.Shuffle(len(testCases), func(i, j int) {
				testCases[i], testCases[j] = testCases[j], testCases[i]
			})
			for _, tc := range testCases {
				eg.Go(func() error {
					runTestCase(ctx, t, pool, tc, attemptNumber)
					return nil
				})
			}
		}
		err := eg.Wait()
		require.NoError(t, err)
	})
}

func overrideSysctlsForTest(t *testing.T, values map[string]string) {
	oldValues := map[string]string{}
	for k, v := range values {
		out := testshell.Run(t, "" /*=workDir*/, fmt.Sprintf("sysctl -n %s", k))
		oldValues[k] = strings.TrimSpace(out)
		testshell.Run(t, "" /*=workDir*/, fmt.Sprintf("sysctl -w %s=%q", k, v))
	}
	t.Cleanup(func() {
		for k, v := range oldValues {
			testshell.Run(t, "" /*=workDir*/, fmt.Sprintf("sysctl -w %s=%q", k, v))
		}
	})
}

func defaultInterfaceIP(t *testing.T) net.IP {
	ip, err := networking.DefaultIP(context.Background())
	require.NoError(t, err)
	return ip
}

func defaultInterfaceListener(t *testing.T) net.Listener {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:0", defaultInterfaceIP(t)))
	require.NoError(t, err)
	return listener
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

func netnsExec(t *testing.T, nsPath, script string) string {
	b, err := exec.Command("ip", "netns", "exec", filepath.Base(nsPath), "sh", "-eu", "-c", script).CombinedOutput()
	require.NoError(t, err, "%s", string(b))
	return string(b)
}

// Handler that allows testing tx/rx traffic. Spin it up using httptest from
// stdlib. To test the send direction, make a request with a body payload of the
// desired size, and the handler will fully consume the request body. To test
// the receive direction, send a request with the "rx" query parameter set to
// the desired size, and the handler will respond with arbitrary response data
// of the same size.
func trafficTestHandler(w http.ResponseWriter, r *http.Request) {
	buf := bytes.Repeat([]byte{'x'}, 1024)
	// Fully consume the request body.
	io.Copy(io.Discard, r.Body)

	// If the request has a "rx" query parameter, respond with the specified
	// number of bytes.
	b := r.URL.Query().Get("rx")
	if b == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	n, err := strconv.Atoi(b)
	if err != nil {
		http.Error(w, "Invalid response_bytes", http.StatusBadRequest)
		return
	}
	w.Header().Add("Content-Length", strconv.Itoa(n))
	w.WriteHeader(http.StatusOK)
	for i := 0; i < n; i += len(buf) {
		if _, err := w.Write(buf[:min(len(buf), n-i)]); err != nil {
			return
		}
	}
}
