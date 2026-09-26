package firecracker_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const routedProbeBody = "firecracker-forwarded-network-probe\n"
const routedProbeEnv = "BUILDBUDDY_ROUTED_NETWORK_PROBE_IP"

type routedNetworkProbe struct{ url string }

func routedProbeCommand(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput()
	return string(out), err
}

// Call at suite scope BEFORE starting the executor. Its later -I FORWARD rules
// must precede our narrowly scoped ACCEPTs, so a LOCAL REJECT cannot be bypassed.
// Cleanup is root-owned and LIFO: executor, child, rules, veth, namespace.
func newRoutedNetworkProbe(t *testing.T) *routedNetworkProbe {
	t.Helper()
	require.Zero(t, os.Geteuid(), "routed network probe requires root; see README.md")
	t.Setenv("PATH", os.Getenv("PATH")+":/usr/sbin:/sbin")
	forwarding, err := os.ReadFile("/proc/sys/net/ipv4/ip_forward")
	require.NoError(t, err)
	require.Equal(t, "1", strings.TrimSpace(string(forwarding)), "enable host IPv4 forwarding before running Firecracker tests; see README.md")
	run := func(args ...string) string {
		t.Helper()
		out, err := routedProbeCommand(args...)
		require.NoError(t, err, "%v: %s", args, out)
		return out
	}
	cleanup := func(args ...string) {
		t.Cleanup(func() {
			if out, err := routedProbeCommand(args...); err != nil {
				t.Errorf("cleanup %v: %s: %v", args, out, err)
			}
		})
	}

	// All test binaries share this lock. Keep its inode permanently; closing
	// releases the lock. The connected route leases the /30 until link deletion.
	lock, err := os.OpenFile("/tmp/buildbuddy-fc-e2e-routed-probe.lock", os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	defer lock.Close()
	require.Eventually(t, func() bool {
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		return err == nil
	}, 15*time.Second, 20*time.Millisecond, "lease routed probe subnet")
	var id [16]byte
	_, err = rand.Read(id[:])
	require.NoError(t, err)
	ns := fmt.Sprintf("rp%x", id[:6])
	host, peer := ns+"h", ns+"n" // Linux interface names are at most 15 bytes.
	var gateway, endpoint string
	deadline := time.Now().Add(15 * time.Second)
	for i := 0; i < 32768 && time.Now().Before(deadline); i++ {
		n := (int(id[6])*256 + int(id[7]) + i) % 32768
		prefix, base := fmt.Sprintf("198.%d.%d", 18+n/16384, (n/64)%256), (n%64)*4
		subnet := fmt.Sprintf("%s.%d/30", prefix, base)
		if strings.TrimSpace(run("ip", "-4", "route", "show", "table", "all", "exact", subnet)) == "" {
			gateway, endpoint = fmt.Sprintf("%s.%d", prefix, base+1), fmt.Sprintf("%s.%d", prefix, base+2)
			break
		}
	}
	require.NotEmpty(t, endpoint, "no free benchmark /30 for routed probe")
	// 198.18/15 is not RFC1918 or the host's primary IP: neither private-range
	// denial nor task_allowed_private_ips=default can mask LOCAL's policy.
	run("ip", "netns", "add", ns)
	cleanup("ip", "netns", "delete", ns)
	run("ip", "link", "add", host, "type", "veth", "peer", "name", peer)
	cleanup("ip", "link", "delete", host)
	run("ip", "link", "set", peer, "netns", ns)
	run("ip", "addr", "add", gateway+"/30", "dev", host)
	run("ip", "link", "set", host, "up")
	run("ip", "-n", ns, "addr", "add", endpoint+"/30", "dev", peer)
	run("ip", "-n", ns, "link", "set", peer, "up")
	run("ip", "-n", ns, "link", "set", "lo", "up")
	// Return guest traffic through the host without adding SNAT or public routes.
	run("ip", "-n", ns, "route", "add", "default", "via", gateway)
	for _, rule := range [][]string{
		{"FORWARD", "-o", host, "-d", endpoint, "-j", "ACCEPT"},
		{"FORWARD", "-i", host, "-s", endpoint, "-j", "ACCEPT"},
	} {
		run(append([]string{"iptables", "--wait", "5", "-I"}, rule...)...)
		cleanup(append([]string{"iptables", "--wait", "5", "-D"}, rule...)...)
	}

	binary, err := os.Executable()
	require.NoError(t, err)
	logFile, err := os.Create(filepath.Join(t.TempDir(), "probe.log"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = logFile.Close() })
	ready, writer, err := os.Pipe()
	require.NoError(t, err)
	defer ready.Close()
	defer writer.Close()
	cmd := exec.Command("ip", "netns", "exec", ns, binary, "-test.run=^TestRoutedNetworkProbeProcess$", "-test.timeout=0")
	cmd.Env = append(os.Environ(), routedProbeEnv+"="+endpoint)
	cmd.ExtraFiles = []*os.File{writer}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	require.NoError(t, cmd.Start())
	_ = writer.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("routed probe child did not exit after SIGKILL")
		}
		if t.Failed() {
			out, _ := os.ReadFile(logFile.Name())
			t.Logf("routed probe child: %s", out)
		}
	})
	address := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(ready)
		scanner.Scan()
		address <- scanner.Text()
	}()
	p := &routedNetworkProbe{}
	select {
	case addr := <-address:
		require.True(t, strings.HasPrefix(addr, endpoint+":"), "invalid readiness handshake: %q", addr)
		p.url = "http://" + addr + "/"
	case <-time.After(10 * time.Second):
		t.Fatal("routed probe child readiness timed out")
	}
	p.check(t)
	return p
}

func (p *routedNetworkProbe) check(t *testing.T) {
	t.Helper()
	// Independent host-side positive control, including around every denial.
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	res, err := client.Get(p.url)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1024))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, routedProbeBody, string(body))
}

// Re-exec the test binary only inside the endpoint namespace. FD 3 reports the
// actual listener after bind, avoiding sleeps, fixed ports, and external tools.
func TestRoutedNetworkProbeProcess(t *testing.T) {
	ip := os.Getenv(routedProbeEnv)
	if ip == "" {
		t.Skip("subprocess helper")
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort(ip, "0"))
	require.NoError(t, err)
	ready := os.NewFile(3, "ready")
	_, err = fmt.Fprintln(ready, listener.Addr().String())
	require.NoError(t, err)
	require.NoError(t, ready.Close())
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, routedProbeBody)
	})}
	require.NoError(t, server.Serve(listener))
}
