package testnetworking

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/stretchr/testify/require"
)

// Setup sets up the test to be able to call networking functions.
// It skips the test if the required net tools aren't available.
func Setup(t *testing.T) {
	// Ensure ip tools are in PATH
	os.Setenv("PATH", os.Getenv("PATH")+":/usr/sbin:/sbin")

	// Make sure the 'ip' tool is available and that we have the necessary
	// permissions to use it.
	cmd := []string{"ip", "link"}
	if os.Getuid() != 0 {
		cmd = append([]string{"sudo", "--non-interactive"}, cmd...)
	}
	if b, err := exec.Command(cmd[0], cmd[1:]...).CombinedOutput(); err != nil {
		t.Logf("%s failed: %s: %s", cmd, err, strings.TrimSpace(string(b)))
		t.Skipf("test requires passwordless sudo for 'ip' command - run ./tools/enable_local_firecracker.sh")
	}

	// Ensure IP forwarding is enabled
	b, err := os.ReadFile("/proc/sys/net/ipv4/ip_forward")
	require.NoError(t, err)
	if strings.TrimSpace(string(b)) != "1" {
		os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1"), 0)
		require.NoError(t, err, "enable IPv4 forwarding")
	}

	// Configure networking
	err = networking.Configure(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := networking.Cleanup(context.Background())
		require.NoError(t, err, "cleanup networking")
	})
}

// PacketCapture represents a packet capture process. A packet capture can be
// started with StartPacketCapture, and stopped by calling Stop and Wait on the
// returned instance. The resulting packet capture can be viewed with a program
// like wireshark.
//
// NOTE: this is intended for debugging purposes only and should not be used in
// production.
type PacketCapture struct {
	cmd    *exec.Cmd
	stderr *bytes.Buffer
	done   chan struct{}
	err    error
}

// StartPacketCapture starts a packet capture on the given device, writing
// captured packets to the given writer. The resulting packet capture can be
// viewed with a program like wireshark.
//
// Requires tcpdump to be available on $PATH.
func StartPacketCapture(device string, w io.Writer) (*PacketCapture, error) {
	stderr := &bytes.Buffer{}
	args := []string{"tcpdump", "-i", device, "-w", "-"}
	if os.Getuid() != 0 {
		args = append([]string{"sudo", "-A"}, args...)
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = w
	lockbuf := lockingbuffer.New()
	cmd.Stderr = io.MultiWriter(stderr, lockbuf)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	pcap := &PacketCapture{
		cmd:    cmd,
		stderr: stderr,
		done:   make(chan struct{}),
	}
	go func() {
		err := cmd.Wait()
		pcap.err = err
		close(pcap.done)
	}()
	for !strings.Contains(lockbuf.String(), "listening on") {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-pcap.done:
			return nil, pcap.Wait()
		}
	}
	return pcap, nil
}

// Stop stops packet capture.
// Call Wait() to wait for the packet capture to finish.
func (p *PacketCapture) Stop() error {
	// Wait a bit for packets to be captured - it takes some time for tcpdump
	// to actually capture the packets even if they have already been sent.
	// TODO: there has to be a better way to do this...
	time.Sleep(1 * time.Second)
	return p.cmd.Process.Signal(os.Interrupt)
}

func (p *PacketCapture) Wait() error {
	<-p.done
	// Ignore SIGINT triggered by Stop()
	if ws, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGINT {
		log.Debugf("Stopped packet capture. tcpdump stderr: %q", p.stderr)
		return nil
	}
	if p.err != nil {
		return fmt.Errorf("packet capture failed: %w: %q", p.err, p.stderr)
	}
	log.Debugf("Stopped packet capture. tcpdump stderr: %q", p.stderr)
	return nil
}
