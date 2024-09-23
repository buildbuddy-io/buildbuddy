package testnetworking

import (
	"os"
	"os/exec"
	"strings"
	"testing"

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
}
