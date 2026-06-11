// Package testhosts provides helpers for configuring static hostname
// mappings in /etc/hosts for the duration of a test.
//
// Modifying /etc/hosts requires root privileges, so tests using this package
// are expected to run with workload isolation (e.g. firecracker), where the
// test runs as root and any modifications are scoped to the VM.
package testhosts

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const hostsFilePath = "/etc/hosts"

// Add appends entries to /etc/hosts mapping each of the given hostnames to
// the given IP address, and registers a cleanup function to restore the
// original /etc/hosts contents when the test completes.
//
// The test is skipped if it is not running as root, since root privileges
// are required to modify /etc/hosts.
func Add(t *testing.T, ip string, hostnames ...string) {
	if os.Geteuid() != 0 {
		t.Skipf("test requires root privileges to modify %s", hostsFilePath)
	}
	original, err := os.ReadFile(hostsFilePath)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Note: the mode is ignored since the file already exists.
		require.NoError(t, os.WriteFile(hostsFilePath, original, 0644))
	})
	contents := string(original)
	if !strings.HasSuffix(contents, "\n") {
		contents += "\n"
	}
	contents += fmt.Sprintf("%s %s\n", ip, strings.Join(hostnames, " "))
	require.NoError(t, os.WriteFile(hostsFilePath, []byte(contents), 0644))
}
