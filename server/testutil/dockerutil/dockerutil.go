package dockerutil

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// FindServerContainer lists running server by name, and if we find one
// that looks like `<prefix>-$PORT>, then return the port and the container
// name.
func FindServerContainer(t testing.TB, prefix string) (int, string) {
	cmd := exec.Command("docker", "ps", fmt.Sprintf("--filter=name=%s", prefix), "--format={{.Names}}")
	b, err := cmd.CombinedOutput()
	require.NoError(t, err)
	lines := strings.Split(string(b), "\n")
	for _, containerName := range lines {
		if containerName == "" {
			continue
		}
		port, err := strconv.Atoi(strings.TrimPrefix(containerName, prefix))
		require.NoError(t, err, "failed to parse container port from %q", containerName)
		return port, containerName
	}
	return 0, ""
}
