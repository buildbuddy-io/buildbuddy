package testshell

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

// Run runs the given shell script in the given working directory.
func Run(t testing.TB, workDir, script string) string {
	cmd := exec.Command("/usr/bin/env", "bash", "-e", "-c", script)
	cmd.Dir = workDir
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "script %q failed: %s", script, string(b))
	return string(b)
}
