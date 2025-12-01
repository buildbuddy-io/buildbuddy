package testshell

import (
	"bytes"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

// Run runs the given shell script in the given working directory,
// returning the combined output. It fails the test if the script fails.
func Run(t testing.TB, workDir, script string) string {
	cmd := exec.Command("/usr/bin/env", "bash", "-e", "-c", script)
	cmd.Dir = workDir
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "script %q failed: %s", script, string(b))
	return string(b)
}

// Try runs the given shell script in the given working directory, and does not
// fail the test if the script fails. Instead, it returns stdout, stderr, and an
// error as separate values, which allow tests to assert on the values
// separately.
func Try(t testing.TB, workDir, script string) (string, string, error) {
	cmd := exec.Command("/usr/bin/env", "bash", "-e", "-c", script)
	cmd.Dir = workDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}
