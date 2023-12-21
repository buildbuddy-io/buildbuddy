package versioncmd_test

import (
	"io"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/versioncmd"
	"github.com/stretchr/testify/require"
)

func TestHandleVersion(t *testing.T) {
	rescueStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	exitCode, err := versioncmd.HandleVersion([]string{"--cli"})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)

	err = w.Close()
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	os.Stdout = rescueStdout

	require.Contains(t, string(out), "unknown")
}
