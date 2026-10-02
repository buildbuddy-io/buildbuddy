package ociruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestInvokeRuntime_ExitStatus(t *testing.T) {
	for _, tc := range []struct {
		name             string
		script           string
		wantExitCode     int
		wantRuntimeError bool
	}{
		{name: "success", script: "exit 0", wantExitCode: 0},
		{name: "action failure", script: "exit 1", wantExitCode: 1},
		{name: "action segfault", script: "exit 139", wantExitCode: commandutil.SegmentationFaultExitCode},
		{name: "runtime segfault", script: "kill -SEGV $$", wantExitCode: commandutil.NoExitCode, wantRuntimeError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A fake runtime lets us distinguish a forwarded action exit code
			// from a signal terminating the runtime itself, without containers.
			runtimePath := filepath.Join(t.TempDir(), "runtime")
			require.NoError(t, os.WriteFile(runtimePath, []byte("#!/bin/sh\n"+tc.script+"\n"), 0755))
			c := &ociContainer{runtime: runtimePath}
			res := c.invokeRuntime(context.Background(), &repb.Command{}, &interfaces.Stdio{}, 0, "exec")
			require.Equal(t, tc.wantExitCode, res.ExitCode)
			if tc.wantRuntimeError {
				require.True(t, status.IsUnavailableError(res.Error), "got: %v", res.Error)
				require.Contains(t, res.Error.Error(), "OCI runtime")
				require.Contains(t, res.Error.Error(), "SIGSEGV")
			} else {
				require.NoError(t, res.Error)
			}
		})
	}
}
