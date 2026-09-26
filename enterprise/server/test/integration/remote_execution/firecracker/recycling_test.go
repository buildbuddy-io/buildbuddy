package firecracker_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func testSnapshotResumeReplacesWorkspace(t *testing.T, rbe *firecrackerEnv) {
	props := []*repb.Platform_Property{{Name: "recycle-runner", Value: "true"}}
	inputs := t.TempDir()
	testfs.WriteAllFileContents(t, inputs, map[string]string{
		"old.txt": "old input",
		"script.sh": `#!/bin/sh
set -eu
[ "$(cat old.txt)" = 'old input' ]
printf 'persistent guest state' > /root/marker
cat /proc/sys/kernel/random/boot_id
printf first > output.txt
`,
	})
	cmd := firecrackerCommand("sh script.sh", props...)
	cmd.OutputFiles = []string{"output.txt"}
	res := rbe.Execute(cmd, &rbetest.ExecuteOpts{InputRootDir: inputs, APIKey: rbe.APIKey1, ActionTimeout: time.Minute}).Wait()
	require.Equal(t, 0, res.ExitCode, res.Stderr)
	require.Empty(t, res.Stderr)
	bootID := res.Stdout
	require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\n$`, bootID)
	outputs := rbe.DownloadOutputsToNewTempDir(res)
	require.Equal(t, "first", testfs.ReadFileAsString(t, outputs, "output.txt"))

	// Completion precedes Pause. Wait for this execution's post-completion
	// metadata, never an executor-wide pool counter that other cases can change.
	waitForFirecrackerSnapshot(t, rbe, res)
	inputs = t.TempDir()
	testfs.WriteAllFileContents(t, inputs, map[string]string{
		"new.txt": "new input",
		"script.sh": `#!/bin/sh
set -eu
[ ! -e old.txt ]
[ ! -e output.txt ]
[ "$(cat new.txt)" = 'new input' ]
[ "$(cat /root/marker)" = 'persistent guest state' ]
cat /proc/sys/kernel/random/boot_id
printf second > output.txt
`,
	})
	// The same command/platform but entirely new CAS inputs. The guest marker
	// AND boot ID prevent fresh-boot fallback from passing this test.
	res = rbe.Execute(cmd, &rbetest.ExecuteOpts{InputRootDir: inputs, APIKey: rbe.APIKey1, ActionTimeout: time.Minute}).Wait()
	require.Equal(t, 0, res.ExitCode, res.Stderr)
	require.Empty(t, res.Stderr)
	require.Equal(t, bootID, res.Stdout, "must resume the original VM, not boot a new one")
	outputs = rbe.DownloadOutputsToNewTempDir(res)
	require.Equal(t, "second", testfs.ReadFileAsString(t, outputs, "output.txt"))
}
