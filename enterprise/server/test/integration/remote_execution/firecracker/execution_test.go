package firecracker_test

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func testExecution(t *testing.T, rbe *firecrackerEnv) {
	for _, tc := range []struct {
		name, script, stdout, stderr string
		exitCode                     int
		properties                   []*repb.Platform_Property
	}{
		{name: "stdout_stderr_and_exit_code", script: `printf hello; printf goodbye >&2; exit 7`, stdout: "hello", stderr: "goodbye", exitCode: 7},
		{name: "environment_and_working_directory", script: `test "$GREETING" = 'hello world'; test "$PWD" = /workspace; printf '%s' "$GREETING"`, stdout: "hello world"},
		{name: "non_root", script: `test "$(id -u)" != 0; test "$(id -un)" = nobody; touch writable; printf ok`, stdout: "ok", properties: []*repb.Platform_Property{{Name: "dockerUser", Value: "nobody"}}},
		{name: "numeric_uid_without_passwd_entry", script: `test "$(id -u)" = 1234; touch writable; printf ok`, stdout: "ok", properties: []*repb.Platform_Property{{Name: "dockerUser", Value: "1234"}}},
		{name: "small_scratch_disk", script: `test "$PWD" = /workspace; printf ok`, stdout: "ok", properties: []*repb.Platform_Property{{Name: "EstimatedFreeDiskBytes", Value: "1MB"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rbe := rbe.forTest(t)
			cmd := firecrackerCommand(tc.script, tc.properties...)
			cmd.EnvironmentVariables = []*repb.Command_EnvironmentVariable{{Name: "GREETING", Value: "hello world"}}
			res := rbe.Execute(cmd, &rbetest.ExecuteOpts{}).Wait()
			require.Equal(t, tc.exitCode, res.ExitCode, res.Stderr)
			require.Equal(t, tc.stdout, res.Stdout)
			require.Equal(t, tc.stderr, res.Stderr)
		})
	}
}

// Check the public CAS-to-workspace-to-CAS path, including lazy VFS reads.
// Files not declared as outputs must not be returned to the RE client.
func testInputsAndOutputs(t *testing.T, rbe *firecrackerEnv) {
	for _, vfs := range []bool{false, true} {
		t.Run(fmt.Sprintf("vfs=%t", vfs), func(t *testing.T) {
			t.Parallel()
			rbe := rbe.forTest(t)
			inputs := t.TempDir()
			testfs.WriteAllFileContents(t, inputs, map[string]string{
				"input.txt": "hello", "nested/input.txt": "world",
				"script.sh": "#!/bin/sh\nset -eu\ncat input.txt nested/input.txt\n",
			})
			require.NoError(t, os.Chmod(filepath.Join(inputs, "script.sh"), 0755))
			require.NoError(t, os.Symlink("input.txt", filepath.Join(inputs, "link.txt")))
			cmd := firecrackerCommand(`
./script.sh
[ "$(cat link.txt)" = hello ]
mkdir -p out/deep tree/deep
cat input.txt nested/input.txt > out/deep/result.txt
cp input.txt tree/deep/copied.txt
printf ignored > ignored.txt
`, &repb.Platform_Property{Name: "enable-vfs", Value: fmt.Sprint(vfs)})
			cmd.OutputFiles = []string{"out/deep/result.txt"}
			cmd.OutputDirectories = []string{"tree"}
			res := rbe.Execute(cmd, &rbetest.ExecuteOpts{InputRootDir: inputs}).Wait()
			require.Equal(t, 0, res.ExitCode, res.Stderr)
			require.Equal(t, "helloworld", res.Stdout)
			require.Empty(t, res.Stderr)
			outputs := rbe.DownloadOutputsToNewTempDir(res)
			require.Equal(t, "helloworld", testfs.ReadFileAsString(t, outputs, "out/deep/result.txt"))
			require.Equal(t, "hello", testfs.ReadFileAsString(t, outputs, "tree/deep/copied.txt"))
			_, err := os.Stat(filepath.Join(outputs, "ignored.txt"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func testLargeStdout(t *testing.T, rbe *firecrackerEnv) {
	const size = 10_000_000
	cmd := firecrackerCommand(fmt.Sprintf("yes | head -c %d", size))
	res := rbe.Execute(cmd, &rbetest.ExecuteOpts{}).Wait()
	require.Equal(t, 0, res.ExitCode, res.Stderr)
	require.Empty(t, res.Stderr)
	require.Equal(t, strings.Repeat("y\n", size/2), res.Stdout)
}

func testTimeoutPreservesDebugOutputs(t *testing.T, rbe *firecrackerEnv) {
	// Warm the image first; a slow registry should not consume our timeout.
	res := rbe.Execute(firecrackerCommand("true"), &rbetest.ExecuteOpts{}).Wait()
	require.Equal(t, 0, res.ExitCode, res.Stderr)
	cmd := firecrackerCommand(`
printf 'stdout\n'
printf 'stderr\n' >&2
printf 'output\n' > output.txt
sleep 120
`)
	cmd.OutputFiles = []string{"output.txt"}
	res = rbe.Execute(cmd, &rbetest.ExecuteOpts{ActionTimeout: 20 * time.Second}).MustTerminateAbnormally()
	require.True(t, status.IsDeadlineExceededError(res.Err), "got %v", res.Err)
	require.Equal(t, "stdout\n", res.Stdout)
	require.Equal(t, "stderr\n", res.Stderr)
	outputs := rbe.DownloadOutputsToNewTempDir(res)
	require.Equal(t, "output\n", testfs.ReadFileAsString(t, outputs, "output.txt"))
}

func testOrphanedProcessIsReaped(t *testing.T, rbe *firecrackerEnv) {
	// Synchronize the child using a FIFO. Poll /proc with bounded waits instead
	// of assuming the init process has reparented/reaped it immediately.
	cmd := firecrackerCommand(`
mkfifo release
sh -c 'sh -c "read message < release" & echo $! > child.pid' &
wait
pid=$(cat child.pid)
for i in $(seq 1 100); do
  if grep -q '^PPid:[[:space:]]*1$' /proc/$pid/status; then break; fi
  sleep 0.1
done
grep -q '^PPid:[[:space:]]*1$' /proc/$pid/status
printf 'exit\n' > release
for i in $(seq 1 100); do
  if [ ! -e /proc/$pid ]; then printf reaped; exit 0; fi
  sleep 0.1
done
cat /proc/$pid/status >&2
exit 1
`)
	res := rbe.Execute(cmd, &rbetest.ExecuteOpts{ActionTimeout: time.Minute}).Wait()
	require.Equal(t, 0, res.ExitCode, res.Stderr)
	require.Equal(t, "reaped", res.Stdout)
	require.Empty(t, res.Stderr)
}

func testConcurrentIO(t *testing.T, rbe *firecrackerEnv) {
	inputs := t.TempDir()
	data := strings.Repeat("0123456789abcdef", 64*1024)
	testfs.WriteAllFileContents(t, inputs, map[string]string{"input": data})
	var commands []*rbetest.Command
	for i := range 4 {
		cmd := firecrackerCommand(fmt.Sprintf(`
mkdir -p out
for n in $(seq 1 8); do
  cp input out/copy
  cmp input out/copy
  sync
  rm out/copy
done
cp input out/result
printf '%d'
`, i))
		cmd.OutputDirectories = []string{"out"}
		commands = append(commands, rbe.Execute(cmd, &rbetest.ExecuteOpts{InputRootDir: inputs, ActionTimeout: 2 * time.Minute}))
	}
	for i, cmd := range commands {
		res := cmd.Wait()
		require.Equal(t, 0, res.ExitCode, res.Stderr)
		require.Empty(t, res.Stderr)
		require.Equal(t, fmt.Sprint(i), res.Stdout)
		outputs := rbe.DownloadOutputsToNewTempDir(res)
		require.Equal(t, data, testfs.ReadFileAsString(t, outputs, "out/result"))
	}
}

// Neither HTTP response is released until both distinct actions have reached
// the barrier. A sequential executor cannot pass merely by returning two
// successful results: both guest commands must actually be running together.
func testExecutionsOverlap(t *testing.T, rbe *firecrackerEnv) {
	var mu sync.Mutex
	arrived := make(map[string]bool)
	gate := make(chan struct{})
	var release sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		id := req.URL.Query().Get("id")
		if id != "0" && id != "1" {
			http.Error(w, "unknown action", http.StatusBadRequest)
			return
		}
		mu.Lock()
		arrived[id] = true
		if len(arrived) == 2 {
			release.Do(func() { close(gate) })
		}
		mu.Unlock()
		timer := time.NewTimer(45 * time.Second)
		defer timer.Stop()
		select {
		case <-gate:
			fmt.Fprint(w, "overlapped")
		case <-req.Context().Done():
		case <-timer.C:
			http.Error(w, "second execution did not reach the barrier", http.StatusRequestTimeout)
		}
	}))
	ip, err := networking.DefaultIP(t.Context())
	require.NoError(t, err)
	listener, err := net.Listen("tcp4", net.JoinHostPort(ip.String(), "0"))
	require.NoError(t, err)
	require.NoError(t, server.Listener.Close())
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	var commands []*rbetest.Command
	for i := range 2 {
		cmd := firecrackerCommand(fmt.Sprintf("wget -q -T 50 -O - '%s/?id=%d'", server.URL, i),
			&repb.Platform_Property{Name: "network", Value: "external"})
		commands = append(commands, rbe.Execute(cmd, &rbetest.ExecuteOpts{ActionTimeout: time.Minute}))
	}
	var worker string
	for _, cmd := range commands {
		res := cmd.Wait()
		require.Equal(t, 0, res.ExitCode, res.Stderr)
		require.Empty(t, res.Stderr)
		require.Equal(t, "overlapped", res.Stdout)
		if worker == "" {
			worker = res.ActionResult.GetExecutionMetadata().GetWorker()
			require.NotEmpty(t, worker)
		} else {
			require.Equal(t, worker, res.ActionResult.GetExecutionMetadata().GetWorker())
		}
	}
}
