package vmexec_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vmexec_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

func TestExecStreamed_Simple(t *testing.T) {
	client := startExecService(t)
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			echo foo-stdout >&1
			echo bar-stderr >&2
			exit 7
		`},
	}

	res := vmexec_client.Execute(context.Background(), client, cmd, ".", "" /*=user*/, nil /*=statsListener*/, nil /*=stdio*/)

	require.NoError(t, res.Error)
	assert.Equal(t, "foo-stdout\n", string(res.Stdout))
	assert.Equal(t, "bar-stderr\n", string(res.Stderr))
	assert.Equal(t, 7, res.ExitCode)
}

func TestExecStreamed_Stdio(t *testing.T) {
	client := startExecService(t)
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			in=$(cat)
			if [[ "$in" != baz-stdin ]]; then
			  echo "unexpected stdin: $in" >&2
				exit 1
			fi

			echo foo-stdout >&1
			echo bar-stderr >&2
			exit 7
		`},
	}
	var stdout, stderr bytes.Buffer
	stdin := strings.NewReader("baz-stdin")
	stdio := &container.Stdio{
		Stdin:  stdin,
		Stdout: &stdout,
		Stderr: &stderr,
	}

	res := vmexec_client.Execute(context.Background(), client, cmd, ".", "" /*=user*/, nil /*=statsListener*/, stdio)

	require.NoError(t, res.Error)
	assert.Equal(t, "foo-stdout\n", stdout.String())
	assert.Equal(t, "bar-stderr\n", stderr.String())
	assert.Equal(t, 7, res.ExitCode)
}

func TestExecStreamed_Stats(t *testing.T) {
	wd := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, wd, map[string]string{
		"mem.py": useMemPythonScript(1e9, 1*time.Second),
		"cpu.py": useCPUPythonScript(1 * time.Second),
	})
	client := startExecService(t)
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			python3 ./mem.py &
			python3 ./cpu.py &
			wait
		`},
	}

	res := vmexec_client.Execute(context.Background(), client, cmd, wd, "" /*=user*/, nil /*=statsListener*/, nil /*=stdio*/)

	require.NoError(t, res.Error)
	require.NotNil(t, res.UsageStats)
	assert.GreaterOrEqual(t, res.UsageStats.GetCpuNanos(), int64(0.7e9), "should use around 1e9 CPU nanos")
	assert.LessOrEqual(t, res.UsageStats.GetCpuNanos(), int64(1.6e9), "should use around 1e9 CPU nanos")
	assert.GreaterOrEqual(t, res.UsageStats.GetMemoryBytes(), int64(1e9), "should use at least 1GB memory")
	assert.LessOrEqual(t, res.UsageStats.GetMemoryBytes(), int64(1.6e9), "shouldn't use much more than 1GB memory")
}

func TestExecStreamed_Timeout(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	client := startExecService(t)
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			echo foo-stdout >&1
			echo bar-stderr >&2
			sleep 60
		`},
	}

	res := vmexec_client.Execute(ctx, client, cmd, ".", "" /*=user*/, nil /*=statsListener*/, nil /*=stdio*/)

	assert.Error(t, res.Error)

	assert.True(t, status.IsDeadlineExceededError(res.Error), "expected DeadlineExceeded but got %T: %s", res.Error, res.Error)
	assert.Equal(t, "foo-stdout\n", string(res.Stdout), "should get partial stdout despite timeout")
	assert.Equal(t, "bar-stderr\n", string(res.Stderr), "should get partial stderr despite timeout")
	assert.Equal(t, commandutil.NoExitCode, res.ExitCode)
}

func TestExecStreamed_Cancel(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	client := startExecService(t)
	wd := testfs.MakeTempDir(t)
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			echo foo-stdout >&1
			echo bar-stderr >&2
			touch cancel
			sleep 60
		`},
	}

	go func() {
		err := disk.WaitUntilExists(ctx, path.Join(wd, "cancel"), disk.WaitOpts{})
		require.NoError(t, err)
		cancel()
	}()

	res := vmexec_client.Execute(ctx, client, cmd, wd, "" /*=user*/, nil /*=statsListener*/, nil /*=stdio*/)

	assert.Error(t, res.Error)

	assert.True(t, status.IsCanceledError(res.Error), "expected Canceled but got %T: %s", res.Error, res.Error)
	assert.Equal(t, "foo-stdout\n", string(res.Stdout), "should get partial stdout despite cancel")
	assert.Equal(t, "bar-stderr\n", string(res.Stderr), "should get partial stderr despite cancel")
	assert.Equal(t, commandutil.NoExitCode, res.ExitCode)
}

func startExecService(t *testing.T) vmxpb.ExecClient {
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		lis.Close()
	})
	server := grpc.NewServer()
	execServer, err := vmexec.NewServer(&sync.RWMutex{})
	require.NoError(t, err)
	vmxpb.RegisterExecServer(server, execServer)
	go server.Serve(lis)
	t.Cleanup(func() {
		server.Stop()
	})

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	client := vmxpb.NewExecClient(conn)
	return client
}

// Returns a python script that consumes 1 CPU core continuously for the given
// duration.
func useCPUPythonScript(dur time.Duration) string {
	return fmt.Sprintf(`
import time
end = time.time() + %f
while time.time() < end:
    pass
`, dur.Seconds())
}

// Returns a python script that uses the given amount of resident memory and
// holds onto that memory for the given duration.
func useMemPythonScript(memBytes int64, dur time.Duration) string {
	return fmt.Sprintf(`
import time
arr = b'1' * %d
time.sleep(%f)
`, memBytes, dur.Seconds())
}
