package vmexec_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vmexec_client"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
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
	stdio := &interfaces.Stdio{
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

func TestExecStreamed_LargeOutput(t *testing.T) {
	client := startExecService(t)
	// Write about 600 KB of distinct lines to stdout and stderr. The server reads
	// output in chunks of up to 32 KiB, usually faster than it can stream them
	// to the client, so several chunks are typically waiting to be sent while
	// the next chunk is read.
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			seq 1 100000
			seq 1 100000 >&2
		`},
	}
	var expected strings.Builder
	for i := range 100_000 {
		fmt.Fprintf(&expected, "%d\n", i+1)
	}

	res := vmexec_client.Execute(t.Context(), client, cmd, ".", "" /*=user*/, nil /*=statsListener*/, nil /*=stdio*/)

	// Every chunk should be sent with the contents it had when it was read, so
	// the output received by the client should match exactly. Compare with ==
	// to avoid printing a huge diff on failure.
	require.NoError(t, res.Error)
	stdout := string(res.Stdout)
	stderr := string(res.Stderr)
	assert.Equal(t, expected.Len(), len(stdout))
	assert.True(t, stdout == expected.String(), "stdout should match")
	assert.Equal(t, expected.Len(), len(stderr))
	assert.True(t, stderr == expected.String(), "stderr should match")
	assert.Equal(t, 0, res.ExitCode)
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

			# Wait a little bit for the output to reach the vmexec runner.
			sleep 0.1

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

func TestExecStreamed_Crash(t *testing.T) {
	ctx := context.Background()
	client := startExecService(t)
	wd := testfs.MakeTempDir(t)
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			echo foo-stdout >&1
			echo bar-stderr >&2
			kill 0 -KILL
		`},
	}

	res := vmexec_client.Execute(ctx, client, cmd, wd, "" /*=user*/, nil /*=statsListener*/, nil /*=stdio*/)

	assert.Error(t, res.Error)

	assert.True(t, status.IsResourceExhaustedError(res.Error), "expected ResourceExhausted but got %T: %s", res.Error, res.Error)
	assert.Equal(t, "foo-stdout\n", string(res.Stdout), "should get partial stdout despite crash")
	assert.Equal(t, "bar-stderr\n", string(res.Stderr), "should get partial stderr despite crash")
	assert.Equal(t, commandutil.KilledExitCode, res.ExitCode)
}

func startExecService(t *testing.T) vmxpb.ExecClient {
	lis, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		lis.Close()
	})
	server := grpc.NewServer()
	execServer, err := vmexec.NewServer("" /*=workspaceDevice*/)
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
