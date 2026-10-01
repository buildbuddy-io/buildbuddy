package smoke_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	akpb "github.com/buildbuddy-io/buildbuddy/proto/api_key"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const digestFunction = repb.DigestFunction_SHA256

// TestExecutorSmoke starts one real app and one real executor binary, both
// built from source, registers the executor using an executor API key, and
// then runs a set of actions against it in parallel.
func TestExecutorSmoke(t *testing.T) {
	app := buildbuddy_enterprise.Run(
		t,
		"--remote_execution.enable_remote_exec=true",
		"--remote_execution.require_executor_authorization=true",
		"--remote_execution.enable_user_owned_executors=true",
	)
	wc := buildbuddy_enterprise.LoginAsDefaultSelfAuthUser(t, app)
	createAPIKey := func(c cappb.Capability) string {
		rsp := &akpb.CreateApiKeyResponse{}
		err := wc.RPC("CreateApiKey", &akpb.CreateApiKeyRequest{
			RequestContext: wc.RequestContext,
			Capability:     []cappb.Capability{c},
		}, rsp)
		require.NoError(t, err)
		return rsp.GetApiKey().GetValue()
	}

	// testexecutor.Run waits for the executor to be ready, which includes
	// registering with the scheduler.
	c := &client{pool: "smoke-" + uuid.New(), hostID: "smoke-" + uuid.New()}
	dir := testfs.MakeTempDir(t)
	testexecutor.Run(
		t,
		"--executor.app_target="+app.GRPCAddress(),
		"--executor.api_key="+createAPIKey(cappb.Capability_REGISTER_EXECUTOR),
		"--executor.pool="+c.pool,
		"--executor.host_id="+c.hostID,
		"--executor.root_directory="+filepath.Join(dir, "builds"),
		"--executor.local_cache_directory="+filepath.Join(dir, "filecache"),
		"--executor.enable_bare_runner=true",
	)

	conn, err := grpc_client.DialSimple(app.GRPCAddress())
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	c.env = real_environment.NewBatchEnv()
	c.env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	c.env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	c.env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	c.ctx = metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", createAPIKey(cappb.Capability_CACHE_WRITE))

	for _, tc := range []struct {
		name string
		test func(t *testing.T, c *client)
	}{
		{"StdioAndExitCode", testStdioAndExitCode},
		{"InputsAndOutputs", testInputsAndOutputs},
		{"EnvAndWorkingDirectory", testEnvAndWorkingDirectory},
		{"Timeout", testTimeout},
		{"ActionCache", testActionCache},
		{"Fanout", testFanout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.test(t, c)
		})
	}
}

func testStdioAndExitCode(t *testing.T, c *client) {
	res := c.mustExecute(t, &action{args: []string{"sh", "-c", "echo out; echo err >&2; exit 3"}})
	require.Equal(t, int32(3), res.ActionResult.GetExitCode())
	require.Equal(t, "out\n", res.Stdout)
	require.Equal(t, "err\n", res.Stderr)
}

func testInputsAndOutputs(t *testing.T, c *client) {
	res := c.mustExecute(t, &action{
		args: []string{"sh", "-c", `
			cat in/a.txt in/nested/b.txt > out/file.txt
			mkdir -p outdir/sub
			echo y > outdir/sub/y.txt
		`},
		inputs:      map[string]string{"in/a.txt": "hello ", "in/nested/b.txt": "world"},
		outputPaths: []string{"out/file.txt", "outdir"},
	})
	require.Equal(t, int32(0), res.ActionResult.GetExitCode(), "stderr: %s", res.Stderr)

	// The executor creates parent dirs of declared outputs.
	require.Len(t, res.ActionResult.GetOutputFiles(), 1)
	f := res.ActionResult.GetOutputFiles()[0]
	require.Equal(t, "out/file.txt", f.GetPath())
	require.Equal(t, "hello world", c.download(t, f.GetDigest()))

	require.Len(t, res.ActionResult.GetOutputDirectories(), 1)
	d := res.ActionResult.GetOutputDirectories()[0]
	require.Equal(t, "outdir", d.GetPath())
	tree := &repb.Tree{}
	rn := digest.NewCASResourceName(d.GetTreeDigest(), "", digestFunction)
	require.NoError(t, cachetools.GetBlobAsProto(c.ctx, c.env.GetByteStreamClient(), rn, tree))
	require.Len(t, tree.GetChildren(), 1)
	require.Equal(t, "sub", tree.GetRoot().GetDirectories()[0].GetName())
	y := tree.GetChildren()[0].GetFiles()[0]
	require.Equal(t, "y.txt", y.GetName())
	require.Equal(t, "y\n", c.download(t, y.GetDigest()))
}

func testEnvAndWorkingDirectory(t *testing.T, c *client) {
	res := c.mustExecute(t, &action{
		args:       []string{"sh", "-c", `echo "$GREETING"; pwd`},
		env:        []string{"GREETING=hello world"},
		workingDir: "sub/dir",
		inputs:     map[string]string{"sub/dir/.keep": ""},
	})
	require.Equal(t, int32(0), res.ActionResult.GetExitCode(), "stderr: %s", res.Stderr)
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	require.Len(t, lines, 2)
	require.Equal(t, "hello world", lines[0])
	require.True(t, strings.HasSuffix(lines[1], "/sub/dir"), "pwd: %s", lines[1])
}

func testTimeout(t *testing.T, c *client) {
	res := c.execute(t, &action{args: []string{"sleep", "600"}, timeout: 2 * time.Second})
	require.Equal(t, codes.DeadlineExceeded, status.Code(res.Err), "execution error: %v", res.Err)
}

func testActionCache(t *testing.T, c *client) {
	a := &action{args: []string{"echo", "cached-" + uuid.New()}}
	res := c.mustExecute(t, a)
	require.False(t, res.ExecuteResponse.GetCachedResult())

	a.cacheLookup = true
	res2 := c.mustExecute(t, a)
	require.True(t, res2.ExecuteResponse.GetCachedResult(), "second execution should hit the action cache")
	require.Equal(t, res.Stdout, res2.Stdout)
}

func testFanout(t *testing.T, c *client) {
	for i := range 20 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			res := c.mustExecute(t, &action{args: []string{"echo", fmt.Sprint(i)}})
			require.Equal(t, fmt.Sprintf("%d\n", i), res.Stdout)
		})
	}
}

// client runs actions in the executor's pool.
type client struct {
	env    *real_environment.RealEnv
	ctx    context.Context
	pool   string
	hostID string
}

type action struct {
	args        []string
	env         []string
	inputs      map[string]string
	outputPaths []string
	workingDir  string
	timeout     time.Duration
	// cacheLookup allows the result to be served from the action cache.
	cacheLookup bool
}

type result struct {
	*rexec.Response
	ActionResult *repb.ActionResult
	Stdout       string
	Stderr       string
}

func (c *client) execute(t *testing.T, a *action) *result {
	inputRoot := ""
	if len(a.inputs) > 0 {
		inputRoot = testfs.MakeTempDir(t)
		testfs.WriteAllFileContents(t, inputRoot, a.inputs)
	}
	env, err := rexec.MakeEnv(a.env...)
	require.NoError(t, err)
	platform, err := rexec.MakePlatform(
		"OSFamily="+runtime.GOOS,
		"Arch="+runtime.GOARCH,
		"Pool="+c.pool,
		"use-self-hosted-executors=true",
	)
	require.NoError(t, err)
	cmd := &repb.Command{
		Arguments:            a.args,
		EnvironmentVariables: env,
		Platform:             platform,
		OutputPaths:          a.outputPaths,
		WorkingDirectory:     a.workingDir,
	}
	act := &repb.Action{Platform: platform}
	if a.timeout > 0 {
		act.Timeout = durationpb.New(a.timeout)
	}
	arn, err := rexec.Prepare(c.ctx, c.env, "", digestFunction, act, cmd, inputRoot)
	require.NoError(t, err)
	stream, err := rexec.Start(c.ctx, c.env, arn, rexec.WithSkipCacheLookup(!a.cacheLookup))
	require.NoError(t, err)
	rsp, err := rexec.Wait(stream)
	require.NoError(t, err)

	res := &result{Response: rsp, ActionResult: rsp.ExecuteResponse.GetResult()}
	if rsp.Err != nil {
		return res
	}
	if !rsp.ExecuteResponse.GetCachedResult() {
		require.Equal(t, c.hostID, res.ActionResult.GetExecutionMetadata().GetWorker(), "action should run on the executor under test")
	}
	cmdResult, err := rexec.GetResult(c.ctx, c.env, "", digestFunction, res.ActionResult)
	require.NoError(t, err)
	res.Stdout = string(cmdResult.Stdout)
	res.Stderr = string(cmdResult.Stderr)
	return res
}

func (c *client) mustExecute(t *testing.T, a *action) *result {
	res := c.execute(t, a)
	require.NoError(t, res.Err)
	return res
}

func (c *client) download(t *testing.T, d *repb.Digest) string {
	var buf bytes.Buffer
	rn := digest.NewCASResourceName(d, "", digestFunction)
	require.NoError(t, cachetools.GetBlob(c.ctx, c.env.GetByteStreamClient(), rn, &buf))
	return buf.String()
}
