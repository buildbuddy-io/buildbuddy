// Package executor_smoke_test is a smoke test suite for an executor binary.
//
// It starts a minimal fake app in-process, launches the executor binary as a
// subprocess pointed at the fake app, and runs a batch of actions against it in
// parallel. It has no dependencies beyond the executor binary itself, so it can
// run on any OS/arch that the executor supports, including Windows.
//
// By default, it tests the executor built by Bazel for the host platform. To
// test a prebuilt executor binary, pass --executor_binary:
//
//	executor_smoke_test --executor_binary=/path/to/executor -test.v
package executor_smoke_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	gstatus "google.golang.org/grpc/status"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	executorBinary = flag.String("executor_binary", "", "Path to the executor binary to test. Defaults to the executor built by Bazel.")
	executorArgs   = flag.String("executor_args", "", "Extra space-separated flags to pass to the executor.")
	executorArch   = flag.String("executor_arch", "", "If set, the GOARCH that the executor is expected to report. Useful when testing a binary for a different arch than the test binary, such as windows/386 on windows/amd64.")

	// Set by x_defs in the BUILD file.
	executorRlocationpath string
)

const (
	executorEnvSentinel = "EXECUTOR_SMOKE_SENTINEL"

	// How long to wait for the executor to start and register.
	startupTimeout = 60 * time.Second
	// Default timeout for a single action, from scheduling to completion.
	actionTimeout = 60 * time.Second
)

func TestMain(m *testing.M) {
	if os.Getenv(helperEnvVar) != "" {
		os.Exit(runHelper(os.Args[1:]))
	}
	os.Exit(m.Run())
}

func TestExecutorSmoke(t *testing.T) {
	e := setup(t)

	cases := []struct {
		name string
		fn   func(t *testing.T, e *smokeEnv)
	}{
		{"Basic", testBasic},
		{"NonZeroExit", testNonZeroExit},
		{"ArgQuoting", testArgQuoting},
		{"EnvVars", testEnvVars},
		{"InputTree", testInputTree},
		{"LargeInput", testLargeInput},
		{"ManyInputs", testManyInputs},
		{"OutputPaths", testOutputPaths},
		{"LegacyOutputFilesAndDirs", testLegacyOutputs},
		{"LargeOutput", testLargeOutput},
		{"LargeStdout", testLargeStdout},
		{"WorkingDirectory", testWorkingDirectory},
		{"LongPaths", testLongPaths},
		{"DigestFunctions", testDigestFunctions},
		{"InstanceName", testInstanceName},
		{"NativeShell", testNativeShell},
		{"NativeScript", testNativeScript},
		{"Timeout", testTimeout},
		{"TimeoutKillsProcessTree", testTimeoutKillsProcessTree},
		{"RunnerRecycling", testRunnerRecycling},
		{"Burst", testBurst},
		{"PersistentWorkers", testPersistentWorkers},
		{"FileCacheReuse", testFileCacheReuse},
		{"InputMutationIsolated", testInputMutationIsolated},
		{"ManyOutputs", testManyOutputs},
		{"LongCommandLine", testLongCommandLine},
		{"EnvNotInheritedFromExecutor", testEnvNotInheritedFromExecutor},
		{"MissingInputBlob", testMissingInputBlob},
		{"CommandNotFound", testCommandNotFound},
		{"UnsupportedIsolationType", testUnsupportedIsolationType},
	}
	t.Run("Actions", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				start := time.Now()
				t.Cleanup(func() {
					t.Logf("%s took %s", tc.name, time.Since(start).Round(time.Millisecond))
				})
				tc.fn(t, e)
			})
		}
	})

	t.Run("ExecutorStillHealthy", func(t *testing.T) {
		e.checkHealthy(t)
	})
	if u := e.app.Unimplemented(); len(u) > 0 {
		t.Logf("Executor called RPCs that the fake app does not implement: %s", strings.Join(u, ", "))
	}
	t.Run("GracefulShutdown", func(t *testing.T) {
		e.shutdown(t)
	})
}

// smokeEnv holds the state shared by all smoke test cases: the fake app and the
// executor under test.
type smokeEnv struct {
	app      *fakeApp
	node     *scpb.ExecutionNode
	httpPort int
	tmpDir   string

	cmd      *exec.Cmd
	exited   chan struct{}
	exitErr  error
	logPath  string
	shutOnce sync.Once

	helper []byte
}

func setup(t *testing.T) *smokeEnv {
	bin := *executorBinary
	if bin == "" {
		require.NotEmpty(t, executorRlocationpath, "--executor_binary must be set when not running under Bazel")
		var err error
		bin, err = runfiles.Rlocation(executorRlocationpath)
		require.NoError(t, err)
	}
	bin, err := filepath.Abs(bin)
	require.NoError(t, err)
	t.Logf("Testing executor binary %s", bin)

	self, err := os.Executable()
	require.NoError(t, err)
	helper, err := os.ReadFile(self)
	require.NoError(t, err)

	app, err := startFakeApp()
	require.NoError(t, err)
	t.Cleanup(app.Stop)

	tmpDir := t.TempDir()
	e := &smokeEnv{
		app:      app,
		tmpDir:   tmpDir,
		helper:   helper,
		httpPort: freePort(t),
		exited:   make(chan struct{}),
		logPath:  filepath.Join(tmpDir, "executor.log"),
	}

	logFile, err := os.Create(e.logPath)
	require.NoError(t, err)
	args := []string{
		"--executor.app_target=" + app.Target(),
		"--executor.root_directory=" + filepath.Join(tmpDir, "builds"),
		"--executor.local_cache_directory=" + filepath.Join(tmpDir, "filecache"),
		"--executor.local_cache_size_bytes=5000000000",
		"--executor.enable_bare_runner=true",
		"--executor.default_isolation_type=none",
		// Let the executor run plenty of the small smoke test actions
		// concurrently, regardless of the size of the host.
		"--executor.millicpu=32000",
		"--executor.memory_bytes=32000000000",
		"--listen=127.0.0.1",
		fmt.Sprintf("--port=%d", e.httpPort),
		fmt.Sprintf("--monitoring_port=%d", freePort(t)),
		"--app.log_level=info",
	}
	args = append(args, strings.Fields(*executorArgs)...)
	e.cmd = exec.Command(bin, args...)
	e.cmd.Dir = tmpDir
	// Actions should not see the executor's own environment.
	e.cmd.Env = append(os.Environ(), executorEnvSentinel+"=leaked")
	e.cmd.Stdout = logFile
	e.cmd.Stderr = logFile
	startTime := time.Now()
	require.NoError(t, e.cmd.Start())
	go func() {
		e.exitErr = e.cmd.Wait()
		logFile.Close()
		close(e.exited)
	}()
	t.Cleanup(func() {
		e.kill()
		if t.Failed() {
			e.dumpLogs(t)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()
	go func() {
		select {
		case <-e.exited:
			cancel()
		case <-ctx.Done():
		}
	}()
	e.node, err = app.WaitForExecutor(ctx)
	require.NoError(t, err, "executor did not register (exited: %v)", e.exitErr)
	t.Logf("Executor registered after %s: os=%s arch=%s version=%q isolation=%v milliCPU=%d",
		time.Since(startTime).Round(time.Millisecond), e.node.GetOsFamily(), e.node.GetArch(),
		e.node.GetVersion(), e.node.GetSupportedIsolationTypes(), e.node.GetAssignableMilliCpu())
	require.Equal(t, runtime.GOOS, e.node.GetOsFamily(), "executor OS")
	if *executorArch != "" {
		require.Equal(t, *executorArch, e.node.GetArch(), "executor arch")
	}
	require.Contains(t, e.node.GetSupportedIsolationTypes(), "none")
	return e
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (e *smokeEnv) checkHealthy(t *testing.T) {
	select {
	case <-e.exited:
		require.FailNow(t, "executor exited unexpectedly", "%v", e.exitErr)
	default:
	}
	rsp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz?server-type=prod-buildbuddy-executor", e.httpPort))
	require.NoError(t, err)
	rsp.Body.Close()
	require.Equal(t, http.StatusOK, rsp.StatusCode, "executor /readyz status")
}

func (e *smokeEnv) shutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		// There is no portable way to deliver a graceful shutdown signal to
		// a process on Windows.
		t.Skip("graceful shutdown is not tested on Windows")
	}
	require.NoError(t, e.cmd.Process.Signal(os.Interrupt))
	select {
	case <-e.exited:
		require.NoError(t, e.exitErr, "executor exit status after graceful shutdown")
	case <-time.After(30 * time.Second):
		require.FailNow(t, "executor did not exit within 30s of SIGINT")
	}
}

func (e *smokeEnv) kill() {
	select {
	case <-e.exited:
		return
	default:
	}
	e.cmd.Process.Kill()
	<-e.exited
}

func (e *smokeEnv) dumpLogs(t *testing.T) {
	const maxLines = 300
	f, err := os.Open(e.logPath)
	if err != nil {
		t.Logf("could not read executor logs: %s", err)
		return
	}
	defer f.Close()
	var lines []string
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 1<<20), 1<<20)
	for s.Scan() {
		lines = append(lines, s.Text())
		if len(lines) > maxLines {
			lines = lines[1:]
		}
	}
	t.Logf("Last %d lines of executor logs:\n%s", len(lines), strings.Join(lines, "\n"))
}

// inputFile is an action input file.
type inputFile struct {
	contents   []byte
	executable bool
}

// action describes an action to run on the executor.
type action struct {
	args []string
	env  map[string]string
	// Input files by slash-separated path. A path ending in "/" is an empty
	// directory. The helper binary is always added to the input root.
	inputs map[string]inputFile

	outputPaths []string
	// Legacy (pre-REAPI v2.1) output fields.
	outputFiles       []string
	outputDirectories []string

	workingDirectory string
	timeout          time.Duration
	platform         map[string]string
	instanceName     string
	digestFunction   repb.DigestFunction_Value
	// Input paths whose contents should not be uploaded to the CAS.
	skipUpload []string
}

// result is the outcome of running an action.
type result struct {
	action         *action
	actionDigest   *repb.Digest
	response       *repb.ExecuteResponse
	task           *fakeTask
	digestFunction repb.DigestFunction_Value
}

func (r *result) ActionResult() *repb.ActionResult {
	return r.response.GetResult()
}

func (e *smokeEnv) cacheSet(ctx context.Context, instanceName string, df repb.DigestFunction_Value, data []byte) (*repb.Digest, error) {
	d, err := digest.Compute(bytes.NewReader(data), df)
	if err != nil {
		return nil, err
	}
	ctx, err = e.app.Context(ctx)
	if err != nil {
		return nil, err
	}
	rn := digest.NewCASResourceName(d, instanceName, df).ToProto()
	return d, e.app.env.GetCache().Set(ctx, rn, data)
}

func (e *smokeEnv) cacheSetProto(ctx context.Context, instanceName string, df repb.DigestFunction_Value, m proto.Message) (*repb.Digest, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	return e.cacheSet(ctx, instanceName, df, b)
}

func (e *smokeEnv) casGet(ctx context.Context, r *result, d *repb.Digest) ([]byte, error) {
	// The empty blob is never actually stored.
	if d.GetSizeBytes() == 0 {
		return nil, nil
	}
	ctx, err := e.app.Context(ctx)
	if err != nil {
		return nil, err
	}
	rn := digest.NewCASResourceName(d, r.action.instanceName, r.digestFunction).ToProto()
	return e.app.env.GetCache().Get(ctx, rn)
}

// uploadInputRoot uploads the input tree and returns the root digest.
func (e *smokeEnv) uploadInputRoot(ctx context.Context, a *action, df repb.DigestFunction_Value) (*repb.Digest, error) {
	type dirNode struct {
		files map[string]inputFile
		dirs  map[string]*dirNode
	}
	newDir := func() *dirNode { return &dirNode{files: map[string]inputFile{}, dirs: map[string]*dirNode{}} }
	root := newDir()
	getDir := func(parts []string) *dirNode {
		d := root
		for _, p := range parts {
			if d.dirs[p] == nil {
				d.dirs[p] = newDir()
			}
			d = d.dirs[p]
		}
		return d
	}
	inputs := map[string]inputFile{helperName(): {contents: e.helper, executable: true}}
	maps.Copy(inputs, a.inputs)
	for p, f := range inputs {
		if dir, ok := strings.CutSuffix(p, "/"); ok {
			getDir(strings.Split(dir, "/"))
			continue
		}
		parts := strings.Split(p, "/")
		getDir(parts[:len(parts)-1]).files[parts[len(parts)-1]] = f
	}

	var upload func(path string, d *dirNode) (*repb.Digest, error)
	upload = func(path string, d *dirNode) (*repb.Digest, error) {
		dir := &repb.Directory{}
		for name, f := range d.files {
			var fd *repb.Digest
			var err error
			if slices.Contains(a.skipUpload, path+name) {
				fd, err = digest.Compute(bytes.NewReader(f.contents), df)
			} else {
				fd, err = e.cacheSet(ctx, a.instanceName, df, f.contents)
			}
			if err != nil {
				return nil, err
			}
			dir.Files = append(dir.Files, &repb.FileNode{Name: name, Digest: fd, IsExecutable: f.executable})
		}
		for name, child := range d.dirs {
			cd, err := upload(path+name+"/", child)
			if err != nil {
				return nil, err
			}
			dir.Directories = append(dir.Directories, &repb.DirectoryNode{Name: name, Digest: cd})
		}
		sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })
		sort.Slice(dir.Directories, func(i, j int) bool { return dir.Directories[i].Name < dir.Directories[j].Name })
		return e.cacheSetProto(ctx, a.instanceName, df, dir)
	}
	return upload("", root)
}

// run runs the action on the executor and waits for it to complete. It fails
// the test if the executor does not return an ExecuteResponse.
func (e *smokeEnv) run(t *testing.T, a *action) *result {
	r, err := e.tryRun(t.Context(), a)
	require.NoError(t, err)
	return r
}

func (e *smokeEnv) tryRun(ctx context.Context, a *action) (*result, error) {
	ctx, cancel := context.WithTimeout(ctx, actionTimeout)
	defer cancel()

	df := a.digestFunction
	if df == repb.DigestFunction_UNKNOWN {
		df = repb.DigestFunction_SHA256
	}
	rootDigest, err := e.uploadInputRoot(ctx, a, df)
	if err != nil {
		return nil, fmt.Errorf("upload inputs: %w", err)
	}

	platform := &repb.Platform{}
	for k, v := range a.platform {
		platform.Properties = append(platform.Properties, &repb.Platform_Property{Name: k, Value: v})
	}
	sort.Slice(platform.Properties, func(i, j int) bool { return platform.Properties[i].Name < platform.Properties[j].Name })

	env := map[string]string{helperEnvVar: "1"}
	maps.Copy(env, a.env)
	cmd := &repb.Command{
		Arguments:         a.args,
		OutputPaths:       a.outputPaths,
		OutputFiles:       a.outputFiles,
		OutputDirectories: a.outputDirectories,
		WorkingDirectory:  a.workingDirectory,
		Platform:          platform,
	}
	for k, v := range env {
		cmd.EnvironmentVariables = append(cmd.EnvironmentVariables, &repb.Command_EnvironmentVariable{Name: k, Value: v})
	}
	sort.Slice(cmd.EnvironmentVariables, func(i, j int) bool {
		return cmd.EnvironmentVariables[i].Name < cmd.EnvironmentVariables[j].Name
	})
	cmdDigest, err := e.cacheSetProto(ctx, a.instanceName, df, cmd)
	if err != nil {
		return nil, err
	}
	act := &repb.Action{
		CommandDigest:   cmdDigest,
		InputRootDigest: rootDigest,
		Platform:        platform,
	}
	if a.timeout > 0 {
		act.Timeout = durationpb.New(a.timeout)
	}
	actionDigest, err := e.cacheSetProto(ctx, a.instanceName, df, act)
	if err != nil {
		return nil, err
	}

	executionID := digest.NewCASResourceName(actionDigest, a.instanceName, df).NewUploadString()
	task, err := e.app.Schedule(ctx, &repb.ExecutionTask{
		ExecuteRequest: &repb.ExecuteRequest{
			InstanceName:    a.instanceName,
			ActionDigest:    actionDigest,
			SkipCacheLookup: true,
			DigestFunction:  df,
		},
		Action:          act,
		Command:         cmd,
		ExecutionId:     executionID,
		QueuedTimestamp: timestamppb.Now(),
	}, &scpb.TaskSize{EstimatedMemoryBytes: 100_000_000, EstimatedMilliCpu: 250})
	if err != nil {
		return nil, fmt.Errorf("schedule: %w", err)
	}
	rsp, err := task.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := task.WaitFinalized(ctx); err != nil {
		return nil, fmt.Errorf("wait for executor to finalize lease: %w", err)
	}
	return &result{action: a, actionDigest: actionDigest, response: rsp, task: task, digestFunction: df}, nil
}

func (e *smokeEnv) stdout(t *testing.T, r *result) string {
	s, err := e.getStdout(t.Context(), r)
	require.NoError(t, err)
	return s
}

func (e *smokeEnv) getStdout(ctx context.Context, r *result) (string, error) {
	ar := r.ActionResult()
	if len(ar.GetStdoutRaw()) > 0 || ar.GetStdoutDigest() == nil {
		return string(ar.GetStdoutRaw()), nil
	}
	b, err := e.casGet(ctx, r, ar.GetStdoutDigest())
	return string(b), err
}

func (e *smokeEnv) stderr(t *testing.T, r *result) string {
	ar := r.ActionResult()
	if len(ar.GetStderrRaw()) > 0 || ar.GetStderrDigest() == nil {
		return string(ar.GetStderrRaw())
	}
	b, err := e.casGet(t.Context(), r, ar.GetStderrDigest())
	require.NoError(t, err)
	return string(b)
}

// requireSuccess fails the test unless the action ran and exited with code 0.
func (e *smokeEnv) requireSuccess(t *testing.T, r *result) {
	t.Helper()
	require.NoError(t, gstatus.FromProto(r.response.GetStatus()).Err(), "execution status")
	if code := r.ActionResult().GetExitCode(); code != 0 {
		require.FailNow(t, "action failed", "exit code %d\nstdout:\n%s\nstderr:\n%s", code, e.stdout(t, r), e.stderr(t, r))
	}
}

// outputFiles returns the contents of all output files, including files
// within output directories, keyed by slash-separated path.
func (e *smokeEnv) outputFiles(t *testing.T, r *result) map[string]string {
	ctx := t.Context()
	out := map[string]string{}
	ar := r.ActionResult()
	for _, f := range ar.GetOutputFiles() {
		b, err := e.casGet(ctx, r, f.GetDigest())
		require.NoError(t, err)
		out[f.GetPath()] = string(b)
	}
	for _, d := range ar.GetOutputDirectories() {
		b, err := e.casGet(ctx, r, d.GetTreeDigest())
		require.NoError(t, err)
		tree := &repb.Tree{}
		require.NoError(t, proto.Unmarshal(b, tree))
		children := map[string]*repb.Directory{}
		for _, c := range tree.GetChildren() {
			cd, err := digest.ComputeForMessage(c, r.digestFunction)
			require.NoError(t, err)
			children[cd.GetHash()] = c
		}
		var walk func(prefix string, dir *repb.Directory)
		walk = func(prefix string, dir *repb.Directory) {
			if len(dir.GetFiles()) == 0 && len(dir.GetDirectories()) == 0 {
				out[prefix+"/"] = ""
			}
			for _, f := range dir.GetFiles() {
				b, err := e.casGet(ctx, r, f.GetDigest())
				require.NoError(t, err)
				out[prefix+"/"+f.GetName()] = string(b)
			}
			for _, sub := range dir.GetDirectories() {
				child, ok := children[sub.GetDigest().GetHash()]
				require.True(t, ok, "tree for %s is missing child %s", d.GetPath(), sub.GetName())
				walk(prefix+"/"+sub.GetName(), child)
			}
		}
		walk(d.GetPath(), tree.GetRoot())
	}
	return out
}

func testBasic(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{args: helperArgs(
		step("print", "hello stdout"),
		step("eprint", "hello stderr"),
	)})
	e.requireSuccess(t, r)
	assert.Equal(t, "hello stdout", e.stdout(t, r))
	assert.Equal(t, "hello stderr", e.stderr(t, r))

	md := r.ActionResult().GetExecutionMetadata()
	assert.NotEmpty(t, md.GetWorker(), "ExecutionMetadata.worker")
	assert.NotEmpty(t, md.GetExecutorId(), "ExecutionMetadata.executor_id")
	ts := []time.Time{
		md.GetWorkerStartTimestamp().AsTime(),
		md.GetInputFetchStartTimestamp().AsTime(),
		md.GetInputFetchCompletedTimestamp().AsTime(),
		md.GetExecutionStartTimestamp().AsTime(),
		md.GetExecutionCompletedTimestamp().AsTime(),
		md.GetOutputUploadStartTimestamp().AsTime(),
		md.GetOutputUploadCompletedTimestamp().AsTime(),
		md.GetWorkerCompletedTimestamp().AsTime(),
	}
	for i := 1; i < len(ts); i++ {
		assert.False(t, ts[i].Before(ts[i-1]), "execution metadata timestamps out of order: %v", ts)
	}
	assert.Contains(t, r.task.Stages(), repb.ExecutionStage_EXECUTING)
	assert.Contains(t, r.task.Stages(), repb.ExecutionStage_COMPLETED)
}

func testNonZeroExit(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{args: helperArgs(
		step("print", "about to fail"),
		step("exit", "42"),
	)})
	require.NoError(t, gstatus.FromProto(r.response.GetStatus()).Err())
	assert.Equal(t, int32(42), r.ActionResult().GetExitCode())
	assert.Equal(t, "about to fail", e.stdout(t, r))
}

func testArgQuoting(t *testing.T, e *smokeEnv) {
	// Arguments that are easy to mangle when building a command line,
	// particularly on Windows, where arguments are passed as a single string.
	args := []string{
		"plain",
		"",
		"with space",
		"  leading and trailing  ",
		`double"quote`,
		`"quoted"`,
		`single'quote`,
		`back\slash`,
		`trailing\`,
		`trailing\\`,
		`\"escaped quote\"`,
		"tab\there",
		"new\nline",
		`$HOME ${HOME} %PATH% !bang! ^caret`,
		"& | < > ; ( ) *",
		"unicode: héllo wörld ✓ 日本",
		"--flag=value with spaces",
	}
	r := e.run(t, &action{args: helperArgs(append([]string{"echo-args"}, args...))})
	e.requireSuccess(t, r)
	var got []string
	require.NoError(t, json.Unmarshal([]byte(e.stdout(t, r)), &got))
	assert.Equal(t, args, got)
}

func testEnvVars(t *testing.T, e *smokeEnv) {
	env := map[string]string{
		"SMOKE_PLAIN":   "value",
		"SMOKE_EMPTY":   "",
		"SMOKE_SPACES":  "  a value with spaces  ",
		"SMOKE_EQUALS":  "a=b=c",
		"SMOKE_QUOTES":  `"double" 'single'`,
		"SMOKE_UNICODE": "héllo ✓",
		"SMOKE_PATHISH": "/a/b:/c/d;C:\\e\\f",
	}
	names := []string{"SMOKE_UNSET"}
	for k := range env {
		names = append(names, k)
	}
	sort.Strings(names)
	r := e.run(t, &action{
		args: helperArgs(append([]string{"env"}, names...)),
		env:  env,
	})
	e.requireSuccess(t, r)
	var got map[string]*string
	require.NoError(t, json.Unmarshal([]byte(e.stdout(t, r)), &got))
	for _, k := range names {
		want, ok := env[k]
		if !ok {
			assert.Nil(t, got[k], "%s should be unset", k)
			continue
		}
		if assert.NotNil(t, got[k], "%s should be set", k) {
			assert.Equal(t, want, *got[k], "value of %s", k)
		}
	}
}

func testInputTree(t *testing.T, e *smokeEnv) {
	inputs := map[string]inputFile{
		"a.txt":                          {contents: []byte("a")},
		"empty.txt":                      {contents: nil},
		"dir/b.txt":                      {contents: []byte("b")},
		"dir/sub/c.txt":                  {contents: []byte("c")},
		"dir/sub/deeper/d.txt":           {contents: []byte("d")},
		"dir/empty_dir/":                 {},
		"empty_top/":                     {},
		"tool.sh":                        {contents: []byte("#!/bin/sh\n"), executable: true},
		"name with spaces/file name.txt": {contents: []byte("spaces")},
		"ünïcödé/файл.txt":               {contents: []byte("unicode")},
		"dup1.txt":                       {contents: []byte("same")},
		"dir/dup2.txt":                   {contents: []byte("same")},
	}
	r := e.run(t, &action{
		args:   helperArgs(step("tree", ".")),
		inputs: inputs,
	})
	e.requireSuccess(t, r)
	var got []treeEntry
	require.NoError(t, json.Unmarshal([]byte(e.stdout(t, r)), &got))

	want := map[string]treeEntry{}
	for p, f := range inputs {
		if dir, ok := strings.CutSuffix(p, "/"); ok {
			p = dir
			want[p] = treeEntry{Path: p, Dir: true}
		} else {
			want[p] = treeEntry{
				Path:       p,
				Size:       int64(len(f.contents)),
				Executable: f.executable && runtime.GOOS != "windows",
				SHA256:     sha256Hex(f.contents),
			}
		}
		// Add parent directories.
		for d := filepath.ToSlash(filepath.Dir(p)); d != "."; d = filepath.ToSlash(filepath.Dir(d)) {
			want[d] = treeEntry{Path: d, Dir: true}
		}
	}
	gotMap := map[string]treeEntry{}
	for _, g := range got {
		if g.Path == helperName() {
			continue
		}
		gotMap[g.Path] = g
	}
	assert.Equal(t, want, gotMap)
}

func testLargeInput(t *testing.T, e *smokeEnv) {
	// Larger than the gRPC message size limit, so it must be streamed.
	data := genBytes(24<<20, 1)
	r := e.run(t, &action{
		args:   helperArgs(step("sha256", "big.bin")),
		inputs: map[string]inputFile{"big.bin": {contents: data}},
	})
	e.requireSuccess(t, r)
	assert.Equal(t, sha256Hex(data)+"\n", e.stdout(t, r))
}

func testManyInputs(t *testing.T, e *smokeEnv) {
	inputs := map[string]inputFile{}
	for i := range 2000 {
		inputs[fmt.Sprintf("many/d%02d/f%04d.txt", i%20, i)] = inputFile{contents: []byte(strconv.Itoa(i))}
	}
	r := e.run(t, &action{
		args:   helperArgs(step("tree", "many")),
		inputs: inputs,
	})
	e.requireSuccess(t, r)
	var got []treeEntry
	require.NoError(t, json.Unmarshal([]byte(e.stdout(t, r)), &got))
	files := 0
	for _, g := range got {
		if g.Dir {
			continue
		}
		files++
		f, ok := inputs["many/"+g.Path]
		if assert.True(t, ok, "unexpected file %s", g.Path) {
			assert.Equal(t, sha256Hex(f.contents), g.SHA256, g.Path)
		}
	}
	assert.Equal(t, len(inputs), files)
}

func testOutputPaths(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{
		args: helperArgs(
			// Parent directories of output paths must be created by the
			// executor.
			step("write", "out/nested/file.txt", "file contents"),
			step("write", "empty.txt", ""),
			step("mkdir", "outdir/sub/deeper"),
			step("mkdir", "outdir/empty"),
			step("write", "outdir/top.txt", "top"),
			step("write", "outdir/sub/deeper/leaf.txt", "leaf"),
			step("write", "out/tool", "#!/bin/sh\n"),
			step("chmod-x", "out/tool"),
			step("write", "out/spaces and ünïcödé.txt", "unicode output"),
		),
		outputPaths: []string{
			"out/nested/file.txt",
			"out/tool",
			"out/spaces and ünïcödé.txt",
			"empty.txt",
			"outdir",
			// Declared but never created; should be silently omitted.
			"missing.txt",
		},
	})
	e.requireSuccess(t, r)
	assert.Equal(t, map[string]string{
		"out/nested/file.txt":        "file contents",
		"out/tool":                   "#!/bin/sh\n",
		"out/spaces and ünïcödé.txt": "unicode output",
		"empty.txt":                  "",
		"outdir/top.txt":             "top",
		"outdir/sub/deeper/leaf.txt": "leaf",
		"outdir/empty/":              "",
	}, e.outputFiles(t, r))
	for _, f := range r.ActionResult().GetOutputFiles() {
		wantExec := f.GetPath() == "out/tool" && runtime.GOOS != "windows"
		assert.Equal(t, wantExec, f.GetIsExecutable(), "is_executable for %s", f.GetPath())
	}
}

func testLegacyOutputs(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{
		args: helperArgs(
			step("write", "a/b/file.txt", "legacy file"),
			step("mkdir", "d/e"),
			step("write", "d/e/f.txt", "legacy dir file"),
		),
		outputFiles:       []string{"a/b/file.txt"},
		outputDirectories: []string{"d"},
	})
	e.requireSuccess(t, r)
	assert.Equal(t, map[string]string{
		"a/b/file.txt": "legacy file",
		"d/e/f.txt":    "legacy dir file",
	}, e.outputFiles(t, r))
}

func testLargeOutput(t *testing.T, e *smokeEnv) {
	const size = 24 << 20
	r := e.run(t, &action{
		args:        helperArgs(step("gen", "big.bin", strconv.Itoa(size), "2")),
		outputPaths: []string{"big.bin"},
	})
	e.requireSuccess(t, r)
	files := r.ActionResult().GetOutputFiles()
	require.Len(t, files, 1)
	want, err := digest.Compute(bytes.NewReader(genBytes(size, 2)), r.digestFunction)
	require.NoError(t, err)
	assert.Equal(t, want.GetHash(), files[0].GetDigest().GetHash())
	b, err := e.casGet(t.Context(), r, files[0].GetDigest())
	require.NoError(t, err)
	assert.Len(t, b, size)
}

func testLargeStdout(t *testing.T, e *smokeEnv) {
	const size = 8 << 20
	r := e.run(t, &action{args: helperArgs(step("genstdout", strconv.Itoa(size), "3"))})
	e.requireSuccess(t, r)
	assert.True(t, bytes.Equal(genBytes(size, 3), []byte(e.stdout(t, r))), "stdout contents do not match")
}

func testWorkingDirectory(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{
		args: []string{
			"../../" + helperName(),
			"cat", "input.txt", stepSeparator,
			"pwd", stepSeparator,
			"write", "out/result.txt", "from subdir",
		},
		inputs:           map[string]inputFile{"sub/dir/input.txt": {contents: []byte("input in subdir\n")}},
		workingDirectory: "sub/dir",
		outputPaths:      []string{"out/result.txt"},
	})
	e.requireSuccess(t, r)
	lines := strings.Split(strings.TrimSpace(e.stdout(t, r)), "\n")
	require.Len(t, lines, 2)
	assert.Equal(t, "input in subdir", lines[0])
	assert.True(t, strings.HasSuffix(filepath.ToSlash(lines[1]), "/sub/dir"), "working directory %q should end with sub/dir", lines[1])
	// Output paths, both in the Command and in the ActionResult, are relative
	// to the working directory.
	assert.Equal(t, map[string]string{"out/result.txt": "from subdir"}, e.outputFiles(t, r))
}

func testLongPaths(t *testing.T, e *smokeEnv) {
	// Deep enough that the absolute path is well past Windows' legacy 260
	// character MAX_PATH limit.
	var parts []string
	for i := range 12 {
		parts = append(parts, fmt.Sprintf("long_directory_name_%02d", i))
	}
	dir := strings.Join(parts, "/")
	require.Greater(t, len(dir), 260)
	r := e.run(t, &action{
		args: helperArgs(
			step("cat", dir+"/input.txt"),
			step("write", dir+"/out/output.txt", "long path output"),
		),
		inputs:      map[string]inputFile{dir + "/input.txt": {contents: []byte("long path input")}},
		outputPaths: []string{dir + "/out/output.txt"},
	})
	e.requireSuccess(t, r)
	assert.Equal(t, "long path input", e.stdout(t, r))
	assert.Equal(t, map[string]string{dir + "/out/output.txt": "long path output"}, e.outputFiles(t, r))
}

func testDigestFunctions(t *testing.T, e *smokeEnv) {
	for _, df := range []repb.DigestFunction_Value{
		repb.DigestFunction_SHA256,
		repb.DigestFunction_BLAKE3,
		repb.DigestFunction_SHA1,
		repb.DigestFunction_SHA384,
		repb.DigestFunction_SHA512,
	} {
		t.Run(df.String(), func(t *testing.T) {
			t.Parallel()
			r := e.run(t, &action{
				args: helperArgs(
					step("cat", "in.txt"),
					step("write", "out/out.txt", "output for "+df.String()),
				),
				inputs:         map[string]inputFile{"in.txt": {contents: []byte("input for " + df.String())}},
				outputPaths:    []string{"out/out.txt"},
				digestFunction: df,
			})
			e.requireSuccess(t, r)
			assert.Equal(t, "input for "+df.String(), e.stdout(t, r))
			assert.Equal(t, map[string]string{"out/out.txt": "output for " + df.String()}, e.outputFiles(t, r))
		})
	}
}

func testInstanceName(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{
		args: helperArgs(
			step("cat", "in.txt"),
			step("write", "out.txt", "instance output"),
		),
		inputs:       map[string]inputFile{"in.txt": {contents: []byte("instance input")}},
		outputPaths:  []string{"out.txt"},
		instanceName: "smoke/instance",
	})
	e.requireSuccess(t, r)
	assert.Equal(t, "instance input", e.stdout(t, r))
	assert.Equal(t, map[string]string{"out.txt": "instance output"}, e.outputFiles(t, r))
}

// testNativeShell runs the host's shell, to cover resolving and running a
// command that is not part of the input root.
func testNativeShell(t *testing.T, e *smokeEnv) {
	var args []string
	if runtime.GOOS == "windows" {
		args = []string{"cmd.exe", "/c", "echo hello from cmd& echo to stderr 1>&2& exit /b 3"}
	} else {
		args = []string{"sh", "-c", "echo hello from sh; echo to stderr >&2; exit 3"}
	}
	r := e.run(t, &action{
		args: args,
		env:  map[string]string{"PATH": os.Getenv("PATH")},
	})
	require.NoError(t, gstatus.FromProto(r.response.GetStatus()).Err())
	assert.Equal(t, int32(3), r.ActionResult().GetExitCode(), "stderr: %s", e.stderr(t, r))
	assert.Contains(t, e.stdout(t, r), "hello from")
	assert.Contains(t, e.stderr(t, r), "to stderr")
}

// testNativeScript runs a script from the input root, which exercises
// executable bits on Unix and .bat handling on Windows.
func testNativeScript(t *testing.T, e *smokeEnv) {
	var name, script string
	if runtime.GOOS == "windows" {
		name, script = "script.bat", "@echo off\r\necho script ran %1\r\n"
	} else {
		name, script = "script.sh", "#!/bin/sh\necho script ran \"$1\"\n"
	}
	r := e.run(t, &action{
		args:   []string{"./" + name, "arg1"},
		inputs: map[string]inputFile{name: {contents: []byte(script), executable: true}},
		env:    map[string]string{"PATH": os.Getenv("PATH")},
	})
	e.requireSuccess(t, r)
	assert.Equal(t, "script ran arg1", strings.TrimSpace(e.stdout(t, r)))
}

func testTimeout(t *testing.T, e *smokeEnv) {
	start := time.Now()
	r := e.run(t, &action{
		args: helperArgs(
			step("print", "before sleep"),
			step("sleep", "10m"),
		),
		timeout: 2 * time.Second,
	})
	assert.Equal(t, codes.DeadlineExceeded, gstatus.FromProto(r.response.GetStatus()).Code(), "status: %v", r.response.GetStatus())
	assert.Less(t, time.Since(start), 30*time.Second, "timed out action took too long to complete")
}

func testTimeoutKillsProcessTree(t *testing.T, e *smokeEnv) {
	heartbeat := filepath.Join(e.tmpDir, "heartbeat-"+strconv.FormatInt(time.Now().UnixNano(), 10))
	r := e.run(t, &action{
		args: helperArgs(
			step("spawn-heartbeat", heartbeat),
			step("sleep", "10m"),
		),
		timeout: 3 * time.Second,
	})
	assert.Equal(t, codes.DeadlineExceeded, gstatus.FromProto(r.response.GetStatus()).Code(), "status: %v", r.response.GetStatus())

	// The child process should have started, and should now be dead.
	_, err := os.Stat(heartbeat)
	require.NoError(t, err, "child process did not start")
	time.Sleep(500 * time.Millisecond)
	before, err := os.ReadFile(heartbeat)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	after, err := os.ReadFile(heartbeat)
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after), "child process of timed out action is still running")
}

func testRunnerRecycling(t *testing.T, e *smokeEnv) {
	// Use a unique platform property so that only these two actions can share
	// a runner.
	platform := map[string]string{
		"recycle-runner":     "true",
		"preserve-workspace": "true",
		"smoke-runner-key":   strconv.FormatInt(time.Now().UnixNano(), 10),
	}
	r1 := e.run(t, &action{
		args:     helperArgs(step("write", "state.txt", "left by first action")),
		platform: platform,
	})
	e.requireSuccess(t, r1)
	r2 := e.run(t, &action{
		args:     helperArgs(step("cat", "state.txt")),
		platform: platform,
	})
	e.requireSuccess(t, r2)
	assert.Equal(t, "left by first action", e.stdout(t, r2))
}

func testBurst(t *testing.T, e *smokeEnv) {
	const n = 200
	var eg errgroup.Group
	start := time.Now()
	for i := range n {
		eg.Go(func() error {
			want := fmt.Sprintf("burst %d", i)
			r, err := e.tryRun(t.Context(), &action{
				args:        helperArgs(step("print", want), step("write", "out.txt", want)),
				outputPaths: []string{"out.txt"},
			})
			if err != nil {
				return fmt.Errorf("action %d: %w", i, err)
			}
			if err := gstatus.FromProto(r.response.GetStatus()).Err(); err != nil {
				return fmt.Errorf("action %d: %w", i, err)
			}
			if code := r.ActionResult().GetExitCode(); code != 0 {
				return fmt.Errorf("action %d: exit code %d", i, code)
			}
			if got, err := e.getStdout(t.Context(), r); err != nil || got != want {
				return fmt.Errorf("action %d: stdout %q (err: %v), want %q", i, got, err, want)
			}
			if len(r.ActionResult().GetOutputFiles()) != 1 {
				return fmt.Errorf("action %d: got %d output files, want 1", i, len(r.ActionResult().GetOutputFiles()))
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())
	t.Logf("Ran %d actions in %s", n, time.Since(start).Round(time.Millisecond))
}

func sha256Hex(b []byte) string {
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_SHA256)
	if err != nil {
		panic(err)
	}
	return d.GetHash()
}

func testPersistentWorkers(t *testing.T, e *smokeEnv) {
	for _, protocol := range []string{"json", "proto"} {
		t.Run(protocol, func(t *testing.T) {
			t.Parallel()
			platform := map[string]string{
				"persistent-workers":       "true",
				"persistentWorkerKey":      fmt.Sprintf("smoke-%s-%d", protocol, time.Now().UnixNano()),
				"persistentWorkerProtocol": protocol,
			}
			var pids []string
			for i := 1; i <= 3; i++ {
				arg := fmt.Sprintf("request %d", i)
				r := e.run(t, &action{
					args: []string{"./" + helperName(), "--worker_protocol=" + protocol, "@args.txt"},
					inputs: map[string]inputFile{
						"args.txt": {contents: []byte(arg + "\nsecond line\n")},
					},
					platform: platform,
				})
				e.requireSuccess(t, r)
				// Work responses are reported as stderr.
				out := e.stderr(t, r)
				var pid string
				var count int
				var args string
				_, err := fmt.Sscanf(out, "pid=%s count=%d args=%s", &pid, &count, &args)
				require.NoError(t, err, "worker output: %q", out)
				assert.Equal(t, i, count, "the worker should be reused; output: %q", out)
				assert.Contains(t, out, fmt.Sprintf(`args=["%s","second line"]`, arg))
				pids = append(pids, pid)
			}
			assert.Equal(t, []string{pids[0], pids[0], pids[0]}, pids, "worker PIDs")
		})
	}
}

func testFileCacheReuse(t *testing.T, e *smokeEnv) {
	// Unique contents, so that no other test can have cached this file.
	const size = 8 << 20
	data := genBytes(size, time.Now().UnixNano())
	var downloaded []int64
	for range 2 {
		r := e.run(t, &action{
			args:   helperArgs(step("sha256", "input.bin")),
			inputs: map[string]inputFile{"input.bin": {contents: data}},
		})
		e.requireSuccess(t, r)
		assert.Equal(t, sha256Hex(data)+"\n", e.stdout(t, r))
		downloaded = append(downloaded, r.ActionResult().GetExecutionMetadata().GetIoStats().GetFileDownloadSizeBytes())
	}
	assert.GreaterOrEqual(t, downloaded[0], int64(size), "first action should download the input")
	assert.Less(t, downloaded[1], int64(size), "second action should get the input from the local file cache")
}

func testInputMutationIsolated(t *testing.T, e *smokeEnv) {
	// Inputs are often hard-linked from the executor's file cache. An action
	// that modifies an input must not affect later actions.
	t.Skip("Known issue: inputs are hard-linked from the file cache with write permissions, so bare actions can corrupt the file cache (see the TODO in workspace.go)")
	original := fmt.Sprintf("original contents %d", time.Now().UnixNano())
	inputs := map[string]inputFile{"shared.txt": {contents: []byte(original)}}
	r1 := e.run(t, &action{
		args:   helperArgs(step("try-append", "shared.txt", " MODIFIED")),
		inputs: inputs,
	})
	e.requireSuccess(t, r1)
	r2 := e.run(t, &action{
		args:   helperArgs(step("cat", "shared.txt")),
		inputs: inputs,
	})
	e.requireSuccess(t, r2)
	assert.Equal(t, original, e.stdout(t, r2))
}

func testManyOutputs(t *testing.T, e *smokeEnv) {
	const n = 1000
	r := e.run(t, &action{
		args:        helperArgs(step("mkfiles", "outdir", strconv.Itoa(n))),
		outputPaths: []string{"outdir"},
	})
	e.requireSuccess(t, r)
	out := e.outputFiles(t, r)
	assert.Len(t, out, n)
	assert.Equal(t, "123", out["outdir/d03/f0123.txt"])
}

func testLongCommandLine(t *testing.T, e *smokeEnv) {
	// Long, but within the 32767 character limit for command lines on
	// Windows.
	var args []string
	for i := range 1000 {
		args = append(args, fmt.Sprintf("argument-%04d", i))
	}
	r := e.run(t, &action{args: helperArgs(append([]string{"echo-args"}, args...))})
	e.requireSuccess(t, r)
	var got []string
	require.NoError(t, json.Unmarshal([]byte(e.stdout(t, r)), &got))
	assert.Equal(t, args, got)
}

func testEnvNotInheritedFromExecutor(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{
		args: helperArgs(step("env", executorEnvSentinel, "SMOKE_SET")),
		env:  map[string]string{"SMOKE_SET": "set"},
	})
	e.requireSuccess(t, r)
	var got map[string]*string
	require.NoError(t, json.Unmarshal([]byte(e.stdout(t, r)), &got))
	assert.Nil(t, got[executorEnvSentinel], "action should not inherit the executor's environment")
	assert.NotNil(t, got["SMOKE_SET"])
}

func testMissingInputBlob(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{
		args: helperArgs(step("cat", "missing.txt")),
		inputs: map[string]inputFile{
			"missing.txt": {contents: []byte(fmt.Sprintf("never uploaded %d", time.Now().UnixNano()))},
		},
		skipUpload: []string{"missing.txt"},
	})
	// REAPI requires FAILED_PRECONDITION for missing inputs, so that the
	// client knows to re-upload them.
	assert.Equal(t, codes.FailedPrecondition, gstatus.FromProto(r.response.GetStatus()).Code(), "status: %v", r.response.GetStatus())
}

func testCommandNotFound(t *testing.T, e *smokeEnv) {
	r := e.run(t, &action{args: []string{"./does-not-exist"}})
	st := gstatus.FromProto(r.response.GetStatus())
	assert.True(t, st.Code() != codes.OK || r.ActionResult().GetExitCode() != 0,
		"running a missing binary should fail; status: %v, exit code: %d", st, r.ActionResult().GetExitCode())
}

func testUnsupportedIsolationType(t *testing.T, e *smokeEnv) {
	r, err := e.tryRun(t.Context(), &action{
		args:     helperArgs(step("print", "should not run")),
		platform: map[string]string{"workload-isolation-type": "firecracker"},
	})
	if err != nil {
		// The executor may refuse the task by re-enqueueing it.
		t.Logf("Executor refused task: %s", err)
		return
	}
	assert.NotEqual(t, codes.OK, gstatus.FromProto(r.response.GetStatus()).Code(), "status: %v", r.response.GetStatus())
}
