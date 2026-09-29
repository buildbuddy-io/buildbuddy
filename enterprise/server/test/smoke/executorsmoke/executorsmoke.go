// Package executorsmoke contains a smoke test suite for BuildBuddy executor
// binaries.
//
// The suite starts a given executor binary on the local host, registers it
// with a BuildBuddy app in a unique pool, and then runs a set of actions
// against it via the remote execution API. It is intended for verifying
// release artifacts for each supported OS/arch pair, so it only depends on
// public APIs and runs anywhere the executor itself runs.
package executorsmoke

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	executorBinary       = flag.String("executor_binary", "", "Path or http(s) URL of the executor binary under test.")
	executorArgs         = stringSliceFlag("executor_arg", "Extra flag to pass to the executor under test, e.g. --executor_arg=--executor.enable_podman=true. May be repeated.")
	expectedOS           = flag.String("expected_os", "", "OS the executor binary is expected to target (linux, darwin, windows). Defaults to the host OS.")
	expectedArch         = flag.String("expected_arch", "", "Arch the executor binary is expected to target (amd64, arm64, 386). Defaults to the host arch.")
	expectedVersion      = flag.String("expected_version", "", "If set, the version the executor must report, e.g. v2.200.0.")
	expectStaticBinary   = flag.Bool("expect_static_binary", false, "If set, require the executor to be a statically linked ELF binary.")
	isolationTypes       = stringSliceFlag("isolation_type", "Additional workload isolation type to smoke test (e.g. podman, firecracker), beyond the bare runner. The executor must be configured to support it via --executor_arg. May be repeated.")
	containerImage       = flag.String("container_image", "", "container-image platform property used for --isolation_type actions. If empty, the executor default is used.")
	registrationDeadline = flag.Duration("registration_timeout", 2*time.Minute, "How long to wait for the executor to register with the app.")
)

// Target describes the BuildBuddy app that the executor under test connects
// to.
type Target struct {
	// AppTarget is the gRPC target of the app, e.g. grpcs://remote.buildbuddy.io.
	AppTarget string
	// APIKey is used by the test client to upload inputs and execute actions.
	// May be empty if the app does not require auth.
	APIKey string
	// ExecutorAPIKey is the key the executor registers with. It must have the
	// REGISTER_EXECUTOR capability if the app requires executor auth.
	ExecutorAPIKey string
	// GroupID is the ID of the group that owns the API keys. If set, the suite
	// verifies the executor's registration details via GetExecutionNodes,
	// which requires AdminAPIKey (or APIKey) to have ORG_ADMIN capability.
	GroupID string
	// AdminAPIKey is used to look up registered executors. Defaults to APIKey.
	AdminAPIKey string
	// InstanceName is the remote instance name to use.
	InstanceName string
}

// Options configures which executor to test and what to expect of it.
type Options struct {
	// ExecutorBinary is a local path or http(s) URL of the executor binary.
	ExecutorBinary string
	// ExecutorArgs are appended to the default executor flags.
	ExecutorArgs []string
	// ExpectedOS and ExpectedArch are the platform the binary should target.
	// They default to the host platform.
	ExpectedOS, ExpectedArch string
	// ExpectedVersion is the version the executor must report, if non-empty.
	ExpectedVersion string
	// ExpectStaticBinary requires a statically linked ELF binary.
	ExpectStaticBinary bool
	// IsolationTypes are workload isolation types to test in addition to the
	// bare runner.
	IsolationTypes []string
	// ContainerImage is used for actions run with IsolationTypes.
	ContainerImage string
	// RegistrationTimeout bounds how long to wait for registration.
	RegistrationTimeout time.Duration
}

// OptionsFromFlags returns Options populated from command line flags.
func OptionsFromFlags() Options {
	return Options{
		ExecutorBinary:      *executorBinary,
		ExecutorArgs:        *executorArgs,
		ExpectedOS:          *expectedOS,
		ExpectedArch:        *expectedArch,
		ExpectedVersion:     *expectedVersion,
		ExpectStaticBinary:  *expectStaticBinary,
		IsolationTypes:      *isolationTypes,
		ContainerImage:      *containerImage,
		RegistrationTimeout: *registrationDeadline,
	}
}

type stringSlice []string

func (s *stringSlice) String() string     { return strings.Join(*s, ",") }
func (s *stringSlice) Set(v string) error { *s = append(*s, v); return nil }

func stringSliceFlag(name, usage string) *[]string {
	s := &stringSlice{}
	flag.Var(s, name, usage)
	return (*[]string)(s)
}

// suite holds state shared by all smoke test cases.
type suite struct {
	target Target
	opts   Options

	ctx context.Context
	env *real_environment.RealEnv
	bbs bbspb.BuildBuddyServiceClient
	// adminCtx is used for GetExecutionNodes.
	adminCtx context.Context
	df       repb.DigestFunction_Value
	workDir  string

	executor *executorProcess
	pool     string
	hostID   string
	os       string
	arch     string

	// toolPath is the local path of the smoke tool (this test binary).
	toolPath string
	// toolName is the name of the tool in each action's input root.
	toolName string
	// version is the version reported by the executor's metrics.
	version string
}

// Run runs the executor smoke test suite against the given target.
func Run(t *testing.T, target Target, opts Options) {
	require.NotEmpty(t, target.AppTarget, "app target is required")
	require.NotEmpty(t, opts.ExecutorBinary, "executor binary is required (--executor_binary)")
	if opts.ExpectedOS == "" {
		opts.ExpectedOS = runtime.GOOS
	}
	if opts.ExpectedArch == "" {
		opts.ExpectedArch = runtime.GOARCH
	}
	if opts.RegistrationTimeout == 0 {
		opts.RegistrationTimeout = 2 * time.Minute
	}

	s := &suite{
		target:   target,
		opts:     opts,
		df:       repb.DigestFunction_SHA256,
		pool:     "executor-smoke-" + uuid.New(),
		hostID:   "executor-smoke-" + uuid.New(),
		os:       opts.ExpectedOS,
		arch:     opts.ExpectedArch,
		toolName: "smoketool",
	}
	if runtime.GOOS == "windows" {
		s.toolName += ".exe"
	}
	var err error
	s.toolPath, err = os.Executable()
	require.NoError(t, err)
	s.workDir = makeWorkDir(t)

	binary, err := resolveExecutorBinary(opts.ExecutorBinary, s.workDir)
	require.NoError(t, err)

	if !t.Run("Binary", func(t *testing.T) { s.testBinary(t, binary) }) {
		t.FailNow()
	}

	s.executor = startExecutor(t, &executorConfig{
		binary:         binary,
		workDir:        s.workDir,
		logPath:        logPath(s.workDir),
		appTarget:      target.AppTarget,
		apiKey:         target.ExecutorAPIKey,
		pool:           s.pool,
		hostID:         s.hostID,
		extraArgs:      opts.ExecutorArgs,
		httpPort:       freePort(t),
		monitoringPort: freePort(t),
	})
	s.connect(t)

	if !t.Run("Health", s.testHealth) {
		t.FailNow()
	}
	t.Run("Version", s.testVersion)
	if !t.Run("Registration", s.testRegistration) {
		t.FailNow()
	}

	t.Run("Actions", func(t *testing.T) {
		t.Run("Stdio", s.testStdio)
		t.Run("ExitCode", s.testExitCode)
		t.Run("Inputs", s.testInputs)
		t.Run("Outputs", s.testOutputs)
		t.Run("LargeInputAndOutput", s.testLargeInputAndOutput)
		t.Run("EnvironmentVariables", s.testEnvironmentVariables)
		t.Run("WorkingDirectory", s.testWorkingDirectory)
		t.Run("Timeout", s.testTimeout)
		t.Run("ActionCache", s.testActionCache)
		t.Run("RecycledRunner", s.testRecycledRunner)
		t.Run("Concurrent", s.testConcurrent)
		t.Run("NativeShell", s.testNativeShell)
		for _, isolationType := range opts.IsolationTypes {
			t.Run("Isolation_"+isolationType, func(t *testing.T) { s.testIsolation(t, isolationType) })
		}
	})

	t.Run("Shutdown", s.testShutdown)
}

func makeWorkDir(t *testing.T) string {
	// Keep the path short, since Windows has path length limits and the
	// executor nests action workspaces a few levels deep.
	parent := os.Getenv("TEST_TMPDIR")
	if runtime.GOOS == "windows" {
		parent = ""
	}
	dir, err := os.MkdirTemp(parent, "exsmoke-")
	require.NoError(t, err)
	t.Cleanup(func() {
		// The executor may leave read-only files behind; cleanup is best
		// effort since the directory is temporary anyway.
		_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err == nil {
				_ = os.Chmod(p, 0755)
			}
			return nil
		})
		_ = os.RemoveAll(dir)
	})
	return dir
}

// logPath returns where executor logs should be written: the undeclared
// outputs dir under Bazel, so they are available after the test, or else
// the work dir.
func logPath(workDir string) string {
	if dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR"); dir != "" {
		return filepath.Join(dir, "executor.log")
	}
	return filepath.Join(workDir, "executor.log")
}

func (s *suite) connect(t *testing.T) {
	conn, err := grpc_client.DialSimple(s.target.AppTarget)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	env := real_environment.NewBatchEnv()
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))
	s.env = env
	s.bbs = bbspb.NewBuildBuddyServiceClient(conn)
	s.ctx = withAPIKey(context.Background(), s.target.APIKey)
	s.adminCtx = withAPIKey(context.Background(), cmp.Or(s.target.AdminAPIKey, s.target.APIKey))
}

func withAPIKey(ctx context.Context, apiKey string) context.Context {
	if apiKey == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
}

func (s *suite) testBinary(t *testing.T, binary string) {
	info, err := inspectBinary(binary)
	require.NoError(t, err)
	t.Logf("Executor binary %s targets %s/%s (static=%t)", binary, info.OS, info.Arch, info.Static)
	assert.Equal(t, s.os, info.OS, "executor binary OS")
	assert.Equal(t, s.arch, info.Arch, "executor binary arch")
	if s.opts.ExpectStaticBinary {
		assert.True(t, info.Static, "executor binary should be statically linked")
	}
	require.Equal(t, runtime.GOOS, info.OS, "the executor binary must target the OS of the host running this test")
}

func (s *suite) testHealth(t *testing.T) {
	require.NoError(t, s.executor.waitHealthy(s.ctx, "/healthz"), "liveness check")
	require.NoError(t, s.executor.waitHealthy(s.ctx, "/readyz"), "readiness check")
}

func (s *suite) testVersion(t *testing.T) {
	version, commit, err := s.executor.versionFromMetrics(s.ctx)
	require.NoError(t, err)
	t.Logf("Executor reports version %q, commit %q", version, commit)
	s.version = version
	if s.opts.ExpectedVersion != "" {
		assert.Equal(t, s.opts.ExpectedVersion, version, "executor version")
	}
}

// testRegistration waits for the executor to register with the app. If a
// group ID is known, the registration details are checked via
// GetExecutionNodes; otherwise, registration is verified by successfully
// executing an action in the executor's unique pool.
func (s *suite) testRegistration(t *testing.T) {
	if s.target.GroupID != "" {
		ctx, cancel := context.WithTimeout(s.adminCtx, s.opts.RegistrationTimeout)
		defer cancel()
		node := s.waitForExecutionNode(t, ctx)
		assert.Equal(t, s.pool, node.GetPool())
		assert.Equal(t, s.os, node.GetOsFamily())
		assert.Equal(t, s.arch, node.GetArch())
		assert.Contains(t, node.GetSupportedIsolationTypes(), "none", "bare runner should be supported")
		for _, isolationType := range s.opts.IsolationTypes {
			assert.Contains(t, node.GetSupportedIsolationTypes(), isolationType)
		}
		assert.Positive(t, node.GetAssignableMilliCpu())
		assert.Positive(t, node.GetAssignableMemoryBytes())
		if s.version != "" {
			assert.Equal(t, s.version, node.GetVersion())
		}
		t.Logf("Executor registered: host=%q os=%q (%s) arch=%q isolation=%v cpu=%dm mem=%dB",
			node.GetHost(), node.GetOsFamily(), node.GetOsDisplayName(), node.GetArch(),
			node.GetSupportedIsolationTypes(), node.GetAssignableMilliCpu(), node.GetAssignableMemoryBytes())
	}
	// Whether or not we could see the registration, make sure tasks can be
	// routed to the executor. Retry while the executor is still connecting.
	ctx, cancel := context.WithTimeout(s.ctx, s.opts.RegistrationTimeout)
	defer cancel()
	var lastErr error
	for {
		res, err := s.execute(ctx, &action{tool: []string{"info"}, doNotCache: true})
		if err == nil {
			s.checkRanOnExecutor(t, res)
			require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)
			t.Logf("Probe action output:\n%s", res.Stdout)
			require.Contains(t, res.Stdout, fmt.Sprintf("platform=%s/%s", runtime.GOOS, runtime.GOARCH))
			return
		}
		lastErr = err
		select {
		case <-ctx.Done():
			require.FailNowf(t, "executor did not accept work", "last error: %s", lastErr)
		case <-time.After(1 * time.Second):
		}
		if s.executor.exited() {
			require.FailNowf(t, "executor exited", "%v", s.executor.exitErr())
		}
	}
}

func (s *suite) waitForExecutionNode(t *testing.T, ctx context.Context) *scpb.ExecutionNode {
	req := &scpb.GetExecutionNodesRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: s.target.GroupID},
	}
	var lastErr error
	for {
		rsp, err := s.bbs.GetExecutionNodes(ctx, req)
		if err == nil {
			for _, e := range rsp.GetExecutor() {
				if e.GetNode().GetExecutorHostId() == s.hostID {
					return e.GetNode()
				}
			}
			lastErr = fmt.Errorf("executor host ID %q not among %d registered executors", s.hostID, len(rsp.GetExecutor()))
		} else {
			lastErr = err
		}
		if s.executor.exited() {
			require.FailNowf(t, "executor exited", "%v", s.executor.exitErr())
		}
		select {
		case <-ctx.Done():
			require.FailNowf(t, "executor did not register", "last error: %s", lastErr)
		case <-time.After(1 * time.Second):
		}
	}
}

func (s *suite) testStdio(t *testing.T) {
	stdout := "hello from stdout " + uuid.New()
	stderr := "hello from stderr " + uuid.New()
	res := s.mustExecute(t, &action{tool: []string{"stdio", stdout, stderr}})
	assert.Equal(t, 0, int(res.ActionResult.GetExitCode()))
	assert.Equal(t, stdout, res.Stdout)
	assert.Equal(t, stderr, res.Stderr)
}

func (s *suite) testExitCode(t *testing.T) {
	res := s.mustExecute(t, &action{tool: []string{"exit", "42"}})
	assert.Equal(t, 42, int(res.ActionResult.GetExitCode()))
	assert.Contains(t, res.Stderr, "exiting with code 42")
}

func (s *suite) testInputs(t *testing.T) {
	m := &manifest{
		Files: []fileSpec{
			{Path: "top.txt", Size: 100, Seed: 1},
			{Path: "empty.txt", Size: 0, Seed: 2},
			{Path: "a/b/c/nested.bin", Size: 64 * 1024, Seed: 3},
			{Path: "a/sibling.bin", Size: 1, Seed: 4},
			{Path: "bin/tool.sh", Size: 10, Seed: 5, Executable: true},
			// Identical contents in different paths.
			{Path: "dup/one.txt", Size: 1000, Seed: 6},
			{Path: "dup/two.txt", Size: 1000, Seed: 6},
		},
		EmptyDirs: []dirSpec{{Path: "empty_dir"}, {Path: "a/b/empty_nested"}},
	}
	res := s.mustExecute(t, &action{
		tool:      []string{"check-inputs", "manifest.json"},
		inputs:    m,
		addInputs: true,
	})
	assert.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stdout: %s\nstderr: %s", res.Stdout, res.Stderr)
}

func (s *suite) testOutputs(t *testing.T) {
	m := &manifest{
		Files: []fileSpec{
			{Path: "out.txt", Size: 123, Seed: 11},
			// The executor is responsible for creating parent dirs of
			// declared outputs.
			{Path: "nested/dir/out.bin", Size: 4096, Seed: 12},
			{Path: "empty_out.txt", Size: 0, Seed: 13},
			{Path: "exec_out", Size: 10, Seed: 14, Executable: true},
			// Output directory contents.
			{Path: "outdir/x.txt", Size: 10, Seed: 15, Mkdir: true},
			{Path: "outdir/sub/y.txt", Size: 20, Seed: 16, Mkdir: true},
		},
		EmptyDirs: []dirSpec{{Path: "outdir/emptysub"}},
	}
	res := s.mustExecute(t, &action{
		tool:        []string{"write-outputs", "manifest.json"},
		inputs:      m,
		outputPaths: []string{"out.txt", "nested/dir/out.bin", "empty_out.txt", "exec_out", "outdir"},
	})
	require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)
	s.checkOutputs(t, res.ActionResult, m, map[string]bool{"outdir": true})
}

func (s *suite) testLargeInputAndOutput(t *testing.T) {
	const size = 64 * 1024 * 1024
	in := &manifest{Files: []fileSpec{{Path: "large_input.bin", Size: size, Seed: 21}}}
	res := s.mustExecute(t, &action{
		tool:      []string{"check-inputs", "manifest.json"},
		inputs:    in,
		addInputs: true,
	})
	require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)

	out := &manifest{Files: []fileSpec{{Path: "large_output.bin", Size: size, Seed: 22}}}
	res = s.mustExecute(t, &action{
		tool:        []string{"write-outputs", "manifest.json"},
		inputs:      out,
		outputPaths: []string{"large_output.bin"},
	})
	require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)
	s.checkOutputs(t, res.ActionResult, out, nil)
}

func (s *suite) testEnvironmentVariables(t *testing.T) {
	value := "value with spaces, 'quotes' and \"double quotes\" " + uuid.New()
	res := s.mustExecute(t, &action{
		tool: []string{"env", "SMOKE_A", "SMOKE_B", "SMOKE_UNSET"},
		env:  []string{"SMOKE_A=" + value, "SMOKE_B="},
	})
	require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)
	assert.Equal(t, "SMOKE_A="+value+"\nSMOKE_B=\nSMOKE_UNSET unset\n", normalizeNewlines(res.Stdout))
}

func (s *suite) testWorkingDirectory(t *testing.T) {
	res := s.mustExecute(t, &action{
		tool:       []string{"write-outputs", "../../manifest.json"},
		workingDir: "some/subdir",
		inputs:     &manifest{Files: []fileSpec{{Path: "out.txt", Size: 5, Seed: 31}}},
		// Output paths are relative to the working directory.
		outputPaths: []string{"out.txt"},
		// Make sure the working directory exists in the input root.
		extraInputs: map[string]string{"some/subdir/.keep": ""},
	})
	require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)
	require.Len(t, res.ActionResult.GetOutputFiles(), 1)
	f := res.ActionResult.GetOutputFiles()[0]
	assert.Equal(t, "out.txt", f.GetPath())
	assert.Equal(t, digestOf(t, contents(31, 5)).GetHash(), f.GetDigest().GetHash())
}

func (s *suite) testTimeout(t *testing.T) {
	start := time.Now()
	res, err := s.execute(s.ctx, &action{
		tool:    []string{"sleep", "10m"},
		timeout: 3 * time.Second,
	})
	require.NoError(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(res.Err), "execution error: %v", res.Err)
	assert.Less(t, time.Since(start), 2*time.Minute, "timed-out action should be killed promptly")
}

func (s *suite) testActionCache(t *testing.T) {
	// Salt the action so it isn't already cached from an earlier run.
	salt := uuid.New()
	a := &action{
		tool:        []string{"stdio", "cached " + salt, ""},
		cacheLookup: true,
	}
	res := s.mustExecute(t, a)
	require.Equal(t, 0, int(res.ActionResult.GetExitCode()))
	assert.False(t, res.Response.ExecuteResponse.GetCachedResult(), "first execution should not be cached")

	// The executor should have written the result to the action cache.
	acRes, err := s.env.GetActionCacheClient().GetActionResult(s.ctx, &repb.GetActionResultRequest{
		InstanceName:   s.target.InstanceName,
		ActionDigest:   res.ActionDigest,
		DigestFunction: s.df,
	})
	require.NoError(t, err, "action result should be in the action cache")
	assert.Equal(t, res.ActionResult.GetStdoutDigest().GetHash(), acRes.GetStdoutDigest().GetHash())

	res2 := s.mustExecute(t, a)
	assert.True(t, res2.Response.ExecuteResponse.GetCachedResult(), "second execution should be served from the action cache")
	assert.Equal(t, "cached "+salt, res2.Stdout)
}

func (s *suite) testRecycledRunner(t *testing.T) {
	if s.target.APIKey == "" {
		t.Skip("runner recycling requires an authenticated client")
	}
	// Runners are returned to the pool asynchronously after a task
	// completes, so a task can start on a fresh runner even though recycling
	// works. Keep running tasks until one reuses a previously seen runner.
	props := []string{"recycle-runner=true"}
	seen := map[string]bool{}
	deadline := time.Now().Add(1 * time.Minute)
	for i := 0; ; i++ {
		msg := fmt.Sprintf("recycled run %d", i)
		res := s.mustExecute(t, &action{tool: []string{"stdio", msg, ""}, props: props})
		require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)
		require.Equal(t, msg, res.Stdout)
		md := s.runnerMetadata(t, res)
		require.NotEmpty(t, md.GetRunnerId(), "runner metadata should include a runner ID")
		t.Logf("Task %d ran on runner %q (task number %d)", i, md.GetRunnerId(), md.GetTaskNumber())
		if seen[md.GetRunnerId()] {
			require.Greater(t, md.GetTaskNumber(), int64(1), "reused runner should report task number > 1")
			return
		}
		require.Equal(t, int64(1), md.GetTaskNumber(), "new runner should report task number 1")
		seen[md.GetRunnerId()] = true
		if time.Now().After(deadline) {
			require.FailNowf(t, "runner was never recycled", "ran %d tasks on %d distinct runners", i+1, len(seen))
		}
		time.Sleep(1 * time.Second)
	}
}

// runnerMetadata returns the runner metadata that the executor recorded for
// the given execution. It is read from the cached ExecuteResponse, which is
// written asynchronously after the execution completes.
func (s *suite) runnerMetadata(t *testing.T, res *actionResult) *espb.RunnerMetadata {
	t.Helper()
	var execRes *repb.ExecuteResponse
	require.Eventually(t, func() bool {
		var err error
		execRes, err = rexec.GetCachedExecuteResponse(s.ctx, s.env.GetActionCacheClient(), res.Name)
		return err == nil
	}, 1*time.Minute, 250*time.Millisecond, "cached ExecuteResponse for %s", res.Name)
	aux := &espb.ExecutionAuxiliaryMetadata{}
	ok, err := rexec.FindFirstAuxiliaryMetadata(execRes.GetResult().GetExecutionMetadata(), aux)
	require.NoError(t, err)
	require.True(t, ok, "execution metadata should include auxiliary metadata")
	return aux.GetRunnerMetadata()
}

func (s *suite) testConcurrent(t *testing.T) {
	const n = 16
	eg := errgroup.Group{}
	results := make([]*actionResult, n)
	for i := range n {
		eg.Go(func() error {
			m := &manifest{Files: []fileSpec{{Path: "out.bin", Size: 10_000, Seed: uint64(100 + i)}}}
			res, err := s.execute(s.ctx, &action{
				tool:        []string{"write-outputs", "manifest.json"},
				inputs:      m,
				outputPaths: []string{"out.bin"},
			})
			results[i] = res
			return err
		})
	}
	require.NoError(t, eg.Wait())
	for i, res := range results {
		require.NoError(t, res.Err, "action %d", i)
		s.checkRanOnExecutor(t, res)
		require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "action %d stderr: %s", i, res.Stderr)
		require.Len(t, res.ActionResult.GetOutputFiles(), 1)
		assert.Equal(t, digestOf(t, contents(uint64(100+i), 10_000)).GetHash(), res.ActionResult.GetOutputFiles()[0].GetDigest().GetHash())
	}
}

// testNativeShell runs a command through the host's native shell rather than
// the smoke tool, since that's how most real actions (e.g. genrules) run.
func (s *suite) testNativeShell(t *testing.T) {
	var args []string
	if runtime.GOOS == "windows" {
		args = []string{"cmd.exe", "/C", "echo hello-shell> out.txt && type out.txt"}
	} else {
		args = []string{"/bin/sh", "-c", "echo hello-shell > out.txt && cat out.txt && uname -m"}
	}
	res := s.mustExecute(t, &action{args: args, outputPaths: []string{"out.txt"}})
	require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)
	assert.Contains(t, res.Stdout, "hello-shell")
	require.Len(t, res.ActionResult.GetOutputFiles(), 1)
	t.Logf("Shell output:\n%s", res.Stdout)
}

// testIsolation runs a simple shell action with the given workload isolation
// type. The smoke tool can't be used here since the action may run inside a
// container image where the tool binary isn't runnable.
func (s *suite) testIsolation(t *testing.T, isolationType string) {
	props := []string{"workload-isolation-type=" + isolationType}
	if s.opts.ContainerImage != "" {
		props = append(props, "container-image="+s.opts.ContainerImage)
	}
	res := s.mustExecute(t, &action{
		args:        []string{"sh", "-c", "echo hello-" + isolationType + " > out.txt && cat out.txt && uname -a"},
		props:       props,
		outputPaths: []string{"out.txt"},
	})
	require.Equal(t, 0, int(res.ActionResult.GetExitCode()), "stderr: %s", res.Stderr)
	assert.Contains(t, res.Stdout, "hello-"+isolationType)
	require.Len(t, res.ActionResult.GetOutputFiles(), 1)
	t.Logf("%s output:\n%s", isolationType, res.Stdout)
}

func (s *suite) testShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		require.False(t, s.executor.exited(), "executor exited unexpectedly: %v", s.executor.exitErr())
		s.executor.kill()
		t.Skip("graceful shutdown (SIGTERM) is not supported on windows")
	}
	require.NoError(t, s.executor.shutdown(1*time.Minute), "executor should exit cleanly on SIGTERM")
	if s.target.GroupID == "" {
		return
	}
	// The executor should unregister itself from the scheduler.
	req := &scpb.GetExecutionNodesRequest{RequestContext: &ctxpb.RequestContext{GroupId: s.target.GroupID}}
	require.Eventually(t, func() bool {
		rsp, err := s.bbs.GetExecutionNodes(s.adminCtx, req)
		if err != nil {
			return false
		}
		for _, e := range rsp.GetExecutor() {
			if e.GetNode().GetExecutorHostId() == s.hostID {
				return false
			}
		}
		return true
	}, 1*time.Minute, 1*time.Second, "executor should unregister after shutdown")
}

// action describes an action to run on the executor under test.
type action struct {
	// tool is the smoke tool subcommand and args to run.
	tool []string
	// args is a raw command to run instead of the smoke tool.
	args []string
	// inputs is written to manifest.json in the input root.
	inputs *manifest
	// addInputs also materializes the manifest's files in the input root.
	addInputs bool
	// extraInputs are additional input files, keyed by path.
	extraInputs map[string]string
	env         []string
	props       []string
	outputPaths []string
	workingDir  string
	timeout     time.Duration
	doNotCache  bool
	// cacheLookup allows the result to be served from the action cache.
	cacheLookup bool
}

type actionResult struct {
	*rexec.Response
	ActionDigest *repb.Digest
	ActionResult *repb.ActionResult
	Stdout       string
	Stderr       string
}

func (s *suite) mustExecute(t *testing.T, a *action) *actionResult {
	t.Helper()
	res, err := s.execute(s.ctx, a)
	require.NoError(t, err)
	require.NoError(t, res.Err, "execution failed")
	s.checkRanOnExecutor(t, res)
	return res
}

func (s *suite) checkRanOnExecutor(t *testing.T, res *actionResult) {
	t.Helper()
	if res.Response.ExecuteResponse.GetCachedResult() {
		return
	}
	assert.Equal(t, s.hostID, res.ActionResult.GetExecutionMetadata().GetWorker(), "action should run on the executor under test")
}

func (s *suite) execute(ctx context.Context, a *action) (*actionResult, error) {
	inputRoot, err := os.MkdirTemp(s.workDir, "input-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(inputRoot)
	if err := s.populateInputRoot(inputRoot, a); err != nil {
		return nil, fmt.Errorf("populate input root: %w", err)
	}

	args := a.args
	if len(a.tool) > 0 {
		args = append([]string{s.toolArgv0(a.workingDir), toolArg}, a.tool...)
	}
	env, err := rexec.MakeEnv(a.env...)
	if err != nil {
		return nil, err
	}
	props := []string{
		"OSFamily=" + s.os,
		"Arch=" + s.arch,
		"Pool=" + s.pool,
		// Run on the bare runner unless the action overrides this, since
		// enabling other isolation types changes the executor's default.
		"workload-isolation-type=none",
	}
	if s.target.APIKey != "" {
		// Route to the group's own executors rather than any shared pool.
		props = append(props, "use-self-hosted-executors=true")
	}
	platform, err := rexec.MakePlatform(append(props, a.props...)...)
	if err != nil {
		return nil, err
	}
	cmd := &repb.Command{
		Arguments:            args,
		EnvironmentVariables: env,
		Platform:             platform,
		OutputPaths:          a.outputPaths,
		WorkingDirectory:     a.workingDir,
	}
	act := &repb.Action{
		DoNotCache: a.doNotCache,
		Platform:   platform,
	}
	if a.timeout > 0 {
		act.Timeout = durationpb.New(a.timeout)
	}
	arn, err := rexec.Prepare(ctx, s.env, s.target.InstanceName, s.df, act, cmd, inputRoot)
	if err != nil {
		return nil, fmt.Errorf("prepare action: %w", err)
	}
	stream, err := rexec.Start(ctx, s.env, arn, rexec.WithSkipCacheLookup(!a.cacheLookup))
	if err != nil {
		return nil, fmt.Errorf("start execution: %w", err)
	}
	rsp, err := rexec.Wait(stream)
	if err != nil {
		return nil, fmt.Errorf("wait execution: %w", err)
	}
	res := &actionResult{
		Response:     rsp,
		ActionDigest: arn.GetDigest(),
		ActionResult: rsp.ExecuteResponse.GetResult(),
	}
	if rsp.Err != nil {
		return res, nil
	}
	cmdResult, err := rexec.GetResult(ctx, s.env, s.target.InstanceName, s.df, res.ActionResult)
	if err != nil {
		return nil, fmt.Errorf("get stdout/stderr: %w", err)
	}
	res.Stdout = string(cmdResult.Stdout)
	res.Stderr = string(cmdResult.Stderr)
	return res, nil
}

// toolArgv0 returns the path of the smoke tool relative to the given
// working directory within the input root.
func (s *suite) toolArgv0(workingDir string) string {
	rel := s.toolName
	if workingDir != "" {
		for range strings.SplitSeq(path.Clean(workingDir), "/") {
			rel = "../" + rel
		}
	} else {
		rel = "./" + rel
	}
	return filepath.FromSlash(rel)
}

func (s *suite) populateInputRoot(dir string, a *action) error {
	if len(a.tool) > 0 {
		if err := linkOrCopy(s.toolPath, filepath.Join(dir, s.toolName)); err != nil {
			return err
		}
	}
	if a.inputs != nil {
		b, err := json.Marshal(a.inputs)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0644); err != nil {
			return err
		}
		if a.addInputs {
			for _, f := range a.inputs.Files {
				p := filepath.Join(dir, filepath.FromSlash(f.Path))
				if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
					return err
				}
				mode := os.FileMode(0644)
				if f.Executable {
					mode = 0755
				}
				if err := os.WriteFile(p, contents(f.Seed, f.Size), mode); err != nil {
					return err
				}
				// WriteFile doesn't apply mode to existing files or through
				// the umask; set it explicitly.
				if err := os.Chmod(p, mode); err != nil {
					return err
				}
			}
			for _, d := range a.inputs.EmptyDirs {
				if err := os.MkdirAll(filepath.Join(dir, filepath.FromSlash(d.Path)), 0755); err != nil {
					return err
				}
			}
		}
	}
	for p, content := range a.extraInputs {
		fp := filepath.Join(dir, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(fp, []byte(content), 0644); err != nil {
			return err
		}
	}
	return nil
}

func linkOrCopy(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// checkOutputs verifies that the action result contains exactly the outputs
// described by the manifest. Files under any of the given outputDirs are
// expected within the corresponding output directory tree.
func (s *suite) checkOutputs(t *testing.T, ar *repb.ActionResult, m *manifest, outputDirs map[string]bool) {
	t.Helper()
	wantFiles := map[string]*fileSpec{}
	wantDirFiles := map[string]map[string]*fileSpec{}
	wantEmptyDirs := map[string][]string{}
	for i := range m.Files {
		f := &m.Files[i]
		if dir, rel, ok := splitOutputDir(f.Path, outputDirs); ok {
			if wantDirFiles[dir] == nil {
				wantDirFiles[dir] = map[string]*fileSpec{}
			}
			wantDirFiles[dir][rel] = f
		} else {
			wantFiles[f.Path] = f
		}
	}
	for _, d := range m.EmptyDirs {
		if dir, rel, ok := splitOutputDir(d.Path, outputDirs); ok {
			wantEmptyDirs[dir] = append(wantEmptyDirs[dir], rel)
		}
	}

	gotFiles := map[string]*repb.OutputFile{}
	for _, f := range ar.GetOutputFiles() {
		gotFiles[f.GetPath()] = f
	}
	require.ElementsMatch(t, keys(wantFiles), keys(gotFiles), "output files")
	for p, want := range wantFiles {
		got := gotFiles[p]
		assert.Equal(t, digestOf(t, contents(want.Seed, want.Size)).GetHash(), got.GetDigest().GetHash(), "digest of %s", p)
		if runtime.GOOS != "windows" {
			assert.Equal(t, want.Executable, got.GetIsExecutable(), "is_executable of %s", p)
		}
		// Download the file to make sure it was actually uploaded.
		b := s.download(t, got.GetDigest())
		assert.True(t, bytes.Equal(contents(want.Seed, want.Size), b), "contents of %s", p)
	}

	gotDirs := map[string]*repb.OutputDirectory{}
	for _, d := range ar.GetOutputDirectories() {
		gotDirs[d.GetPath()] = d
	}
	require.ElementsMatch(t, keys(wantDirFiles), keys(gotDirs), "output directories")
	for dirPath, want := range wantDirFiles {
		tree := &repb.Tree{}
		rn := digest.NewCASResourceName(gotDirs[dirPath].GetTreeDigest(), s.target.InstanceName, s.df)
		require.NoError(t, cachetools.GetBlobAsProto(s.ctx, s.env.GetByteStreamClient(), rn, tree))
		files, emptyDirs := flattenTree(t, tree)
		require.ElementsMatch(t, keys(want), keys(files), "files in output directory %s", dirPath)
		for rel, spec := range want {
			assert.Equal(t, digestOf(t, contents(spec.Seed, spec.Size)).GetHash(), files[rel].GetDigest().GetHash(), "digest of %s/%s", dirPath, rel)
			b := s.download(t, files[rel].GetDigest())
			assert.True(t, bytes.Equal(contents(spec.Seed, spec.Size), b), "contents of %s/%s", dirPath, rel)
		}
		assert.ElementsMatch(t, wantEmptyDirs[dirPath], emptyDirs, "empty dirs in output directory %s", dirPath)
	}
}

func (s *suite) download(t *testing.T, d *repb.Digest) []byte {
	var buf bytes.Buffer
	rn := digest.NewCASResourceName(d, s.target.InstanceName, s.df)
	require.NoError(t, cachetools.GetBlob(s.ctx, s.env.GetByteStreamClient(), rn, &buf))
	return buf.Bytes()
}

func splitOutputDir(p string, outputDirs map[string]bool) (dir, rel string, ok bool) {
	for d := range outputDirs {
		if rel, ok := strings.CutPrefix(p, d+"/"); ok {
			return d, rel, true
		}
	}
	return "", "", false
}

// flattenTree returns the files in the tree keyed by slash-separated relative
// path, along with the relative paths of empty directories.
func flattenTree(t *testing.T, tree *repb.Tree) (map[string]*repb.FileNode, []string) {
	children := map[string]*repb.Directory{}
	for _, c := range tree.GetChildren() {
		children[digestOfProto(t, c).GetHash()] = c
	}
	files := map[string]*repb.FileNode{}
	var emptyDirs []string
	var walk func(dir *repb.Directory, prefix string)
	walk = func(dir *repb.Directory, prefix string) {
		for _, f := range dir.GetFiles() {
			files[prefix+f.GetName()] = f
		}
		for _, d := range dir.GetDirectories() {
			child, ok := children[d.GetDigest().GetHash()]
			require.True(t, ok, "tree missing child directory %s%s", prefix, d.GetName())
			if len(child.GetFiles()) == 0 && len(child.GetDirectories()) == 0 && len(child.GetSymlinks()) == 0 {
				emptyDirs = append(emptyDirs, prefix+d.GetName())
			}
			walk(child, prefix+d.GetName()+"/")
		}
	}
	walk(tree.GetRoot(), "")
	return files, emptyDirs
}

func digestOf(t *testing.T, b []byte) *repb.Digest {
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	return d
}

func digestOfProto(t *testing.T, dir *repb.Directory) *repb.Digest {
	d, err := digest.ComputeForMessage(dir, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	return d
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func normalizeNewlines(s string) string {
	return strings.ReplaceAll(s, "\r\n", "\n")
}
