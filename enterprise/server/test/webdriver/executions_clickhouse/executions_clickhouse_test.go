package executions_clickhouse_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/webtester"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestInvocationWithRemoteExecutionWithClickHouse(t *testing.T) {
	// This test can't run against cloud yet, since we depend on the test
	// running on the same filesystem as the executor to coordinate action
	// execution via fifo pipes.
	buildbuddy_enterprise.MarkTestLocalOnly(t)
	ctx := t.Context()
	clickhouseDSN := testclickhouse.Start(t, true /*=reuseServer*/)
	target := buildbuddy_enterprise.SetupWebTarget(
		t,
		"--remote_execution.enable_remote_exec=true",
		"--olap_database.data_source="+clickhouseDSN,
		// TODO(#8900): set flags to read Executions from ClickHouse.
	)
	// Register an executor so that we can test RBE end-to-end.
	_ = testexecutor.Run(t, "--executor.app_target="+target.GRPCAddress())
	// Log in and get the flags needed to run an authenticated, RBE-enabled
	// Bazel invocation.
	wt := webtester.New(t)
	webtester.Login(wt, target)
	buildbuddyBuildFlags := webtester.GetBazelBuildFlags(
		wt, target.HTTPURL(),
		webtester.WithEnableCache,
		webtester.WithEnableRemoteExecution,
	)

	controllerDir := testfs.MakeTempDir(t)
	genrule := NewControlledGenrule(t, controllerDir, "target1")
	workspaceContents := map[string]string{
		"WORKSPACE": "",
		"BUILD":     genrule.String(),
		".bazelrc": `
# Execution should not be flaky in this test; disable retries.
common --remote_retries=0
# Target the local buildbuddy server.
common ` + shlex.Quote(buildbuddyBuildFlags...) + `
common --incompatible_strict_action_env=true
`,
	}
	workspace1Path := testbazel.MakeTempWorkspace(t, workspaceContents)
	iid1 := uuid.New()
	buildArgs := []string{"//...", "--invocation_id=" + iid1}

	// Start the invocation in the background. It won't complete until we
	// signal the genrule to exit.
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait()
		require.NoError(t, err)
	})
	eg.Go(func() error {
		t.Log("Starting invocation " + iid1)
		return testbazel.Invoke(ctx, t, workspace1Path, "build", buildArgs...).Error
	})

	err := genrule.WaitStarted(ctx)
	require.NoError(t, err)

	// Before we complete the action, start an identical action in a new
	// invocation, which should get merged against the first execution.
	workspace2Path := testbazel.MakeTempWorkspace(t, workspaceContents)
	iid2 := uuid.New()
	buildArgs2 := []string{"//...", "--invocation_id=" + iid2}
	eg.Go(func() error {
		t.Log("Starting invocation " + iid2)
		return testbazel.Invoke(ctx, t, workspace2Path, "build", buildArgs2...).Error
	})

	// Now we've got two invocations running in parallel, each waiting on
	// the same action to complete.

	goToInvocationPage(t, wt, target.HTTPURL(), iid1)
	// Go to executions tab.
	wt.Find(`[href="#execution"]`).Click()
	waitForExecutionsToAppear(t, wt)

	// There should be one execution in in-progress state.
	execution := wt.Find(".invocation-execution-row")
	require.Regexp(t, "(Executing|Starting)", execution.Text())
	execution.Click()
	originalExecutionID := wt.Find(`[debug-id="execution-id"]`).Text()

	// Go to the second invocation page and load its executions.
	// We should see the original execution, still in progress.
	goToInvocationPage(t, wt, target.HTTPURL(), iid2)
	wt.Find(`[href="#execution"]`).Click()
	t.Log("Clicked executions tab; waiting for executions to appear")
	waitForExecutionsToAppear(t, wt)
	execution = wt.Find(".invocation-execution-row")
	require.Regexp(t, "(Executing|Starting)", execution.Text())

	// Now signal the action to exit.
	genrule.Eval("exit 0")

	// Still in the second invocation page, wait until we can see the successful
	// execution.
	waitForExecutionToSucceed(t, wt)

	// Click the execution and make sure the execution ID matches the original.
	execution = wt.Find(".invocation-execution-row")
	execution.Click()
	executionID2 := wt.Find(`[debug-id="execution-id"]`).Text()
	require.Equal(t, originalExecutionID, executionID2)

	// Now go to Drilldowns, drilldown by execution wall time, and click the
	// rectangle shown in the heatmap. This should select the invocations
	// we just performed.
	wt.Find(`[href="/trends/#drilldown"]`).Click()
	wt.Find(`.drilldown-page-select`).SendKeys("Execution total wall time")
	wt.Find(`[debug-id="heatmap-cells"] > *`).Click()
	// There should be two identical executions shown.
	waitForExecutionsToAppear(t, wt)
	require.Contains(t, wt.Find(`body`).Text(), "Selected executions (2)")
	executions := wt.FindAll(".invocation-execution-row")
	require.Len(t, executions, 2)
	require.Equal(t, executions[0].Text(), executions[1].Text())
}

func goToInvocationPage(t *testing.T, wt *webtester.WebTester, baseURL, iid string) {
	require.Eventually(t, func() bool {
		wt.Get(baseURL + "/invocation/" + iid)
		// Wait for either the invocation view or the empty state page to
		// appear. If it was the empty state page that appeared, keep retrying.
		wt.Find(`.invocation, .state-page, [debug-id="invocation-loading"]`)
		return len(wt.FindAll(`.invocation`)) > 0
	}, 1*time.Minute, 500*time.Millisecond, "go to invocation page")
}

func waitForExecutionsToAppear(t *testing.T, wt *webtester.WebTester) {
	require.Eventually(t, func() bool {
		executions := wt.Find(".invocation-execution-table, .invocation-execution-empty-actions").FindAll(".invocation-execution-row")
		return len(executions) > 0
	}, 1*time.Minute, 500*time.Millisecond, "wait for executions to appear")
}

func waitForExecutionToSucceed(t *testing.T, wt *webtester.WebTester) {
	require.Eventually(t, func() bool {
		executions := wt.Find(".invocation-execution-table").FindAll(".invocation-execution-row")
		require.Len(t, executions, 1)
		if strings.Contains(executions[0].Text(), "Succeeded") {
			return true
		}
		wt.Refresh()
		return false
	}, 1*time.Minute, 500*time.Millisecond, "wait for execution to succeed")
}

// ControlledGenrule represents a genrule action whose execution can be
// controlled by the test. This allows us to test in-progress invocation and
// execution state.
//
// Each ControlledGenrule is only good for one invocation.
type ControlledGenrule struct {
	t            *testing.T
	evalPipePath string
	started      chan struct{}
	buildRule    string
}

func NewControlledGenrule(t *testing.T, controllerDir, name string) *ControlledGenrule {
	// Create pipes for the build action to communicate with the test.
	startedPipePath := filepath.Join(controllerDir, name+".started.pipe")
	err := syscall.Mkfifo(startedPipePath, 0666)
	require.NoError(t, err)
	evalPipePath := filepath.Join(controllerDir, name+".eval.pipe")
	err = syscall.Mkfifo(evalPipePath, 0666)
	require.NoError(t, err)

	// Add a genrule that writes its pid to the "started" pipe when it starts,
	// then reads and executes commands from the "eval" pipe.
	script := `
echo $$ > ` + startedPipePath + `
while true; do eval "$(cat ` + evalPipePath + `)"; done
`
	// Escape Bazel's "make" variable syntax.
	escapedScript := strings.ReplaceAll(script, "$", "$$")
	ruleContents := fmt.Sprintf(
		"genrule(name = %q, outs = [%q], cmd_bash = %q)",
		name, name+".out", "touch $@\n"+escapedScript)
	c := &ControlledGenrule{
		t:            t,
		evalPipePath: evalPipePath,
		started:      make(chan struct{}),
		buildRule:    ruleContents,
	}
	waiterDone := make(chan struct{})
	startedPipe, err := os.OpenFile(startedPipePath, os.O_RDWR, 0)
	require.NoError(t, err)
	go func() {
		defer close(waiterDone)
		defer startedPipe.Close()
		// Read started process PID from the started pipe.
		var pid int
		_, err = fmt.Fscanf(startedPipe, "%d", &pid)
		if t.Context().Err() != nil { // Test is exiting.
			return
		}
		require.NoError(t, err, "read PID of started action")
		close(c.started)
	}()
	// Ensure the goroutine stops execution before returning from the test.
	t.Cleanup(func() {
		_ = startedPipe.Close()
		<-waiterDone
	})
	return c
}

// String returns the genrule() target definition for the controlled genrule.
func (c *ControlledGenrule) String() string {
	return c.buildRule
}

// WaitStarted blocks until the genrule has started execution or the context is
// done.
func (c *ControlledGenrule) WaitStarted(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.started:
		return nil
	}
}

// Eval writes a command to the genrule's eval pipe.
func (c *ControlledGenrule) Eval(command string) {
	err := os.WriteFile(c.evalPipePath, []byte(fmt.Sprintf("%s\n", command)), 0)
	require.NoError(c.t, err)
}
