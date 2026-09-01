package testexecutor

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testserver"
)

// Executor is a handle on a BuildBuddy executor scoped to a test case.
type Executor struct {
	httpPort       int
	monitoringPort int

	server *testserver.Server
}

func (e *Executor) WaitForReady() error {
	return e.server.WaitForReady()
}

// set by x_defs in BUILD file
var executorRlocationpath string

type Opts struct {
	Args             []string
	DebugName        string
	SkipWaitForReady bool
}

// Run a local BuildBuddy executor binary for the scope of the given test case.
func RunWithOpts(t *testing.T, opts *Opts) *Executor {
	e := &Executor{
		httpPort:       testport.FindFree(t),
		monitoringPort: testport.FindFree(t),
	}
	s := testserver.Run(t, &testserver.Opts{
		DebugName:         opts.DebugName,
		BinaryRunfilePath: executorRlocationpath,
		Args: append(
			opts.Args,
			"--health_check_period=1s",
			"--app.log_level=debug",
			fmt.Sprintf("--port=%d", e.httpPort),
			fmt.Sprintf("--monitoring_port=%d", e.monitoringPort),
		),
		HTTPPort:              e.httpPort,
		HealthCheckServerType: "prod-buildbuddy-executor",
		SkipWaitForReady:      opts.SkipWaitForReady,
	})
	e.server = s
	return e
}

func Run(t *testing.T, args ...string) *Executor {
	return RunWithOpts(t, &Opts{Args: args})
}
