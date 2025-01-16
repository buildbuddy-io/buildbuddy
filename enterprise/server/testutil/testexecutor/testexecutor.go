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
}

// set by x_defs in BUILD file
var executorRlocationpath string

// Run a local BuildBuddy executor binary for the scope of the given test case.
func Run(t *testing.T, args ...string) *Executor {
	e := &Executor{
		httpPort:       testport.FindFree(t),
		monitoringPort: testport.FindFree(t),
	}
	testserver.Run(t, &testserver.Opts{
		BinaryRunfilePath: executorRlocationpath,
		Args: append(
			args,
			"--app.log_level=debug",
			fmt.Sprintf("--port=%d", e.httpPort),
			fmt.Sprintf("--monitoring_port=%d", e.monitoringPort),
		),
		HTTPPort:              e.httpPort,
		HealthCheckServerType: "prod-buildbuddy-executor",
	})

	return e
}
