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

// Run a local BuildBuddy executor for the scope of the given test case.
//
// The given command path and config file path refer to the workspace-relative runfile
// paths of the executor server binary.
func Run(t *testing.T, commandPath string, commandArgs []string) *Executor {
	e := &Executor{
		httpPort:       testport.FindFree(t),
		monitoringPort: testport.FindFree(t),
	}
	args := []string{
		"--app.log_level=debug",
		fmt.Sprintf("--port=%d", e.httpPort),
		fmt.Sprintf("--monitoring_port=%d", e.monitoringPort),
	}
	args = append(args, commandArgs...)

	testserver.Run(t, &testserver.Opts{
		BinaryPath:            commandPath,
		Args:                  args,
		HTTPPort:              e.httpPort,
		HealthCheckServerType: "prod-buildbuddy-executor",
	})

	return e
}
