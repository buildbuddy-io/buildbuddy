package testexecutor

import (
	"fmt"
	"testing"
	"time"

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

type Options struct {
	// GracefulShutdownTimeout enables SIGTERM-and-wait cleanup. This should
	// exceed the executor's --max_shutdown_duration to allow runner cleanup
	// to complete before the test force-kills the process.
	GracefulShutdownTimeout time.Duration
}

// Run a local BuildBuddy executor binary for the scope of the given test case.
func Run(t *testing.T, args ...string) *Executor {
	return RunWithOptions(t, &Options{}, args...)
}

// RunWithOptions is Run with explicit process lifecycle options.
func RunWithOptions(t *testing.T, opts *Options, args ...string) *Executor {
	t.Helper()
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
		HTTPPort:                e.httpPort,
		HealthCheckServerType:   "prod-buildbuddy-executor",
		GracefulShutdownTimeout: opts.GracefulShutdownTimeout,
	})

	return e
}

// MetricsURL returns the external executor's Prometheus metrics endpoint.
func (e *Executor) MetricsURL() string {
	return fmt.Sprintf("http://localhost:%d/metrics", e.monitoringPort)
}
