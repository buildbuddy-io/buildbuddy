package buildbuddy

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/phayes/freeport"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

const (
	healthCheckPollInterval = 1 * time.Second
	startupTimeout          = 30 * time.Second
)

type App struct {
	httpPort       int
	gRPCPort       int
	telemetryPort  int
	monitoringPort int
	exited         bool
	mu             sync.Mutex
	stdout         bytes.Buffer
	stderr         bytes.Buffer
}

func freePort(t *testing.T) int {
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func Run(t *testing.T) *App {
	cmdPath, err := bazelgo.Runfile("server/cmd/buildbuddy/buildbuddy_/buildbuddy")
	if err != nil {
		t.Fatal(err)
	}
	dataDir, err := ioutil.TempDir("/tmp", "buildbuddy-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dataDir)
	})
	// NOTE: No SSL ports are required since the server doesn't have an SSL config by default.
	app := &App{
		httpPort:       freePort(t),
		gRPCPort:       freePort(t),
		telemetryPort:  freePort(t),
		monitoringPort: freePort(t),
	}
	cmd := exec.Command(
		cmdPath,
		fmt.Sprintf("--port=%d", app.httpPort),
		fmt.Sprintf("--grpc_port=%d", app.gRPCPort),
		fmt.Sprintf("--telemetry_port=%d", app.telemetryPort),
		fmt.Sprintf("--monitoring_port=%d", app.monitoringPort),
		fmt.Sprintf("--app.build_buddy_url=http://localhost:%d", app.httpPort),
		"--database.data_source=sqlite3://:memory:",
		fmt.Sprintf("--storage.disk.root_directory=%s", filepath.Join(dataDir, "storage")),
		fmt.Sprintf("--cache.disk.root_directory=%s", filepath.Join(dataDir, "cache")),
	)
	cmd.Stdout = &app.stdout
	cmd.Stderr = &app.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		defer func() {
			app.mu.Lock()
			defer app.mu.Unlock()
			app.exited = true
		}()
		if err := cmd.Wait(); err != nil {
			if err, ok := err.(*exec.ExitError); ok {
				// If the server wasn't killed due to the registered `Cleanup` function,
				// It must have exited some other way, which is probably an error.
				if killedExitCode := -1; err.ExitCode() != killedExitCode {
					t.Fatal(err)
				}
				return
			}
		}
		t.Fatal(fmt.Sprintf("Server exited unexpectedly. Server logs: %s", string(app.stdout.Bytes())))
	}()
	t.Cleanup(func() {
		cmd.Process.Kill()
	})
	app.waitForHealthy(t)
	return app
}

func (a *App) waitForHealthy(t *testing.T) {
	start := time.Now()
	for {
		a.mu.Lock()
		exited := a.exited
		a.mu.Unlock()
		if exited {
			return
		}
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/healthz?server-type=buildbuddy-server", a.httpPort))
		if err != nil || !isOK(t, resp) {
			if time.Since(start) > startupTimeout {
				errMsg := ""
				if err == nil {
					errMsg = fmt.Sprintf("/healthz status: %d", resp.StatusCode)
				} else {
					errMsg = fmt.Sprintf("err: %s", err)
				}
				t.Fatal(fmt.Sprintf(
					"Server failed to start after %s (%s). Server logs:\n%s%s",
					startupTimeout, errMsg, string(a.stdout.Bytes()), string(a.stderr.Bytes()),
				))
			}
			time.Sleep(healthCheckPollInterval)
			continue
		}
		break
	}
}

func isOK(t *testing.T, resp *http.Response) bool {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body) == "ok"
}

func (a *App) BESBazelFlags() []string {
	return []string{
		fmt.Sprintf("--bes_results_url=http://localhost:%d/invocation/", a.httpPort),
		fmt.Sprintf("--bes_backend=grpc://localhost:%d", a.gRPCPort),
	}
}

func (a *App) RemoteCacheBazelFlags() []string {
	return []string{
		fmt.Sprintf("--remote_cache=grpc://localhost:%d", a.gRPCPort),
	}
}
