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
	readyCheckPollInterval = 500 * time.Millisecond
	readyCheckTimeout      = 30 * time.Second
	killedExitCode         = -1
)

type App struct {
	httpPort       int
	gRPCPort       int
	monitoringPort int
	mu             sync.Mutex
	exited         bool
	stdout         bytes.Buffer
	stderr         bytes.Buffer
}

// Run a local BuildBuddy server for the scope of the given test case.
//
// The `buildbuddy.local.yaml` config is used, except directories and ports
// are overwritten so that the test is hermetic.
func Run(t *testing.T) *App {
	cmdPath, err := bazelgo.Runfile("server/cmd/buildbuddy/buildbuddy_/buildbuddy")
	if err != nil {
		t.Fatal(err)
	}
	configPath, err := bazelgo.Runfile("config/buildbuddy.local.yaml")
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
		monitoringPort: freePort(t),
	}
	cmd := exec.Command(
		cmdPath,
		fmt.Sprintf("--config_file=%s", configPath),
		fmt.Sprintf("--port=%d", app.httpPort),
		fmt.Sprintf("--grpc_port=%d", app.gRPCPort),
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
	waitC := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		app.mu.Lock()
		defer app.mu.Unlock()
		app.exited = true
		waitC <- err
	}()
	readyC := make(chan error, 1)
	go func() {
		healthy, err := app.waitForReady()
		if err != nil {
			readyC <- err
		} else if healthy {
			readyC <- nil
		}
	}()
	t.Cleanup(func() {
		cmd.Process.Kill()
	})
	select {
	case err := <-readyC:
		if err != nil {
			t.Fatal(app.fmtErrorWithLogs(fmt.Errorf("error occurred during health check: %s", err)))
		}
		return app
	case err := <-waitC:
		// In case the server exits before it becomes healthy, show the logs
		// (usually this is due to misconfiguration)
		t.Fatal(app.fmtErrorWithLogs(fmt.Errorf("buildbuddy exited unexpectedly: %s", err)))
	}
	return nil // should never be reached
}

// BESBazelFlags returns the Bazel flags required to upload build logs to the BuildBuddy app.
func (a *App) BESBazelFlags() []string {
	return []string{
		fmt.Sprintf("--bes_results_url=http://localhost:%d/invocation/", a.httpPort),
		fmt.Sprintf("--bes_backend=grpc://localhost:%d", a.gRPCPort),
	}
}

func freePort(t *testing.T) int {
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func (a *App) fmtErrorWithLogs(err error) error {
	return fmt.Errorf(`%s
=== STDOUT ===
%s
=== STDERR ===
%s`, err, string(a.stdout.Bytes()), string(a.stderr.Bytes()))
}

func (a *App) waitForReady() (bool, error) {
	start := time.Now()
	for {
		a.mu.Lock()
		exited := a.exited
		a.mu.Unlock()
		// If the app process exited, don't keep polling in the background.
		if exited {
			return false, nil
		}
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/readyz?server-type=buildbuddy-server", a.httpPort))
		ok := false
		if err == nil {
			ok, err = isOK(resp)
		}
		if ok {
			return true, nil
		}
		if time.Since(start) > readyCheckTimeout {
			errMsg := ""
			if err == nil {
				errMsg = fmt.Sprintf("/healthz status: %d", resp.StatusCode)
			} else {
				errMsg = fmt.Sprintf("err: %s", err)
			}
			return false, fmt.Errorf("health check timed out after %s (%s)", readyCheckTimeout, errMsg)
		}
		time.Sleep(readyCheckPollInterval)
		continue
	}
}

func isOK(resp *http.Response) (bool, error) {
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	return string(body) == "OK", nil
}
