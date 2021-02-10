package app

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

const (
	// readyCheckPollInterval determines how often to poll BuildBuddy server to check
	// whether it's up and running.
	readyCheckPollInterval = 500 * time.Millisecond
	// readyCheckTimeout determines how long to wait until giving up on waiting for
	// BuildBuddy server to become ready. If this timeout is reached, the test case
	// running the server will fail with a timeout error.
	readyCheckTimeout = 30 * time.Second
)

type App struct {
	httpPort       int
	gRPCPort       int
	monitoringPort int
	mu             sync.Mutex
	stdout         bytes.Buffer
	stderr         bytes.Buffer
	exited         bool
	// err is the error returned by `cmd.Wait()`.
	err error
}

// appParams are configurable via `RunOpt`s.
type appParams struct {
	cmdPath    string
	configPath string
	cmdArgs    []string
}

// Run a local BuildBuddy server for the scope of the given test case.
//
// Options may be given to override the default server options.
func Run(t *testing.T, opts ...RunOpt) *App {
	p := &appParams{cmdArgs: []string{}}
	for _, opt := range opts {
		if err := opt(p); err != nil {
			t.Fatal(err)
		}
	}
	if p.cmdPath == "" {
		t.Fatalf("missing `WithBuildBuddyCmdPath` opt")
	}
	if p.configPath == "" {
		t.Fatalf("missing `WithConfigPath` opt")
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
	cmdArgs := []string{
		fmt.Sprintf("--config_file=%s", p.configPath),
		fmt.Sprintf("--port=%d", app.httpPort),
		fmt.Sprintf("--grpc_port=%d", app.gRPCPort),
		fmt.Sprintf("--monitoring_port=%d", app.monitoringPort),
		fmt.Sprintf("--app.build_buddy_url=http://localhost:%d", app.httpPort),
		"--database.data_source=sqlite3://:memory:",
		fmt.Sprintf("--storage.disk.root_directory=%s", filepath.Join(dataDir, "storage")),
		fmt.Sprintf("--cache.disk.root_directory=%s", filepath.Join(dataDir, "cache")),
	}
	cmdArgs = append(cmdArgs, p.cmdArgs...)
	cmd := exec.Command(p.cmdPath, cmdArgs...)
	cmd.Stdout = &app.stdout
	cmd.Stderr = &app.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill() // ignore errors
	})
	go func() {
		err := cmd.Wait()
		app.mu.Lock()
		defer app.mu.Unlock()
		app.exited = true
		app.err = err
	}()
	err = app.waitForReady()
	if err != nil {
		t.Fatal(app.fmtErrorWithLogs(err))
	}
	return app
}

// BESBazelFlags returns the Bazel flags required to upload build logs to the App.
func (a *App) BESBazelFlags() []string {
	return []string{
		fmt.Sprintf("--bes_results_url=http://localhost:%d/invocation/", a.httpPort),
		fmt.Sprintf("--bes_backend=grpc://localhost:%d", a.gRPCPort),
	}
}

// RemoteCacheBazelFlags returns the Bazel flags required to use the App's remote cache.
func (a *App) RemoteCacheBazelFlags() []string {
	return []string{
		fmt.Sprintf("--remote_cache=grpc://localhost:%d", a.gRPCPort),
	}
}

// Opt customizes the BuildBuddy app instance.
type RunOpt func(*appParams) error

// WithBuildBuddyCmdPath sets the path to the BuildBuddy binary.
func WithBuildBuddyCmdPath(path string) RunOpt {
	return func(p *appParams) error {
		cmdPath, err := bazelgo.Runfile(path)
		if err != nil {
			return err
		}
		p.cmdPath = cmdPath
		return nil
	}
}

// WithConfigPath sets the path to the BuildBuddy server config.
func WithConfigPath(path string) RunOpt {
	return func(p *appParams) error {
		configPath, err := bazelgo.Runfile(path)
		if err != nil {
			return err
		}
		p.configPath = configPath
		return nil
	}
}

// WithArgs adds extra flags to the BuildBuddy server.
func WithFlags(args ...string) RunOpt {
	return func(p *appParams) error {
		p.cmdArgs = append(p.cmdArgs, args...)
		return nil
	}
}

func freePort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (a *App) fmtErrorWithLogs(err error) error {
	return fmt.Errorf(`%s
=== STDOUT ===
%s
=== STDERR ===
%s`, err, string(a.stdout.Bytes()), string(a.stderr.Bytes()))
}

func (a *App) waitForReady() error {
	start := time.Now()
	for {
		a.mu.Lock()
		exited := a.exited
		err := a.err
		a.mu.Unlock()
		if exited {
			return fmt.Errorf("app failed to start: %s", err)
		}
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/readyz?server-type=buildbuddy-server", a.httpPort))
		ok := false
		if err == nil {
			ok, err = isOK(resp)
		}
		if ok {
			return nil
		}
		if time.Since(start) > readyCheckTimeout {
			errMsg := ""
			if err == nil {
				errMsg = fmt.Sprintf("/readyz status: %d", resp.StatusCode)
			} else {
				errMsg = fmt.Sprintf("/readyz err: %s", err)
			}
			return fmt.Errorf("app failed to start within %s: %s", readyCheckTimeout, errMsg)
		}
		time.Sleep(readyCheckPollInterval)
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
