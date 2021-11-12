package app

import (
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

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"google.golang.org/grpc"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
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

var (
	portLeaser freePortLeaser
)

// App is a handle on a BuildBuddy server scoped to a test case, which provides
// basic facilities for connecting to the server and running builds against it.
type App struct {
	// err is the error returned by `cmd.Wait()`.
	err            error
	httpPort       int
	monitoringPort int
	gRPCPort       int
	mu             sync.Mutex
	exited         bool
}

// Run a local BuildBuddy server for the scope of the given test case.
//
// The given command path and config file path refer to the workspace-relative runfile
// paths of the BuildBuddy server binary and config file, respectively.
func Run(t *testing.T, commandPath string, commandArgs []string, configFilePath string) *App {
	dataDir := testfs.MakeTempDir(t)
	// NOTE: No SSL ports are required since the server doesn't have an SSL config by default.
	app := &App{
		httpPort:       FreePort(t),
		gRPCPort:       FreePort(t),
		monitoringPort: FreePort(t),
	}
	args := []string{
		"--app.log_level=debug",
		fmt.Sprintf("--config_file=%s", runfile(t, configFilePath)),
		fmt.Sprintf("--port=%d", app.httpPort),
		fmt.Sprintf("--grpc_port=%d", app.gRPCPort),
		fmt.Sprintf("--monitoring_port=%d", app.monitoringPort),
		"--static_directory=static",
		"--app_directory=/app",
		fmt.Sprintf("--app.build_buddy_url=http://localhost:%d", app.httpPort),
		"--database.data_source=sqlite3://:memory:",
		fmt.Sprintf("--storage.disk.root_directory=%s", filepath.Join(dataDir, "storage")),
		fmt.Sprintf("--cache.disk.root_directory=%s", filepath.Join(dataDir, "cache")),
	}
	args = append(args, commandArgs...)
	cmd := exec.Command(runfile(t, commandPath), args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
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
	if err := app.waitForReady(); err != nil {
		t.Fatal(err)
	}
	return app
}

// HTTPURL returns the URL for the web app.
func (a *App) HTTPURL() string {
	return fmt.Sprintf("http://localhost:%d", a.httpPort)
}

// GRPCAddress returns the gRPC address pointing to the app instance.
func (a *App) GRPCAddress() string {
	return fmt.Sprintf("grpc://localhost:%d", a.gRPCPort)
}

// BESBazelFlags returns the Bazel flags required to upload build logs to the App.
func (a *App) BESBazelFlags() []string {
	return []string{
		fmt.Sprintf("--bes_results_url=%s/invocation/", a.HTTPURL()),
		fmt.Sprintf("--bes_backend=%s", a.GRPCAddress()),
	}
}

// RemoteCacheBazelFlags returns the Bazel flags required to use the App's remote cache.
func (a *App) RemoteCacheBazelFlags() []string {
	return []string{
		fmt.Sprintf("--remote_cache=grpc://localhost:%d", a.gRPCPort),
	}
}

func (a *App) PublishBuildEventClient(t *testing.T) pepb.PublishBuildEventClient {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", a.gRPCPort), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
	return pepb.NewPublishBuildEventClient(conn)
}

func (a *App) BuildBuddyServiceClient(t *testing.T) bbspb.BuildBuddyServiceClient {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", a.gRPCPort), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
	return bbspb.NewBuildBuddyServiceClient(conn)
}

func runfile(t *testing.T, path string) string {
	resolvedPath, err := bazelgo.Runfile(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolvedPath
}

func FreePort(t testing.TB) int {
	return portLeaser.Lease(t)
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

type freePortLeaser struct {
	leasedPorts map[int]struct{}
	mu          sync.Mutex
}

func (p *freePortLeaser) findAPort(t testing.TB) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (p *freePortLeaser) Lease(t testing.TB) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.leasedPorts == nil {
		p.leasedPorts = make(map[int]struct{}, 0)
	}
	for {
		port := p.findAPort(t)
		if _, ok := p.leasedPorts[port]; !ok {
			p.leasedPorts[port] = struct{}{}
			return port
		}
	}
}
