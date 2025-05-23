package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testserver"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// App is a handle on a BuildBuddy server scoped to a test case, which provides
// basic facilities for connecting to the server and running builds against it.
//
// NOTE: No SSL ports are required since the server doesn't have an SSL config by default.
type App struct {
	t *testing.T

	HttpPort       int
	MonitoringPort int
	GRPCPort       int

	// Path to local sqlite DB.
	dbFilePath string
}

// Run a local BuildBuddy server for the scope of the given test case.
//
// The given command path and config file path refer to the workspace-relative runfile
// paths of the BuildBuddy server binary and config file, respectively.
func Run(t *testing.T, commandPath string, commandArgs []string, configFilePath string) *App {
	app := &App{
		HttpPort:       testport.FindFree(t),
		GRPCPort:       testport.FindFree(t),
		MonitoringPort: testport.FindFree(t),
	}
	return RunWithApp(t, app, commandPath, commandArgs, configFilePath)

}

// createMemoryBackedDB attempts to create a uniquly-named file under /dev/shm and return its full path.
// If it cannot create the file, returns "".
func createMemoryBackedDB(t testing.TB) string {
	if _, err := os.Stat("/dev/shm"); errors.Is(err, os.ErrNotExist) {
		return ""
	}
	f, err := os.CreateTemp("/dev/shm", "buildbuddy-test-*.db")
	if err != nil {
		return ""
	}
	defer f.Close()
	return f.Name()
}

func RunWithApp(t *testing.T, app *App, commandPath string, commandArgs []string, configFilePath string) *App {
	dataDir := testfs.MakeTempDir(t)
	app.t = t
	app.dbFilePath = createMemoryBackedDB(t)
	if app.dbFilePath == "" {
		app.dbFilePath = filepath.Join(dataDir, "buildbuddy.db")
	}
	args := []string{
		"--app.log_level=debug",
		"--app.log_include_short_file_name",
		"--disable_telemetry",
		fmt.Sprintf("--config_file=%s", runfile(t, configFilePath)),
		fmt.Sprintf("--port=%d", app.HttpPort),
		fmt.Sprintf("--grpc_port=%d", app.GRPCPort),
		fmt.Sprintf("--internal_grpc_port=%d", testport.FindFree(t)),
		fmt.Sprintf("--monitoring_port=%d", app.MonitoringPort),
		"--static_directory=static",
		"--app_directory=/app",
		fmt.Sprintf("--app.build_buddy_url=http://localhost:%d", app.HttpPort),
		fmt.Sprintf("--database.data_source=sqlite3://%s", app.dbFilePath),
		fmt.Sprintf("--storage.disk.root_directory=%s", filepath.Join(dataDir, "storage")),
		fmt.Sprintf("--cache.disk.root_directory=%s", filepath.Join(dataDir, "cache")),
	}
	args = append(args, commandArgs...)

	testserver.Run(t, &testserver.Opts{
		BinaryRunfilePath:     commandPath,
		Args:                  args,
		HTTPPort:              app.HttpPort,
		HealthCheckServerType: "buildbuddy-server",
	})

	return app
}

// DB returns a direct connection to the sqlite DB that the app is connected to.
// This is intended for performing DB updates which are normally done manually,
// i.e. not exposed via any public API.
func (a *App) DB() *gorm.DB {
	db, err := gorm.Open(sqlite.Open(a.dbFilePath), &gorm.Config{})
	require.NoError(a.t, err)
	return db
}

// HTTPURL returns the URL for the web app.
func (a *App) HTTPURL() string {
	return fmt.Sprintf("http://localhost:%d", a.HttpPort)
}

// GRPCAddress returns the gRPC address pointing to the app instance.
func (a *App) GRPCAddress() string {
	return fmt.Sprintf("grpc://localhost:%d", a.GRPCPort)
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
		fmt.Sprintf("--remote_cache=grpc://localhost:%d", a.GRPCPort),
	}
}

// RemoteExecutorBazelFlags returns the Bazel flags required to use the App's remote cache.
func (a *App) RemoteExecutorBazelFlags() []string {
	return []string{
		fmt.Sprintf("--remote_executor=grpc://localhost:%d", a.GRPCPort),
	}
}

func (a *App) PublishBuildEventClient(t *testing.T) pepb.PublishBuildEventClient {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", a.GRPCPort), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
	return pepb.NewPublishBuildEventClient(conn)
}

func (a *App) BuildBuddyServiceClient(t *testing.T) bbspb.BuildBuddyServiceClient {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", a.GRPCPort), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
	return bbspb.NewBuildBuddyServiceClient(conn)
}

func (a *App) ByteStreamClient(t *testing.T) bspb.ByteStreamClient {
	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", a.GRPCPort), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
	return bspb.NewByteStreamClient(conn)
}

func runfile(t *testing.T, path string) string {
	resolvedPath, err := runfiles.Rlocation(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolvedPath
}
