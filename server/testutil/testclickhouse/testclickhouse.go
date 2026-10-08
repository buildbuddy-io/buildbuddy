package testclickhouse

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	clickhouseutil "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

const configXML = `
<clickhouse>
 <logger><level>information</level><console>1</console></logger>
 <listen_host>127.0.0.1</listen_host>
 <tcp_port>%d</tcp_port>
 <path>./data/</path>
 <tmp_path>./tmp/</tmp_path>
 <user_files_path>./user_files/</user_files_path>
 <format_schema_path>./format_schemas/</format_schema_path>
 <user_directories>
  <users_xml><path>users.xml</path></users_xml>
  <local_directory><path>./access/</path></local_directory>
 </user_directories>
 <background_schedule_pool_size>16</background_schedule_pool_size>
</clickhouse>
`

const usersXML = `
<clickhouse>
 <profiles><default/></profiles>
 <users>
  <default>
   <password/>
   <networks><ip>127.0.0.1</ip></networks>
   <profile>default</profile>
   <quota>default</quota>
   <access_management>1</access_management>
  </default>
 </users>
 <quotas><default/></quotas>
</clickhouse>
`

var (
	// Set by Bazel to the platform-specific ClickHouse executable.
	clickhouseRlocationpath string
	clickhouseBinaryPath    string
	targetsMu               sync.Mutex
	targets                 = map[testing.TB]string{}
)

func init() {
	path, err := runfiles.Rlocation(clickhouseRlocationpath)
	if err != nil {
		log.Fatalf("Failed to locate ClickHouse in runfiles: %s", err)
	}
	clickhouseBinaryPath, err = filepath.Abs(path)
	if err != nil {
		log.Fatalf("Failed to resolve absolute ClickHouse binary path: %s", err)
	}
}

// Configure starts ClickHouse and registers its database handle with env.
func Configure(t testing.TB, env *real_environment.RealEnv) {
	flags.Set(t, "olap_database.data_source", GetOrStart(t))
	// Cleanups run in reverse order. Finish OLAP shutdown hooks before Start's
	// cleanup stops the server, even if testenv was created before Configure.
	t.Cleanup(func() {
		env.GetHealthChecker().Shutdown()
		env.GetHealthChecker().WaitForGracefulShutdown()
	})
	require.NoError(t, clickhouseutil.Register(env))
}

// GetOrStart starts a new instance for the given test if one is not already
// running; otherwise it returns the existing target.
func GetOrStart(t testing.TB) string {
	targetsMu.Lock()
	defer targetsMu.Unlock()
	if target := targets[t]; target != "" {
		return target
	}
	target := Start(t)
	targets[t] = target
	t.Cleanup(func() {
		targetsMu.Lock()
		defer targetsMu.Unlock()
		delete(targets, t)
	})
	return target
}

// Start creates an isolated server which is stopped when the test completes.
func Start(t testing.TB) string {
	const dbName = "buildbuddy_test"

	port := testport.FindFree(t)
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.xml")
	require.NoError(t, os.WriteFile(configPath, fmt.Appendf(nil, configXML, port), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "users.xml"), []byte(usersXML), 0600))
	logPath := filepath.Join(dir, "server.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	t.Cleanup(func() { logFile.Close() })

	processCtx, stopProcess := context.WithCancel(context.Background())
	cmd := exec.CommandContext(processCtx, clickhouseBinaryPath, "server", "--config-file="+configPath)
	cmd.Dir = dir
	// Keep a single child so bounded shutdown cannot orphan a watchdog server.
	cmd.Env = append(os.Environ(), "CLICKHOUSE_WATCHDOG_ENABLE=0")
	// A file is safe for concurrent stdout/stderr writes and retains startup
	// diagnostics without buffering an unbounded amount of output in memory.
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	err = cmd.Start()
	if err != nil {
		stopProcess()
	}
	require.NoError(t, err)
	processDone := make(chan struct{})
	var processErr error
	t.Cleanup(func() {
		stopProcess()
		// CommandContext sends SIGTERM, then WaitDelay kills the process if
		// necessary. Reap it before the log file and temporary data are removed.
		<-processDone
	})
	startupCtx, stopStartup := context.WithTimeout(context.Background(), 15*time.Second)
	defer stopStartup()
	go func() {
		processErr = cmd.Wait()
		close(processDone)
		stopStartup()
	}()

	// Connect to the default database first: unlike the Docker entrypoint, the
	// native server does not create buildbuddy_test automatically.
	options, err := clickhouse.ParseDSN(fmt.Sprintf("clickhouse://default:@127.0.0.1:%d/default", port))
	require.NoError(t, err)
	options.DialTimeout = time.Second
	conn, err := clickhouse.Open(options)
	require.NoError(t, err)
	defer conn.Close()
	err = retry.DoVoid(startupCtx, &retry.Options{
		InitialBackoff:        100 * time.Millisecond,
		MaxBackoff:            100 * time.Millisecond,
		Multiplier:            1,
		MaxRetries:            math.MaxInt,
		DontLogFailedAttempts: true,
	}, func(ctx context.Context) error {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		return conn.Ping(attemptCtx)
	})
	if err == nil {
		err = conn.Exec(startupCtx, fmt.Sprintf("CREATE DATABASE `%s`", dbName))
	}
	if err != nil {
		logs, readErr := os.ReadFile(logPath)
		select {
		case <-processDone:
			t.Fatalf("ClickHouse exited during startup: %v (readiness error: %v; log read error: %v)\n%s", processErr, err, readErr, logs)
		default:
			t.Fatalf("ClickHouse did not become ready: %v (log read error: %v)\n%s", err, readErr, logs)
		}
	}
	return fmt.Sprintf("clickhouse://default:@127.0.0.1:%d/%s", port, dbName)
}
