package scheduler_shutdown_test

// Stress tests for graceful shutdown of a real enterprise app process with
// remote execution enabled. Unlike testserver (which SIGKILLs the app), these
// tests send SIGTERM and observe how shutdown actually goes.
//
// Run with --@io_bazel_rules_go//go/config:race so that the app binary is
// built with the race detector; any data races the app reports fail the test.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testexecutor"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

// set by x_defs in BUILD file
var (
	serverRlocationpath string
	configRlocationpath string
)

const maxShutdownDuration = 10 * time.Second

type appProcess struct {
	cmd      *exec.Cmd
	httpPort int
	grpcPort int
	logPath  string
	exited   chan struct{}
	exitErr  error
}

func rlocation(t *testing.T, path string) string {
	p, err := runfiles.Rlocation(path)
	require.NoError(t, err)
	return p
}

func startApp(t *testing.T, redisTarget string) *appProcess {
	dataDir := testfs.MakeTempDir(t)
	a := &appProcess{
		httpPort: testport.FindFree(t),
		grpcPort: testport.FindFree(t),
		logPath:  filepath.Join(dataDir, "app.log"),
		exited:   make(chan struct{}),
	}
	args := []string{
		"--app.log_include_short_file_name",
		"--disable_telemetry",
		"--telemetry_port=-1",
		"--config_file=" + rlocation(t, configRlocationpath),
		fmt.Sprintf("--port=%d", a.httpPort),
		fmt.Sprintf("--grpc_port=%d", a.grpcPort),
		fmt.Sprintf("--internal_grpc_port=%d", testport.FindFree(t)),
		fmt.Sprintf("--monitoring_port=%d", testport.FindFree(t)),
		"--static_directory=static",
		"--app_directory=/enterprise/app",
		fmt.Sprintf("--app.build_buddy_url=http://localhost:%d", a.httpPort),
		"--database.data_source=sqlite3://" + filepath.Join(dataDir, "buildbuddy.db"),
		"--storage.disk.root_directory=" + filepath.Join(dataDir, "storage"),
		"--cache.pebble.root_directory=" + filepath.Join(dataDir, "pebble"),
		"--app.default_redis_target=" + redisTarget,
		"--remote_execution.enable_remote_exec=true",
		fmt.Sprintf("--max_shutdown_duration=%s", maxShutdownDuration),
	}
	f, err := os.Create(a.logPath)
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	a.cmd = exec.Command(rlocation(t, serverRlocationpath), args...)
	a.cmd.Stdout = f
	a.cmd.Stderr = f
	require.NoError(t, a.cmd.Start())
	go func() {
		a.exitErr = a.cmd.Wait()
		close(a.exited)
	}()
	t.Cleanup(func() {
		select {
		case <-a.exited:
		default:
			_ = a.cmd.Process.Kill()
			<-a.exited
		}
	})

	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case <-a.exited:
			a.dumpLog(t, 100)
			require.FailNow(t, "app exited during startup", "%v", a.exitErr)
		default:
		}
		rsp, err := http.Get(fmt.Sprintf("http://localhost:%d/readyz?server-type=buildbuddy-server", a.httpPort))
		if err == nil {
			rsp.Body.Close()
			if rsp.StatusCode == 200 {
				return a
			}
		}
		require.True(t, time.Now().Before(deadline), "app did not become ready")
		time.Sleep(100 * time.Millisecond)
	}
}

func (a *appProcess) grpcTarget() string {
	return fmt.Sprintf("grpc://localhost:%d", a.grpcPort)
}

func (a *appProcess) dumpLog(t *testing.T, tailLines int) {
	b, _ := os.ReadFile(a.logPath)
	lines := strings.Split(string(b), "\n")
	if len(lines) > tailLines {
		lines = lines[len(lines)-tailLines:]
	}
	t.Logf("app log tail:\n%s", strings.Join(lines, "\n"))
}

var interestingLogPatterns = []string{
	"Caught",
	"Graceful stop of GRPC server succeeded",
	"Hard-stopping GRPC Server",
	"MaxShutdownDuration exceeded",
	"Error gracefully shutting down",
	"stopped",
	"panic:",
	"fatal error:",
	"re-enqueue",
	"Re-enqueueing",
}

// summarize logs interesting shutdown lines from the app log and returns any
// data race reports found.
func (a *appProcess) summarize(t *testing.T, since, until int64) []string {
	f, err := os.Open(a.logPath)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Seek(since, 0)
	require.NoError(t, err)
	var races []string
	var cur []string
	inRace := false
	var r io.Reader = f
	if until > since {
		r = io.LimitReader(f, until-since)
	}
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 1<<20), 1<<24)
	for s.Scan() {
		line := s.Text()
		if strings.Contains(line, "WARNING: DATA RACE") {
			inRace = true
			cur = nil
		}
		if inRace {
			cur = append(cur, line)
			if strings.HasPrefix(line, "==================") && len(cur) > 1 {
				races = append(races, strings.Join(cur, "\n"))
				inRace = false
			}
			continue
		}
		if until > since {
			continue
		}
		for _, p := range interestingLogPatterns {
			if strings.Contains(line, p) {
				t.Logf("APPLOG %s", line)
				break
			}
		}
	}
	return races
}

// raceSignatures condenses race reports into "access @ frame <-> previous
// access @ frame" signatures, using the first buildbuddy frame of each stack.
func raceSignatures(races []string) map[string]int {
	sigs := map[string]int{}
	for _, r := range races {
		var parts []string
		lines := strings.Split(r, "\n")
		for i, line := range lines {
			l := strings.TrimSpace(line)
			if !(strings.HasPrefix(l, "Read at") || strings.HasPrefix(l, "Write at") || strings.HasPrefix(l, "Previous read") || strings.HasPrefix(l, "Previous write")) {
				continue
			}
			kind := strings.Fields(l)[0]
			if kind == "Previous" {
				kind += " " + strings.Fields(l)[1]
			}
			frame := "?"
			for j := i + 1; j+1 < len(lines) && j < i+40; j += 2 {
				fn := strings.TrimSpace(lines[j])
				loc := strings.TrimSpace(lines[j+1])
				if fn == "" || strings.HasPrefix(fn, "Goroutine") || strings.HasPrefix(fn, "Previous") {
					break
				}
				if strings.Contains(fn, "buildbuddy-io/buildbuddy") && !strings.Contains(loc, "/interceptors/") {
					if k := strings.LastIndex(loc, " +0x"); k > 0 {
						loc = loc[:k]
					}
					frame = loc
					break
				}
			}
			parts = append(parts, kind+" @ "+frame)
		}
		sigs[strings.Join(parts, " <-> ")]++
	}
	return sigs
}

// stressClient mimics an executor that spams AskForMoreWork requests.
func stressClient(ctx context.Context, conn *grpc.ClientConn, id string) {
	stream, err := scpb.NewSchedulerClient(conn).RegisterAndStreamWork(ctx)
	if err != nil {
		return
	}
	node := &scpb.ExecutionNode{
		ExecutorId:            id,
		OsFamily:              runtime.GOOS,
		Arch:                  runtime.GOARCH,
		Host:                  "stress",
		AssignableMemoryBytes: 64_000_000_000,
		AssignableMilliCpu:    32_000,
	}
	if err := stream.Send(&scpb.RegisterAndStreamWorkRequest{RegisterExecutorRequest: &scpb.RegisterExecutorRequest{Node: node}}); err != nil {
		return
	}
	// Acknowledge reservations so that the scheduler doesn't wait on them.
	// Only this goroutine sends on the stream.
	acks := make(chan string, 1024)
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			if req := msg.GetEnqueueTaskReservationRequest(); req != nil {
				select {
				case acks <- req.GetTaskId():
				default:
				}
			}
		}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		var req *scpb.RegisterAndStreamWorkRequest
		select {
		case <-recvDone:
			return
		case taskID := <-acks:
			req = &scpb.RegisterAndStreamWorkRequest{EnqueueTaskReservationResponse: &scpb.EnqueueTaskReservationResponse{TaskId: taskID}}
		case <-ticker.C:
			req = &scpb.RegisterAndStreamWorkRequest{AskForMoreWorkRequest: &scpb.AskForMoreWorkRequest{}}
		}
		if err := stream.Send(req); err != nil {
			return
		}
	}
}

type scenario struct {
	name          string
	busy          bool
	stressClients int
}

func TestAppShutdown(t *testing.T) {
	for _, sc := range []scenario{
		{name: "idle"},
		{name: "idle_with_stress_clients", stressClients: 10},
		{name: "busy_build", busy: true},
		{name: "busy_build_with_stress_clients", busy: true, stressClients: 10},
	} {
		t.Run(sc.name, func(t *testing.T) {
			runScenario(t, sc)
		})
	}
}

func runScenario(t *testing.T, sc scenario) {
	redis := testredis.Start(t)
	app := startApp(t, redis.Target)
	_ = testexecutor.Run(t, "--executor.app_target="+app.grpcTarget())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()

	if sc.busy {
		contents := map[string]string{}
		var build strings.Builder
		for i := range 12 {
			fmt.Fprintf(&build, `genrule(
  name = "sleep_%d",
  outs = ["sleep_%d.txt"],
  cmd_bash = "sleep 30 && echo %d > $@",
  exec_properties = {"OSFamily": "%s", "Arch": "%s"},
  tags = ["no-remote-cache"],
)
`, i, i, i, runtime.GOOS, runtime.GOARCH)
		}
		contents["BUILD"] = build.String()
		ws := testbazel.MakeTempModule(t, contents)
		wg.Go(func() {
			res := testbazel.Invoke(ctx, t, ws, "build", "//...", "--jobs=20", "--remote_executor="+app.grpcTarget())
			t.Logf("bazel build finished (expected to fail after app shutdown): err=%v", res.Error)
		})
		deadline := time.Now().Add(90 * time.Second)
		for redis.KeyCount("task/*") == 0 {
			require.True(t, time.Now().Before(deadline), "no tasks were scheduled")
			time.Sleep(50 * time.Millisecond)
		}
		// Let executions get claimed and start running.
		time.Sleep(3 * time.Second)
		t.Logf("%d task keys in redis at SIGTERM", redis.KeyCount("task/*"))
	}

	if sc.stressClients > 0 {
		conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", app.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()
		for i := range sc.stressClients {
			wg.Go(func() { stressClient(ctx, conn, fmt.Sprintf("stress-%d", i)) })
		}
		time.Sleep(time.Second)
	}

	st, err := os.Stat(app.logPath)
	require.NoError(t, err)
	logOffset := st.Size()

	start := time.Now()
	require.NoError(t, app.cmd.Process.Signal(syscall.SIGTERM))
	hardLimit := maxShutdownDuration + 20*time.Second
	select {
	case <-app.exited:
	case <-time.After(hardLimit):
		// Dump goroutines to the log to see what's stuck.
		_ = app.cmd.Process.Signal(syscall.SIGQUIT)
		<-app.exited
		app.dumpLog(t, 400)
		require.FailNow(t, "app did not exit after SIGTERM", "waited %s", hardLimit)
	}
	elapsed := time.Since(start)
	exitCode := app.cmd.ProcessState.ExitCode()
	t.Logf("SUMMARY scenario=%s shutdown_time=%s exit_code=%d", sc.name, elapsed.Round(time.Millisecond), exitCode)

	preRaces := app.summarize(t, 0, logOffset)
	for sig, n := range raceSignatures(preRaces) {
		t.Logf("RACESIG pre-shutdown %dx: %s", n, sig)
	}
	races := app.summarize(t, logOffset, 0)
	for sig, n := range raceSignatures(races) {
		t.Logf("RACESIG shutdown %dx: %s", n, sig)
	}
	if len(races) > 0 {
		t.Logf("first shutdown race:\n%s", races[0])
	}
	assert.Empty(t, preRaces, "app reported data races before shutdown")
	require.Empty(t, races, "app reported data races during shutdown")
	if exitCode != 0 {
		app.dumpLog(t, 100)
	}
}
