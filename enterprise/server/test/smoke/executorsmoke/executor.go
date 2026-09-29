package executorsmoke

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	executorServerType = "prod-buildbuddy-executor"
	readyTimeout       = 2 * time.Minute
)

// executorProcess is a handle on an executor binary started by the suite.
type executorProcess struct {
	cmd            *exec.Cmd
	logPath        string
	httpPort       int
	monitoringPort int

	done    chan struct{}
	mu      sync.Mutex
	waitErr error
}

type executorConfig struct {
	binary         string
	workDir        string
	logPath        string
	appTarget      string
	apiKey         string
	pool           string
	hostID         string
	extraArgs      []string
	httpPort       int
	monitoringPort int
}

func startExecutor(t *testing.T, cfg *executorConfig) *executorProcess {
	args := []string{
		"--executor.app_target=" + cfg.appTarget,
		"--executor.pool=" + cfg.pool,
		"--executor.host_id=" + cfg.hostID,
		"--executor.root_directory=" + filepath.Join(cfg.workDir, "remote_build"),
		"--executor.local_cache_directory=" + filepath.Join(cfg.workDir, "filecache"),
		"--executor.metadata_directory=" + filepath.Join(cfg.workDir, "metadata"),
		"--executor.enable_bare_runner=true",
		"--listen=127.0.0.1",
		fmt.Sprintf("--port=%d", cfg.httpPort),
		fmt.Sprintf("--monitoring_port=%d", cfg.monitoringPort),
	}
	if cfg.apiKey != "" {
		args = append(args, "--executor.api_key="+cfg.apiKey)
	}
	// Extra args come last so that they can override the defaults above.
	args = append(args, cfg.extraArgs...)

	logFile, err := os.Create(cfg.logPath)
	if err != nil {
		t.Fatalf("create executor log file: %s", err)
	}
	cmd := exec.Command(cfg.binary, args...)
	cmd.Dir = cfg.workDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	t.Logf("Starting executor: %s %s", cfg.binary, strings.Join(redactArgs(args), " "))
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("start executor: %s", err)
	}
	p := &executorProcess{
		cmd:            cmd,
		logPath:        cfg.logPath,
		httpPort:       cfg.httpPort,
		monitoringPort: cfg.monitoringPort,
		done:           make(chan struct{}),
	}
	go func() {
		err := cmd.Wait()
		logFile.Close()
		p.mu.Lock()
		p.waitErr = err
		p.mu.Unlock()
		close(p.done)
	}()
	t.Cleanup(func() {
		p.kill()
		if t.Failed() {
			t.Logf("Executor logs (%s):\n%s", p.logPath, tailFile(p.logPath, 300))
		}
	})
	return p
}

func (p *executorProcess) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *executorProcess) exitErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func (p *executorProcess) kill() {
	if !p.exited() {
		_ = p.cmd.Process.Kill()
		<-p.done
	}
}

// shutdown asks the executor to shut down gracefully and waits up to the
// given timeout for it to exit. Graceful shutdown is only supported on
// platforms with SIGTERM; elsewhere the process is killed.
func (p *executorProcess) shutdown(timeout time.Duration) (graceful bool, err error) {
	if runtime.GOOS == "windows" {
		p.kill()
		return false, nil
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return false, err
	}
	select {
	case <-p.done:
		return true, p.exitErr()
	case <-time.After(timeout):
		p.kill()
		return true, fmt.Errorf("executor did not exit within %s of SIGTERM", timeout)
	}
}

func (p *executorProcess) httpURL(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", p.httpPort, path)
}

func (p *executorProcess) monitoringURL(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", p.monitoringPort, path)
}

func (p *executorProcess) httpGet(ctx context.Context, url string) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer rsp.Body.Close()
	b, err := io.ReadAll(rsp.Body)
	return rsp.StatusCode, string(b), err
}

// waitHealthy waits for the executor's health endpoint to report OK.
func (p *executorProcess) waitHealthy(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	url := p.httpURL(path + "?server-type=" + executorServerType)
	var lastErr error
	for {
		if p.exited() {
			return fmt.Errorf("executor exited before becoming healthy: %v", p.exitErr())
		}
		code, body, err := p.httpGet(ctx, url)
		if err == nil && code == http.StatusOK && strings.TrimSpace(body) == "OK" {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("HTTP %d: %q", code, body)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not OK after %s: %v", path, readyTimeout, lastErr)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

var versionMetricRegexp = regexp.MustCompile(`(?m)^buildbuddy_version\{([^}]*)\}`)
var labelRegexp = regexp.MustCompile(`(\w+)="([^"]*)"`)

// versionFromMetrics returns the version and commit labels reported by the
// executor's buildbuddy_version metric.
func (p *executorProcess) versionFromMetrics(ctx context.Context) (version, commit string, err error) {
	code, body, err := p.httpGet(ctx, p.monitoringURL("/metrics"))
	if err != nil {
		return "", "", err
	}
	if code != http.StatusOK {
		return "", "", fmt.Errorf("/metrics: HTTP %d", code)
	}
	m := versionMetricRegexp.FindStringSubmatch(body)
	if m == nil {
		return "", "", fmt.Errorf("buildbuddy_version metric not found")
	}
	for _, l := range labelRegexp.FindAllStringSubmatch(m[1], -1) {
		switch l[1] {
		case "version":
			version = l[2]
		case "commit":
			commit = l[2]
		}
	}
	return version, commit, nil
}

// freePort returns a currently unused localhost TCP port.
func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %s", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func redactArgs(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		if strings.Contains(a, "api_key=") {
			a = a[:strings.Index(a, "=")+1] + "<redacted>"
		}
		out[i] = a
	}
	return out
}

func tailFile(path string, n int) string {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Sprintf("<failed to read %s: %s>", path, err)
	}
	defer f.Close()
	var lines []string
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for s.Scan() {
		lines = append(lines, s.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}
