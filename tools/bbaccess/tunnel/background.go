package tunnel

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/tunnelconfig"
	"github.com/buildbuddy-io/buildbuddy/tools/bbaccess/update"
)

const (
	pidFileName  = "tunnel.pid"
	logFileName  = "tunnel.log"
	startTimeout = 10 * time.Second
	stopGrace    = 5 * time.Second // after SIGTERM, before SIGKILL
	logTailLines = 20
)

// StartDaemon starts the daemon in the background, if it's not already running.
func StartDaemon(cfg *tunnelconfig.Config) error {
	if pid, held := runningPid(); held {
		fmt.Printf("The tunnel daemon is already running%s.\n", pidNote(pid))
		return nil
	}
	if daemonRunning(cfg.DNSListen) {
		return fmt.Errorf("something is already answering on %s; stop it first", cfg.DNSListen)
	}
	if _, err := prepare(cfg); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	logPath, err := runtimePath(logFileName)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return err
	}
	// Keep the previous run's log.
	if err := os.Rename(logPath, logPath+".1"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("rotating %s: %w", logPath, err)
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("creating %s: %w", logPath, err)
	}
	defer logFile.Close()

	cmd := exec.Command(exe, "tunnel", "run")
	cmd.Stdout, cmd.Stderr = logFile, logFile // stdin is /dev/null
	cmd.Dir = "/"
	// Skip the version check since we've already done one.
	cmd.Env = append(os.Environ(), update.NoUpdateEnv+"=1")
	cmd.SysProcAttr = detachAttr()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting the tunnel daemon: %w", err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	exited := func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}

	if !waitUntil(func() bool { return exited() || daemonRunning(cfg.DNSListen) }, startTimeout) {
		msg := fmt.Sprintf("the tunnel daemon did not start within %s", startTimeout)
		if err := stopProcess(cmd.Process, exited); err != nil {
			msg += fmt.Sprintf(" and could not be stopped (pid %d): %s", cmd.Process.Pid, err)
		}
		return fmt.Errorf("%s; the end of %s:\n%s", msg, logPath, logTail(logPath))
	}
	if exited() {
		return fmt.Errorf("the tunnel daemon exited (%s); the end of %s:\n%s", exitStatus(waitErr), logPath, logTail(logPath))
	}
	fmt.Printf("Started the tunnel daemon (pid %d), logging to %s. Stop it with: bbaccess tunnel stop\n", cmd.Process.Pid, logPath)
	return nil
}

// StopDaemon stops the daemon.
func StopDaemon(cfg *tunnelconfig.Config) error {
	pidPath, err := runtimePath(pidFileName)
	if err != nil {
		return err
	}
	pid, held := runningPid()
	if !held {
		if daemonRunning(cfg.DNSListen) {
			return fmt.Errorf("something is answering on %s but no tunnel daemon holds %s; stop it where it runs", cfg.DNSListen, pidPath)
		}
		os.Remove(pidPath) // left behind by a daemon that died
		fmt.Println("The tunnel daemon is not running.")
		return nil
	}
	if pid == 0 {
		return fmt.Errorf("the tunnel daemon is still starting; try again")
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	gone := func() bool { _, held := runningPid(); return !held }
	if err := stopProcess(proc, gone); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return fmt.Errorf("the tunnel daemon (pid %d) belongs to another user; run this with sudo", pid)
		}
		return fmt.Errorf("stopping the tunnel daemon (pid %d): %w", pid, err)
	}
	os.Remove(pidPath) // left behind if it had to be killed
	fmt.Printf("Stopped the tunnel daemon (pid %d).\n", pid)
	return nil
}

// stopProcess sends SIGTERM, then SIGKILL if gone does not report it ended
// within the grace period.
func stopProcess(p *os.Process, gone func() bool) error {
	if err := p.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	if waitUntil(gone, stopGrace) {
		return nil
	}
	if err := p.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	if waitUntil(gone, stopGrace) {
		return nil
	}
	return errors.New("still running after SIGKILL")
}

// waitUntil polls cond until it holds or timeout passes.
func waitUntil(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return true
}

// runtimePath returns the path of one of the daemon's run-time files, which
// live next to the config.
func runtimePath(name string) (string, error) {
	dir, err := tunnelconfig.Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, name), nil
}

var errLocked = errors.New("locked by another process")

// pidFile is held locked by the daemon for as long as it runs.
type pidFile struct {
	f    *os.File
	path string
}

// holdPidFile locks the pid file and records our pid in it.
func holdPidFile(path string) (*pidFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := lockExclusive(f); err != nil {
		f.Close()
		if errors.Is(err, errLocked) {
			pid, _ := lockedPid(path)
			return nil, fmt.Errorf("a tunnel daemon is already running%s", pidNote(pid))
		}
		return nil, fmt.Errorf("locking %s: %w", path, err)
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.WriteString(strconv.Itoa(os.Getpid()) + "\n"); err != nil {
		f.Close()
		return nil, err
	}
	return &pidFile{f: f, path: path}, nil
}

// release removes the file, then drops the lock.
func (p *pidFile) release() {
	os.Remove(p.path)
	p.f.Close()
}

// runningPid reports whether a daemon holds the pid file, and its pid (0 if
// it has not written it yet).
func runningPid() (int, bool) {
	path, err := runtimePath(pidFileName)
	if err != nil {
		return 0, false
	}
	return lockedPid(path)
}

func lockedPid(path string) (int, bool) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer f.Close()
	held, err := lockHeld(f)
	if err != nil || !held {
		return 0, false
	}
	pid, _ := readPid(path)
	return pid, true
}

func readPid(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	text := strings.TrimSpace(string(b))
	pid, err := strconv.Atoi(text)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("%s does not hold a pid: %q", path, text)
	}
	return pid, nil
}

// pidNote describes a pid for a message, or is empty.
func pidNote(pid int) string {
	if pid <= 0 {
		return ""
	}
	return fmt.Sprintf(" (pid %d)", pid)
}

func exitStatus(err error) string {
	if err == nil {
		return "exit status 0"
	}
	return err.Error()
}

// logTail returns the end of the log, for an error message.
func logTail(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return lastLines(string(b), logTailLines)
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
