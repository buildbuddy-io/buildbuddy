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
	stopTimeout  = 10 * time.Second
	logTailLines = 20
)

// StartDaemon starts the daemon in the background, if it's not already running.
func StartDaemon(cfg *tunnelconfig.Config) error {
	if daemonRunning(cfg.DNSListen) {
		fmt.Printf("The tunnel daemon is already running%s.\n", pidNote())
		return nil
	}
	if _, err := prepare(cfg); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	pidPath, err := runtimePath(pidFileName)
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
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	deadline := time.Now().Add(startTimeout)
	for !daemonRunning(cfg.DNSListen) {
		select {
		case err := <-exited:
			return fmt.Errorf("the tunnel daemon exited (%s); the end of %s:\n%s", exitStatus(err), logPath, logTail(logPath))
		case <-poll.C:
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			return fmt.Errorf("the tunnel daemon did not start within %s; the end of %s:\n%s", startTimeout, logPath, logTail(logPath))
		}
	}
	pid := cmd.Process.Pid
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(pid)+"\n"), 0o644); err != nil {
		return fmt.Errorf("the tunnel daemon is running (pid %d) but its pid file could not be written: %w", pid, err)
	}
	fmt.Printf("Started the tunnel daemon (pid %d), logging to %s. Stop it with: bbaccess tunnel stop\n", pid, logPath)
	return nil
}

// StopDaemon stops a daemon started with StartDaemon.
func StopDaemon(cfg *tunnelconfig.Config) error {
	pidPath, err := runtimePath(pidFileName)
	if err != nil {
		return err
	}
	pid, err := readPid(pidPath)
	if errors.Is(err, os.ErrNotExist) {
		if daemonRunning(cfg.DNSListen) {
			return fmt.Errorf("a tunnel daemon is answering on %s but was not started with bbaccess tunnel start; stop it where it runs", cfg.DNSListen)
		}
		fmt.Println("The tunnel daemon is not running.")
		return nil
	}
	if err != nil {
		return err
	}
	if !daemonRunning(cfg.DNSListen) {
		os.Remove(pidPath)
		fmt.Println("The tunnel daemon is not running.")
		return nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return fmt.Errorf("the tunnel daemon (pid %d) belongs to another user; run this with sudo", pid)
		}
		return fmt.Errorf("stopping the tunnel daemon (pid %d): %w", pid, err)
	}
	deadline := time.Now().Add(stopTimeout)
	for processAlive(pid) {
		if time.Now().After(deadline) {
			return fmt.Errorf("the tunnel daemon (pid %d) did not exit within %s", pid, stopTimeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
	os.Remove(pidPath)
	fmt.Printf("Stopped the tunnel daemon (pid %d).\n", pid)
	return nil
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

// pidNote describes the recorded pid for a message, or is empty.
func pidNote() string {
	path, err := runtimePath(pidFileName)
	if err != nil {
		return ""
	}
	pid, err := readPid(path)
	if err != nil {
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
