package testserver

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestShutdownProcess(t *testing.T) {
	for _, behavior := range []string{"graceful", "ignore", "exit"} {
		t.Run(behavior, func(t *testing.T) {
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(t.TempDir(), "shutdown-complete")
			cmd := exec.Command(executable, "-test.run=^TestServerProcess$")
			cmd.Env = append(os.Environ(), "TEST_SERVER_BEHAVIOR="+behavior, "TEST_SERVER_MARKER="+marker)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			// The readiness handshake ensures that the child's signal handler
			// is installed before stopProcess sends SIGTERM.
			line, err := bufio.NewReader(stdout).ReadString('\n')
			if err != nil || line != "ready\n" {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatalf("child readiness: %q, %v", line, err)
			}
			exited := make(chan struct{})
			var waitErr error
			go func() {
				waitErr = cmd.Wait()
				close(exited)
			}()
			t.Cleanup(func() {
				_ = cmd.Process.Kill()
				<-exited
			})

			timeout := 5 * time.Second
			if behavior == "ignore" {
				timeout = 50 * time.Millisecond
			}
			if behavior == "exit" {
				<-exited
			}
			err = stopProcess(cmd, exited, timeout)
			select {
			case <-exited:
			default:
				t.Fatal("stopProcess returned before cmd.Wait completed")
			}
			if behavior == "ignore" {
				if err == nil || !strings.Contains(err.Error(), "killed and reaped") {
					t.Fatalf("expected timeout error, got %v", err)
				}
				if waitErr == nil {
					t.Fatal("expected forced termination")
				}
				return
			}
			if err != nil || waitErr != nil {
				t.Fatalf("shutdown: %v; process wait: %v", err, waitErr)
			}
			if behavior == "graceful" {
				if _, err := os.Stat(marker); err != nil {
					t.Fatalf("shutdown handler did not complete before stopProcess returned: %v", err)
				}
			}
		})
	}
}

// TestServerProcess is invoked only as a subprocess by TestShutdownProcess.
func TestServerProcess(t *testing.T) {
	behavior := os.Getenv("TEST_SERVER_BEHAVIOR")
	if behavior == "" {
		t.Skip("subprocess helper")
	}
	signals := make(chan os.Signal, 1)
	if behavior == "ignore" {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(signals, syscall.SIGTERM)
	}
	fmt.Println("ready")
	if behavior == "exit" {
		os.Exit(0)
	}
	if behavior == "ignore" {
		for {
			time.Sleep(time.Hour)
		}
	}
	<-signals
	if err := os.WriteFile(os.Getenv("TEST_SERVER_MARKER"), []byte("done"), 0600); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
