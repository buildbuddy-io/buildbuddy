package testserver

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

const (
	// readyCheckPollInterval determines how often to poll BuildBuddy server to check
	// whether it's up and running.
	readyCheckPollInterval = 500 * time.Millisecond
	// readyCheckTimeout determines how long to wait until giving up on waiting for
	// BuildBuddy server to become ready. If this timeout is reached, the test case
	// running the server will fail with a timeout error.
	readyCheckTimeout = 60 * time.Second

	// shutdownTimeout is how long to wait for the binary to exit after
	// sending it SIGTERM at the end of the test.
	// --max_shutdown_duration is 25s by default.
	shutdownTimeout = 35 * time.Second

	raceDetectedExitCode = 66
)

type Server struct {
	monitoringPort        int
	healthCheckServerType string
	// done is closed once `cmd.Wait()` returns.
	done chan struct{}
	// err is the error returned by `cmd.Wait()`. Only read it after done is
	// closed.
	err error
}

func runfile(t *testing.T, path string) string {
	resolvedPath, err := runfiles.Rlocation(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolvedPath
}

type Opts struct {
	BinaryRunfilePath     string
	Args                  []string
	HTTPPort              int
	HealthCheckServerType string
}

func Run(t *testing.T, opts *Opts) *Server {
	server := &Server{
		monitoringPort:        opts.HTTPPort,
		healthCheckServerType: opts.HealthCheckServerType,
		done:                  make(chan struct{}),
	}

	cmd := exec.Command(runfile(t, opts.BinaryRunfilePath), opts.Args...)
	cmd.Stdout = log.Writer("[testserver] ")
	cmd.Stderr = log.Writer("[testserver] ")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Shut the binary down gracefully and check that it exited cleanly.
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("Failed to send SIGTERM to %s: %s", opts.BinaryRunfilePath, err)
		}
		select {
		case <-server.done:
		case <-time.After(shutdownTimeout):
			cmd.Process.Kill() // ignore errors
			<-server.done
			t.Errorf("%s did not exit within %s of receiving SIGTERM", opts.BinaryRunfilePath, shutdownTimeout)
			return
		}
		switch exitCode := cmd.ProcessState.ExitCode(); exitCode {
		case 0:
		case raceDetectedExitCode:
			t.Errorf("%s exited with code %d, meaning it detected data races. See the test log for the race reports.", opts.BinaryRunfilePath, exitCode)
		default:
			t.Errorf("%s did not exit cleanly: %s", opts.BinaryRunfilePath, server.err)
		}
	})
	go func() {
		server.err = cmd.Wait()
		close(server.done)
	}()
	if err := server.waitForReady(); err != nil {
		t.Fatal(err)
	}
	return server
}

func isOK(resp *http.Response) (bool, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	return string(body) == "OK", nil
}

func (s *Server) waitForReady() error {
	start := time.Now()
	log.Debug("testserver waitForReady start")
	for i := 0; ; i++ {
		select {
		case <-s.done:
			return fmt.Errorf("binary failed to start: %s", s.err)
		default:
		}
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/readyz?server-type=%s", s.monitoringPort, s.healthCheckServerType))
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
			return fmt.Errorf("binary failed to start within %s (%d requests): %s", readyCheckTimeout, i, errMsg)
		}
		time.Sleep(readyCheckPollInterval)
	}
}
