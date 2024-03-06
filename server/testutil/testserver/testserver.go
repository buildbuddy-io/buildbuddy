package testserver

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

type Server struct {
	t    testing.TB
	opts *Opts
	cmd  *exec.Cmd

	mu     sync.Mutex
	exited bool
	// err is the error returned by `cmd.Wait()`.
	err error
}

func runfile(t testing.TB, path string) string {
	resolvedPath, err := bazelgo.Runfile(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolvedPath
}

type Opts struct {
	BinaryPath            string
	Args                  []string
	HTTPPort              int
	HealthCheckServerType string
}

func Run(t testing.TB, opts *Opts) *Server {
	server := &Server{
		t:    t,
		opts: opts,
	}
	err := server.Start()
	require.NoError(t, err)
	server.WaitForReady()
	return server
}

func (s *Server) Start() error {
	t := s.t
	cmd := exec.Command(runfile(t, s.opts.BinaryPath), s.opts.Args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	t.Cleanup(func() {
		cmd.Process.Kill() // ignore errors
	})
	go func() {
		err := cmd.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.exited = true
		s.err = err
	}()
	s.cmd = cmd
	return nil
}

// Shutdown begins graceful shutdown. It does not wait for shutdown to complete.
func (s *Server) Shutdown() {
	s.cmd.Process.Signal(syscall.SIGTERM)
}

func isOK(resp *http.Response) (bool, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	return string(body) == "OK", nil
}

func (s *Server) WaitForReady() error {
	start := time.Now()
	for {
		s.mu.Lock()
		exited := s.exited
		err := s.err
		s.mu.Unlock()
		if exited {
			return fmt.Errorf("binary failed to start: %s", err)
		}
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/readyz?server-type=%s", s.opts.HTTPPort, s.opts.HealthCheckServerType))
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
			return fmt.Errorf("binary failed to start within %s: %s", readyCheckTimeout, errMsg)
		}
		time.Sleep(readyCheckPollInterval)
	}
}
