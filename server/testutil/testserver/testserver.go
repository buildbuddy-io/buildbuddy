package testserver

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
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

	// exitTimeout is how long to wait for the binary to exit after killing it
	// at the end of the test, so that all of its output has been read.
	exitTimeout = 10 * time.Second
)

type Server struct {
	monitoringPort        int
	healthCheckServerType string
	mu                    sync.Mutex
	exited                bool
	// err is the error returned by `cmd.Wait()`.
	err error
	// done is closed once `cmd.Wait()` returns.
	done chan struct{}
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

	// If the binary was built with the race detector (e.g. --config=race),
	// it reports data races on its output but keeps running, so watch its
	// output and fail the test if any are reported.
	races := &raceDetector{}
	stdout := races.Writer(log.Writer("[testserver] "))
	stderr := races.Writer(log.Writer("[testserver] "))
	cmd := exec.Command(runfile(t, opts.BinaryRunfilePath), opts.Args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill() // ignore errors
		// Wait for the output to be fully read before checking for races.
		select {
		case <-server.done:
		case <-time.After(exitTimeout):
			t.Logf("%s did not exit within %s of being killed", opts.BinaryRunfilePath, exitTimeout)
		}
		stdout.Flush()
		stderr.Flush()
		if reports := races.Reports(); len(reports) > 0 {
			t.Errorf("%s reported %d data race(s). First report:\n%s", opts.BinaryRunfilePath, len(reports), reports[0])
		}
	})
	go func() {
		err := cmd.Wait()
		server.mu.Lock()
		defer server.mu.Unlock()
		server.exited = true
		server.err = err
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
		s.mu.Lock()
		exited := s.exited
		err := s.err
		s.mu.Unlock()
		if exited {
			return fmt.Errorf("binary failed to start: %s", err)
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

// raceDetector records data race reports printed by a binary built with the
// Go race detector.
type raceDetector struct {
	mu      sync.Mutex
	reports []string
}

// Writer returns a writer that passes output through to w while recording any
// race reports in it. Each output stream needs its own writer, so that lines
// from different streams aren't mixed together.
func (d *raceDetector) Writer(w io.Writer) *raceDetectingWriter {
	return &raceDetectingWriter{detector: d, w: w}
}

// Reports returns the race reports seen so far.
func (d *raceDetector) Reports() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.reports...)
}

func (d *raceDetector) add(report string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reports = append(d.reports, report)
}

type raceDetectingWriter struct {
	detector *raceDetector
	w        io.Writer

	// Output after the last newline.
	partial []byte
	// Lines of the race report currently being written, or nil if not in a
	// report.
	report []string
}

func (rw *raceDetectingWriter) Write(b []byte) (int, error) {
	rw.partial = append(rw.partial, b...)
	for {
		i := bytes.IndexByte(rw.partial, '\n')
		if i < 0 {
			break
		}
		rw.line(string(rw.partial[:i]))
		rw.partial = rw.partial[i+1:]
	}
	return rw.w.Write(b)
}

// A race report looks like:
//
//	==================
//	WARNING: DATA RACE
//	Write at 0x00c000123456 by goroutine 7:
//	  ...
//	==================
func (rw *raceDetectingWriter) line(line string) {
	if rw.report == nil {
		if strings.Contains(line, "WARNING: DATA RACE") {
			rw.report = []string{line}
		}
		return
	}
	if strings.HasPrefix(line, "==================") {
		rw.detector.add(strings.Join(rw.report, "\n"))
		rw.report = nil
		return
	}
	rw.report = append(rw.report, line)
}

// Flush records any race report that was cut off, e.g. because the binary
// was killed while writing it.
func (rw *raceDetectingWriter) Flush() {
	if len(rw.partial) > 0 {
		rw.line(string(rw.partial))
		rw.partial = nil
	}
	if rw.report != nil {
		rw.detector.add(strings.Join(rw.report, "\n"))
		rw.report = nil
	}
}
