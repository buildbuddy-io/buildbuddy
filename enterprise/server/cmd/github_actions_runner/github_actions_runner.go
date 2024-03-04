// This binary wraps https://github.com/actions/runner, starting the runner with
// the one-time-use "JIT" config provided as the environment variable
// $RUNNER_ENCODED_JITCONFIG. For more info about just-in-time runners, see
// https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions#using-just-in-time-runners
//
// It also handles killing the runner if it does not accept a job within the
// configured idle timeout, passed as $RUNNER_IDLE_TIMEOUT. This prevents runner
// tasks from waiting around forever in case the number of runners exceeds the
// number of available jobs, for example if a workflow job is canceled.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("Ephemeral GitHub Actions runner failed: %s", err)
	}
}

func run() error {
	// Get config from env
	jitconfig := os.Getenv("RUNNER_ENCODED_JITCONFIG")
	if jitconfig == "" {
		return fmt.Errorf("missing RUNNER_ENCODED_JITCONFIG in env")
	}
	// Clear JITCONFIG to avoid accidentally logging it.
	if err := os.Unsetenv("RUNNER_ENCODED_JITCONFIG"); err != nil {
		return fmt.Errorf("unsetenv: %s", err)
	}
	idleTimeoutSeconds, err := strconv.Atoi(os.Getenv("RUNNER_IDLE_TIMEOUT"))
	if err != nil {
		return fmt.Errorf("invalid RUNNER_IDLE_TIMEOUT in env")
	}
	idleTimeout := time.Duration(idleTimeoutSeconds) * time.Second

	// Change to the GitHub actions runner installation dir.
	if err := os.Chdir("/actions-runner"); err != nil {
		return fmt.Errorf("cd to actions runner install dir: %w", err)
	}

	// Start the runner, setting up a pipe so we can read its output.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx, "./run.sh", "--jitconfig", jitconfig)
	stdoutReader, stdoutWriter := io.Pipe()
	cmd.Stdout = io.MultiWriter(os.Stdout, stdoutWriter)
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	// Read the runner's output and close the jobStarted channel when it prints
	// "Running job:"
	jobStarted := make(chan struct{})
	go func() {
		defer io.Copy(io.Discard, stdoutReader)
		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, "Running job:") {
				log.Infof("Runner started job; canceling idle timeout listener.")
				close(jobStarted)
				return
			}
		}
	}()

	// If the timeout elapses before jobStarted is closed, then kill the runner.
	go func() {
		t := time.NewTimer(idleTimeout)
		defer t.Stop()
		select {
		case <-jobStarted:
			return
		case <-t.C:
			cancel()
		}
	}()

	err = cmd.Wait()
	select {
	case <-ctx.Done():
		return fmt.Errorf("killed after idle timeout (%s)", idleTimeout)
	default:
		return err
	}
}
