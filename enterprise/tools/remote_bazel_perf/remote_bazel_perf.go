package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/creack/pty"
	"github.com/google/shlex"
	"github.com/google/uuid"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"time"
)

var (
	testCommand     = flag.String("cmd", "build //...", "Bazel command to run with remote bazel")
	apiKey          = flag.String("apiKey", "", "API key to authenticate the build")
	numRuns         = flag.Int("n", 1, "Number of times to run the bazel command")
	verbose         = flag.Bool("verbose", false, "Whether to log remote bazel output")
	withRemoteCache = flag.Bool("remote_cache", true, "Whether the remote builds should use a remote cache")
)

func main() {
	flag.Parse()

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		runTrial(true)
		wg.Done()
	}()
	go func() {
		runTrial(false)
		wg.Done()
	}()
	wg.Wait()

}

func runTrial(useBareRunner bool) {
	runner := "grpcs://remote.buildbuddy.dev"
	configFlag := "--config=remote-dev"
	if useBareRunner {
		runner = "grpcs://cloud.metal.buildbuddy.dev"
		configFlag = "--config=bare"
	}
	if !*withRemoteCache {
		configFlag = ""
	}

	results := "Cloud results:\n"
	if useBareRunner {
		results = "Bare runner results:\n"
	}

	for i := 0; i < *numRuns; i++ {
		guid, err := uuid.NewRandom()
		if err != nil {
			log.Fatalf(err.Error())
		}
		randomID := guid.String()

		f := configFlag
		if !*withRemoteCache {
			f += " --remote_instance_name=" + randomID
		}
		resultCleanRunner, err := triggerRemoteRun(runner, f, randomID)
		if err != nil {
			log.Fatalf(err.Error())
		}
		// Sleep to give the snapshot some time to save
		time.Sleep(1 * time.Minute)
		resultRecycledRunner, err := triggerRemoteRun(runner, f, randomID)
		if err != nil {
			log.Fatalf(err.Error())
		}

		results += fmt.Sprintf("[Run %d - Clean runner] %s", i+1, resultCleanRunner)
		results += fmt.Sprintf("[Run %d - Recycled runner] %s", i+1, resultRecycledRunner)
	}

	fmt.Println(results)
}

func triggerRemoteRun(runnerTarget string, extraArgs string, runID string) (string, error) {
	startTime := time.Now()
	log.Infof("Running: %s", fmt.Sprintf(
		"bb remote --remote_runner=%s --runner_exec_properties=instance_name=%s %s  --remote_header=x-buildbuddy-api-key=%s %s",
		runnerTarget, runID, *testCommand, *apiKey, extraArgs))

	results := ""
	output, err := runCommand(fmt.Sprintf(
		"bb remote --remote_runner=%s --runner_exec_properties=instance_name=%s %s --remote_header=x-buildbuddy-api-key=%s %s",
		runnerTarget, runID, *testCommand, *apiKey, extraArgs))
	if err != nil {
		if output == "" {
			return "", err
		}
		results += "Run failed\n"
	}
	endTime := time.Now()

	// Find first timestamp from remote run
	pattern := `2024-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} UTC`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}

	firstRemoteTimestamp := re.FindString(output)
	if firstRemoteTimestamp == "" {
		return "", errors.New("No remote timestamps found in output")
	}
	firstRemoteTimestampParsed, err := time.Parse("2006-01-02 15:04:05.000 MST", firstRemoteTimestamp)
	if err != nil {
		return "", status.WrapErrorf(err, "Could not parse timestamp %s", firstRemoteTimestamp)
	}

	remoteRunDuration := endTime.Sub(firstRemoteTimestampParsed)
	completeRunDuration := endTime.Sub(startTime)

	// Capture invocation link
	pattern = `Streaming remote runner logs to: ([^\n]+)`
	re, err = regexp.Compile(pattern)
	if err != nil {
		return "", err
	}
	invocationLink := re.FindStringSubmatch(output)[0]

	results += fmt.Sprintf("Remote run duration: %s\n", durationStr(remoteRunDuration))
	results += fmt.Sprintf("Complete run duration (including queuing): %s\n", durationStr(completeRunDuration))
	results += fmt.Sprintf("%s\n", invocationLink)
	return results, nil
}

func runCommand(run string) (string, error) {
	var buf bytes.Buffer
	out := io.Discard
	if *verbose {
		out = os.Stdout
	}
	w := io.MultiWriter(out, &buf)

	args, err := shlex.Split(run)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = os.Environ()
	cmd.Dir = "/Users/maggielou/bb/buildbuddy"
	size := &pty.Winsize{Rows: uint16(20), Cols: uint16(114)}
	f, err := pty.StartWithSize(cmd, size)
	if err != nil {
		return "", err
	}
	defer f.Close()
	copyOutputDone := make(chan struct{})
	go func() {
		io.Copy(w, f)
		copyOutputDone <- struct{}{}
	}()
	err = cmd.Wait()
	<-copyOutputDone

	return buf.String(), err
}

func durationStr(d time.Duration) string {
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60

	// Format and print the duration in "MM:SS" format
	return fmt.Sprintf("%02dm %02ds", minutes, seconds)
}
